
import 'dotenv/config';

import axios from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';
import { WebSocketServer, WebSocket } from 'ws';

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const E = process.env;
const ROOT = path.dirname(fileURLToPath(import.meta.url));

const BASE_URL = 'https://www.jobsatamazon.co.uk';
const HOME_URL = `${BASE_URL}/`;

const PORT = Number(E.PORT || 5002);
const WS_PORT = Number(E.WS_PORT || 8082);
const HOST = '127.0.0.1';

const GRAPHQL_URL = new URL(
  E.GRAPHQL_PATH || '/graphql',
  BASE_URL
);

if (
  GRAPHQL_URL.protocol !== 'https:' ||
  GRAPHQL_URL.hostname !== 'www.jobsatamazon.co.uk'
) {
  throw new Error('Invalid GraphQL endpoint');
}

const MIN_SCAN_MS = Math.max(
  900000,
  Number(E.MIN_SCAN_MS || 900000)
);

const MAX_SCAN_MS = Math.max(
  MIN_SCAN_MS,
  Number(E.MAX_SCAN_MS || 1800000)
);

const STATE_FILE = path.join(ROOT, 'seen_jobs.json');

const now = () => new Date().toISOString();

function log(event, details = {}) {
  console.log(JSON.stringify({
    time: now(),
    service: 'amazon-detector',
    event,
    ...details
  }));
}

function randomInterval() {
  return Math.floor(
    MIN_SCAN_MS +
    Math.random() * (MAX_SCAN_MS - MIN_SCAN_MS)
  );
}

function parseJSON(value, fallback = {}) {
  if (!value) return fallback;
  return JSON.parse(value);
}

function readPath(object, dotPath) {
  return dotPath.split('.').reduce(
    (current, key) => current?.[key],
    object
  );
}

const status = {
  service: 'amazon-uk-hourly-detector',
  source: HOME_URL,
  mode: 'graphql',
  startedAt: now(),

  state: 'starting',
  initialized: false,
  paused: false,
  security: null,

  lastHTTP: null,
  lastScan: null,
  lastError: null,
  lastStage: null,

  visibleJobs: 0,
  totalSeen: 0,
  lastNewJobs: 0,

  nextScan: null,
  consecutiveFailures: 0,
  cookieCount: 0
};

let timer = null;
let scanning = false;
let stopping = false;
let sessionReady = false;
let baselineReady = false;

const seen = new Map();

// ----------------------------------------------
// Existing job history
// ----------------------------------------------

function loadState() {
  if (!fs.existsSync(STATE_FILE)) {
    log('state_memory_only');
    return;
  }

  const saved = JSON.parse(
    fs.readFileSync(STATE_FILE, 'utf8')
  );

  if (Array.isArray(saved.jobs)) {
    for (const job of saved.jobs) {
      if (job?.jobId) {
        seen.set(String(job.jobId), job);
      }
    }
  }

  baselineReady = true;
  status.totalSeen = seen.size;

  log('state_loaded', {
    total: seen.size
  });
}

function saveState() {
  // Do not create a new state file.
  if (!fs.existsSync(STATE_FILE)) return;

  fs.writeFileSync(
    STATE_FILE,
    JSON.stringify({
      version: 1,
      updatedAt: now(),
      jobs: [...seen.values()]
    }, null, 2),
    { mode: 0o600 }
  );
}

// ----------------------------------------------
// Axios session
// ----------------------------------------------

let jar = new CookieJar();
let api = createClient();

function createClient() {
  return wrapper(axios.create({
    jar,
    withCredentials: true,

    timeout: 25000,
    maxRedirects: 0,
    validateStatus: () => true,

    headers: {
      Accept:
        'application/json, text/html;q=0.9, */*;q=0.8',
      'Accept-Language': 'en-GB,en;q=0.9'
    }
  }));
}

function resetSession() {
  jar = new CookieJar();
  api = createClient();

  sessionReady = false;
  status.initialized = false;
  status.cookieCount = 0;
}

function checkResponse(response, stage) {
  status.lastHTTP = response.status;
  status.lastStage = stage;

  if (
    response.status === 403 ||
    response.status === 429
  ) {
    const error = new Error(
      `${stage}: HTTP ${response.status} access refused`
    );

    error.accessRestricted = true;
    error.httpStatus = response.status;
    throw error;
  }

  if (response.status === 401) {
    const error = new Error(
      `${stage}: HTTP 401 authorization required`
    );

    error.sessionExpired = true;
    throw error;
  }

  if (
    response.status >= 300 &&
    response.status < 400
  ) {
    throw new Error(
      `${stage}: HTTP ${response.status} redirect`
    );
  }

  if (
    response.status < 200 ||
    response.status >= 300
  ) {
    throw new Error(
      `${stage}: HTTP ${response.status}`
    );
  }

  return response.data;
}

// ----------------------------------------------
// Homepage-first initialization
// ----------------------------------------------

async function initializeSession() {
  status.state = 'initializing';
  status.lastStage = 'homepage';

  log('homepage_request', {
    url: HOME_URL
  });

  const response = await api.get(HOME_URL, {
    headers: {
      Accept:
        'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
  });

  checkResponse(response, 'homepage');

  const cookies = await jar.getCookies(BASE_URL);

  status.cookieCount = cookies.length;
  status.initialized = true;

  sessionReady = true;

  log('homepage_initialized', {
    httpStatus: response.status,
    cookiesReceived: cookies.length
  });

  // No requirement for cookies to exist:
  // some public APIs legitimately use none.
  // Session authorization, if required, must
  // follow the service's permitted workflow.
}

// ----------------------------------------------
// GraphQL job collection
// ----------------------------------------------

function normalizeJob(raw) {
  if (!raw || typeof raw !== 'object') return null;

  const field = value => {
    if (value == null || typeof value === 'object') {
      return null;
    }

    return String(value).trim() || null;
  };

  const jobId = field(
    raw.jobId ??
    raw.jobID ??
    raw.job_id ??
    raw.jobPostingId ??
    raw.id
  );

  const title = field(
    raw.title ??
    raw.jobTitle ??
    raw.name
  );

  if (!jobId || !title) return null;

  let applyUrl = null;

  try {
    if (raw.applyUrl || raw.jobUrl || raw.url) {
      const url = new URL(
        raw.applyUrl || raw.jobUrl || raw.url,
        BASE_URL
      );

      if (
        url.protocol === 'https:' &&
        (
          url.hostname === 'www.jobsatamazon.co.uk' ||
          url.hostname === 'jobsatamazon.co.uk'
        )
      ) {
        applyUrl = url.href;
      }
    }
  } catch {}

  return {
    jobId,
    title,

    location: field(
      raw.location?.label ??
      raw.location?.city ??
      raw.location?.name ??
      raw.location ??
      raw.city ??
      raw.siteName
    ),

    postcode: field(
      raw.postcode ??
      raw.postalCode ??
      raw.location?.postalCode
    ),

    payRate: field(
      raw.payRate ??
      raw.hourlyPay ??
      raw.hourlyRate ??
      raw.compensation?.display
    ),

    shiftDetails: field(
      raw.shiftDetails ??
      raw.shift?.name ??
      raw.schedule ??
      raw.employmentType
    ),

    applyUrl
  };
}

async function fetchJobs() {
  if (!sessionReady) {
    await initializeSession();
  }

  const query = E.GRAPHQL_JOB_QUERY;
  const resultPath = E.GRAPHQL_JOBS_PATH;

  if (!query || !resultPath) {
    throw new Error(
      'GRAPHQL_JOB_QUERY and GRAPHQL_JOBS_PATH are required'
    );
  }

  const variables = parseJSON(
    E.GRAPHQL_JOB_VARIABLES,
    {}
  );

  const payload = {
    query,
    variables
  };

  if (E.GRAPHQL_JOB_OPERATION) {
    payload.operationName =
      E.GRAPHQL_JOB_OPERATION;
  }

  status.lastStage = 'graphql';

  const response = await api.post(
    GRAPHQL_URL.href,
    payload,
    {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json'
      }
    }
  );

  const result = checkResponse(
    response,
    'graphql'
  );

  if (
    Array.isArray(result?.errors) &&
    result.errors.length
  ) {
    const message = result.errors
      .map(error => error.message || 'GraphQL error')
      .join('; ');

    throw new Error(message);
  }

  // Path is relative to result.data.
  const records = readPath(
    result?.data,
    resultPath
  );

  if (!Array.isArray(records)) {
    throw new Error(
      `Job array not found at data.${resultPath}`
    );
  }

  const unique = new Map();

  for (const record of records) {
    const job = normalizeJob(record);

    if (!job) continue;

    const previous = unique.get(job.jobId);

    if (!previous) {
      unique.set(job.jobId, job);
      continue;
    }

    for (const key of Object.keys(job)) {
      if (!previous[key] && job[key]) {
        previous[key] = job[key];
      }
    }
  }

  return [...unique.values()];
}

// ----------------------------------------------
// WebSocket signals: port 8082
// ----------------------------------------------

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 65536
});

const wsHttp = http.createServer((req, res) => {
  res.writeHead(404).end();
});

wsHttp.on('upgrade', (req, socket, head) => {
  if (req.url !== '/') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', socket => {
  log('receiver_connected');

  socket.on('error', error => {
    log('websocket_error', {
      error: error.message
    });
  });

  socket.send(JSON.stringify({
    event: 'DETECTOR_CONNECTED',
    timestamp: now()
  }));
});

function broadcast(job) {
  const message = JSON.stringify({
    event: 'NEW_UK_JOB',
    timestamp: now(),
    job
  });

  for (const socket of wss.clients) {
    if (socket.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (socket.bufferedAmount > 1048576) {
      socket.terminate();
      continue;
    }

    socket.send(message);
  }
}

// ----------------------------------------------
// Job processing
// ----------------------------------------------

function processJobs(records) {
  const fresh = records.filter(
    job => !seen.has(job.jobId)
  );

  for (const job of fresh) {
    seen.set(job.jobId, {
      ...job,
      discoveredAt: now()
    });
  }

  saveState();

  status.visibleJobs = records.length;
  status.lastNewJobs = fresh.length;
  status.totalSeen = seen.size;
  status.lastScan = now();

  if (
    baselineReady ||
    E.ALERT_EXISTING === 'true'
  ) {
    for (const job of fresh) {
      broadcast(job);

      log('new_job_detected', {
        jobId: job.jobId,
        title: job.title,
        location: job.location,
        payRate: job.payRate
      });
    }
  } else {
    log('initial_baseline', {
      jobs: fresh.length
    });
  }

  baselineReady = true;
}

// ----------------------------------------------
// Polling and error handling
// ----------------------------------------------

function scheduleNext(ms) {
  clearTimeout(timer);

  if (stopping || status.paused) return;

  status.nextScan = new Date(
    Date.now() + ms
  ).toISOString();

  timer = setTimeout(() => {
    void scan();
  }, ms);
}

async function scan() {
  if (scanning || stopping || status.paused) {
    return;
  }

  scanning = true;
  status.state = 'scanning';
  status.nextScan = null;

  try {
    const jobs = await fetchJobs();

    processJobs(jobs);

    status.state = 'monitoring';
    status.lastError = null;
    status.security = null;
    status.consecutiveFailures = 0;

    log('scan_complete', {
      visibleJobs: jobs.length,
      newJobs: status.lastNewJobs,
      totalSeen: status.totalSeen
    });

    scheduleNext(randomInterval());

  } catch (error) {
    status.lastError = error.message;

    if (error.accessRestricted) {
      status.paused = true;
      status.state = 'access_restricted';
      status.security = 'access_restricted';
      status.lastHTTP = error.httpStatus;
      status.nextScan = null;

      log('access_refused', {
        stage: status.lastStage,
        httpStatus: error.httpStatus,
        warning: 'Remote access refused; polling stopped'
      });

    } else if (error.sessionExpired) {
      resetSession();

      status.state = 'retry_wait';
      status.consecutiveFailures++;

      scheduleNext(60 * 60 * 1000);

    } else {
      status.state = 'retry_wait';
      status.consecutiveFailures++;

      const backoff = Math.min(
        60 * 60 * 1000,
        randomInterval() *
          Math.pow(
            2,
            Math.min(status.consecutiveFailures, 3)
          )
      );

      log('scan_error', {
        error: error.message,
        retryMinutes: Math.round(backoff / 60000)
      });

      scheduleNext(backoff);
    }
  } finally {
    scanning = false;
  }
}

// ----------------------------------------------
// HTTP health API: port 5002
// ----------------------------------------------

const healthServer = http.createServer((req, res) => {
  const url = new URL(
    req.url,
    `http://${HOST}:${PORT}`
  );

  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');

  if (
    url.pathname === '/health' ||
    url.pathname === '/status'
  ) {
    const unhealthy =
      status.paused ||
      status.state === 'configuration_required';

    res.writeHead(unhealthy ? 503 : 200);

    res.end(JSON.stringify({
      ...status,
      websocketClients: wss.clients.size,
      uptimeSeconds: Math.floor(process.uptime())
    }));

    return;
  }

  if (url.pathname === '/jobs') {
    const requested = Number(
      url.searchParams.get('limit') || 100
    );

    const limit = Number.isFinite(requested)
      ? Math.max(1, Math.min(500, Math.floor(requested)))
      : 100;

    res.writeHead(200);

    res.end(JSON.stringify({
      total: seen.size,
      jobs: [...seen.values()]
        .slice(-limit)
        .reverse()
    }));

    return;
  }

  res.writeHead(404);

  res.end(JSON.stringify({
    error: 'Not found'
  }));
});

async function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, HOST, resolve);
  });
}

// ----------------------------------------------
// Startup / shutdown
// ----------------------------------------------

async function main() {
  loadState();

  await listen(healthServer, PORT);
  await listen(wsHttp, WS_PORT);

  log('detector_started', {
    mode: 'graphql',
    source: HOME_URL,
    healthPort: PORT,
    websocketPort: WS_PORT
  });

  if (
    !E.GRAPHQL_JOB_QUERY ||
    !E.GRAPHQL_JOBS_PATH
  ) {
    status.state = 'configuration_required';
    status.lastError =
      'Missing GRAPHQL_JOB_QUERY or GRAPHQL_JOBS_PATH';

    log('configuration_required');
    return;
  }

  await scan();
}

async function shutdown() {
  if (stopping) return;

  stopping = true;
  clearTimeout(timer);

  for (const socket of wss.clients) {
    socket.terminate();
  }

  wss.close();
  wsHttp.close();
  healthServer.close();

  log('shutdown_complete');
}

process.once('SIGTERM', () => {
  void shutdown();
});

process.once('SIGINT', () => {
  void shutdown();
});

main().catch(error => {
  log('fatal', { error: error.message });
  process.exitCode = 1;
});

