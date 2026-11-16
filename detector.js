
import 'dotenv/config';

import axios from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';
import { WebSocketServer, WebSocket } from 'ws';

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const E = process.env;

const BASE_URL = 'https://www.jobsatamazon.co.uk';

const CONFIG = {
  port: Number(E.PORT || 5002),
  host: E.BIND_HOST || '127.0.0.1',

  wsPort: Number(E.WS_PORT || 8082),
  wsHost: E.WS_HOST || '127.0.0.1',

  csrfPath: E.CSRF_PATH || '/csrf?countryCode=UK',
  authorizePath:
    E.AUTH_PATH || '/authorize?countryCode=UK',
  graphqlPath: E.GRAPHQL_PATH || '/graphql',

  // The actual GraphQL operation must be supplied.
  graphqlQuery: E.GRAPHQL_QUERY || '',
  graphqlOperation: E.GRAPHQL_OPERATION || '',
  graphqlJobsPath: E.GRAPHQL_JOBS_PATH || '',

  minScan: Math.max(
    900000,
    Number(E.MIN_SCAN_MS || 900000)
  ),

  maxScan: Math.max(
    1800000,
    Number(E.MAX_SCAN_MS || 1800000)
  ),

  requestTimeout: 25000,

  maxRetryDelay: 3600000,

  csrfHeader:
    E.CSRF_REQUEST_HEADER || 'x-csrf-token',

  csrfResponseHeader:
    E.CSRF_RESPONSE_HEADER || 'x-csrf-token',

  csrfCookieName: E.CSRF_COOKIE_NAME || '',

  enableAuthorize:
    E.ENABLE_AUTHORIZE === 'true',

  alertExisting:
    E.ALERT_EXISTING === 'true',

  stateFile: path.join(ROOT, 'seen_jobs.json')
};

const now = () => new Date().toISOString();

function log(event, details = {}) {
  console.log(JSON.stringify({
    time: now(),
    service: 'amazon-uk-api-detector',
    event,
    ...details
  }));
}

function randomBetween(min, max) {
  return Math.floor(
    Math.random() * (max - min + 1)
  ) + min;
}

function nextInterval() {
  return randomBetween(
    CONFIG.minScan,
    CONFIG.maxScan
  );
}

function parseVariables() {
  try {
    const data = JSON.parse(
      E.GRAPHQL_VARIABLES || '{}'
    );

    if (!data || typeof data !== 'object' ||
        Array.isArray(data)) {
      throw new Error('Expected JSON object');
    }

    return data;
  } catch (error) {
    throw new Error(
      'Invalid GRAPHQL_VARIABLES: ' + error.message
    );
  }
}

function validateAPIPath(value) {
  const url = new URL(value, BASE_URL);

  if (
    url.protocol !== 'https:' ||
    url.hostname !== 'www.jobsatamazon.co.uk'
  ) {
    throw new Error(
      'API endpoint must remain on jobsatamazon.co.uk'
    );
  }

  return url.href;
}

const ENDPOINTS = {
  csrf: validateAPIPath(CONFIG.csrfPath),
  authorize: validateAPIPath(CONFIG.authorizePath),
  graphql: validateAPIPath(CONFIG.graphqlPath)
};

const status = {
  service: 'amazon-uk-api-detector',
  startedAt: now(),
  source: BASE_URL,
  mode: 'graphql',
  state: 'starting',

  initialized: false,
  paused: false,
  security: null,

  lastHTTP: null,
  lastScan: null,
  lastError: null,

  visibleJobs: 0,
  totalSeen: 0,
  lastNewJobs: 0,

  nextScan: null,
  consecutiveFailures: 0,

  websocketPort: CONFIG.wsPort
};

let stopping = false;
let scanning = false;
let timer = null;
let initialized = false;
let csrfToken = null;
let baselineComplete = false;

const seen = new Map();

// ----------------------------------------------------
// Persistent duplicate detection
// ----------------------------------------------------

function loadState() {
  if (!fs.existsSync(CONFIG.stateFile)) {
    log('state_file_missing', {
      note: 'Memory-only deduplication until an existing state file is available'
    });
    return;
  }

  const parsed = JSON.parse(
    fs.readFileSync(CONFIG.stateFile, 'utf8')
  );

  if (Array.isArray(parsed.jobs)) {
    for (const job of parsed.jobs) {
      if (job?.jobId) {
        seen.set(String(job.jobId), job);
      }
    }
  } else if (Array.isArray(parsed.ids)) {
    for (const id of parsed.ids) {
      seen.set(String(id), {
        jobId: String(id)
      });
    }
  } else {
    throw new Error('Unsupported seen_jobs.json structure');
  }

  baselineComplete = true;
  status.totalSeen = seen.size;

  log('state_loaded', {
    jobs: seen.size
  });
}

function saveState() {
  // Respect the requirement not to create new files.
  if (!fs.existsSync(CONFIG.stateFile)) {
    return;
  }

  const data = JSON.stringify({
    version: 1,
    updatedAt: now(),
    jobs: [...seen.values()]
  }, null, 2);

  fs.writeFileSync(
    CONFIG.stateFile,
    data,
    { mode: 0o600 }
  );
}

// ----------------------------------------------------
// HTTP client with automatic in-memory cookie handling
// ----------------------------------------------------

const jar = new CookieJar();

const client = wrapper(axios.create({
  timeout: CONFIG.requestTimeout,
  jar,
  withCredentials: true,
  maxRedirects: 0,

  validateStatus: () => true,

  headers: {
    Accept: 'application/json',
    'Accept-Language': 'en-GB,en;q=0.9'
  }
}));

function assertResponse(response, operation) {
  status.lastHTTP = response.status;

  if (
    response.status === 403 ||
    response.status === 429
  ) {
    const error = new Error(
      `${operation}: HTTP ${response.status} access refused`
    );

    error.accessRestricted = true;
    error.status = response.status;

    throw error;
  }

  if (response.status === 401) {
    const error = new Error(
      `${operation}: authorization required`
    );

    error.sessionExpired = true;
    error.status = 401;

    throw error;
  }

  if (
    response.status >= 300 &&
    response.status < 400
  ) {
    throw new Error(
      `${operation}: redirect received; interactive authorization may be required`
    );
  }

  if (
    response.status < 200 ||
    response.status >= 300
  ) {
    const error = new Error(
      `${operation}: HTTP ${response.status}`
    );

    error.status = response.status;
    throw error;
  }

  return response.data;
}

async function initializeSession() {
  status.state = 'initializing';

  log('session_initializing');

  const csrfResponse = await client.get(
    ENDPOINTS.csrf
  );

  const csrfData = assertResponse(
    csrfResponse,
    'csrf'
  );

  let cookieToken = null;

  if (CONFIG.csrfCookieName) {
    const cookies = await jar.getCookies(
      BASE_URL
    );

    cookieToken = cookies.find(
      cookie =>
        cookie.key === CONFIG.csrfCookieName
    )?.value || null;
  }

  csrfToken =
    csrfResponse.headers[
      CONFIG.csrfResponseHeader.toLowerCase()
    ] ||
    csrfData?.csrfToken ||
    csrfData?.csrf ||
    cookieToken ||
    null;

  log('csrf_initialized', {
    tokenPresent: Boolean(csrfToken)
  });

  // Authorization is optional and must not be
  // treated as a way to bypass an interactive login.
  if (CONFIG.enableAuthorize) {
    const headers = {};

    if (csrfToken) {
      headers[CONFIG.csrfHeader] = csrfToken;
    }

    const authorizeResponse = await client.get(
      ENDPOINTS.authorize,
      { headers }
    );

    assertResponse(
      authorizeResponse,
      'authorize'
    );

    log('authorize_completed');
  }

  initialized = true;
  status.initialized = true;

  log('session_initialized');
}

// ----------------------------------------------------
// GraphQL operations
// ----------------------------------------------------

function readPath(object, fieldPath) {
  if (!fieldPath) return undefined;

  return fieldPath.split('.').reduce(
    (current, key) => current?.[key],
    object
  );
}

function normalizeText(value) {
  if (value == null) return null;

  if (typeof value === 'object') {
    return null;
  }

  return String(value).trim() || null;
}

function normalizeJob(raw) {
  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const jobId = normalizeText(
    raw.jobId ??
    raw.jobID ??
    raw.job_id ??
    raw.jobPostingId ??
    raw.id
  );

  const title = normalizeText(
    raw.title ??
    raw.jobTitle ??
    raw.name
  );

  if (!jobId || !title) return null;

  const location = normalizeText(
    raw.location?.city ??
    raw.location?.name ??
    raw.location ??
    raw.city ??
    raw.siteName
  );

  const postcode = normalizeText(
    raw.postcode ??
    raw.postalCode ??
    raw.zipCode ??
    raw.location?.postcode
  );

  const payRate = normalizeText(
    raw.payRate ??
    raw.hourlyPay ??
    raw.hourlyRate ??
    raw.compensation?.display
  );

  const shiftDetails = normalizeText(
    raw.shiftDetails ??
    raw.shift?.name ??
    raw.shifts ??
    raw.schedule ??
    raw.employmentType
  );

  let applyUrl = null;

  const originalURL =
    raw.applyUrl ||
    raw.jobUrl ||
    raw.url ||
    null;

  if (originalURL) {
    try {
      const url = new URL(
        originalURL,
        BASE_URL
      );

      if (
        url.protocol === 'https:' &&
        (
          url.hostname === 'jobsatamazon.co.uk' ||
          url.hostname === 'www.jobsatamazon.co.uk'
        )
      ) {
        applyUrl = url.href;
      }
    } catch {}
  }

  return {
    jobId,
    title,
    location,
    postcode,
    payRate,
    shiftDetails,
    shifts: shiftDetails,
    applyUrl
  };
}

async function queryGraphQL() {
  if (!initialized) {
    await initializeSession();
  }

  if (!CONFIG.graphqlQuery) {
    throw new Error(
      'GRAPHQL_QUERY is missing from .env'
    );
  }

  if (!CONFIG.graphqlJobsPath) {
    throw new Error(
      'GRAPHQL_JOBS_PATH is missing from .env'
    );
  }

  const headers = {
    'Content-Type': 'application/json',
    Accept: 'application/json'
  };

  if (csrfToken) {
    headers[CONFIG.csrfHeader] = csrfToken;
  }

  const payload = {
    query: CONFIG.graphqlQuery,
    variables: parseVariables()
  };

  if (CONFIG.graphqlOperation) {
    payload.operationName =
      CONFIG.graphqlOperation;
  }

  const response = await client.post(
    ENDPOINTS.graphql,
    payload,
    { headers }
  );

  const result = assertResponse(
    response,
    'graphql'
  );

  if (
    !result ||
    typeof result !== 'object'
  ) {
    throw new Error(
      'GraphQL response is not a JSON object'
    );
  }

  if (
    Array.isArray(result.errors) &&
    result.errors.length > 0
  ) {
    throw new Error(
      `GraphQL returned ${result.errors.length} error(s)`
    );
  }

  const rawJobs = readPath(
    result,
    CONFIG.graphqlJobsPath
  );

  if (!Array.isArray(rawJobs)) {
    throw new Error(
      `No job array found at ${CONFIG.graphqlJobsPath}`
    );
  }

  const deduplicated = new Map();

  for (const raw of rawJobs) {
    const job = normalizeJob(raw);

    if (!job) continue;

    const previous = deduplicated.get(
      job.jobId
    );

    if (!previous) {
      deduplicated.set(job.jobId, job);
      continue;
    }

    for (const key of Object.keys(job)) {
      if (!previous[key] && job[key]) {
        previous[key] = job[key];
      }
    }
  }

  return [...deduplicated.values()];
}

// ----------------------------------------------------
// WebSocket server for existing Receiver
// ----------------------------------------------------

const wsHttp = http.createServer(
  (req, res) => {
    res.writeHead(404).end();
  }
);

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 65536
});

wsHttp.on('upgrade', (req, socket, head) => {
  if (req.url !== '/') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(
    req,
    socket,
    head,
    ws => {
      wss.emit('connection', ws, req);
    }
  );
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
  const payload = JSON.stringify({
    event: 'NEW_UK_JOB',
    timestamp: now(),
    job
  });

  let clients = 0;

  for (const socket of wss.clients) {
    if (socket.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (socket.bufferedAmount > 1048576) {
      socket.terminate();
      continue;
    }

    socket.send(payload);
    clients++;
  }

  return clients;
}

// ----------------------------------------------------
// Job processing and deduplication
// ----------------------------------------------------

function processJobs(incoming) {
  const fresh = [];

  for (const job of incoming) {
    if (seen.has(job.jobId)) continue;

    seen.set(job.jobId, {
      ...job,
      discoveredAt: now()
    });

    fresh.push(job);
  }

  saveState();

  status.visibleJobs = incoming.length;
  status.lastNewJobs = fresh.length;
  status.totalSeen = seen.size;
  status.lastScan = now();

  if (
    baselineComplete ||
    CONFIG.alertExisting
  ) {
    for (const job of fresh) {
      const clients = broadcast(job);

      log('new_job_detected', {
        jobId: job.jobId,
        title: job.title,
        location: job.location,
        websocketClients: clients
      });
    }
  } else {
    log('initial_baseline', {
      jobs: fresh.length
    });
  }

  baselineComplete = true;

  log('scan_complete', {
    visible: incoming.length,
    newJobs: fresh.length,
    totalSeen: seen.size
  });
}

// ----------------------------------------------------
// Polling, retry and access restrictions
// ----------------------------------------------------

function scheduleNext(ms) {
  if (stopping || status.paused) return;

  clearTimeout(timer);

  status.nextScan = new Date(
    Date.now() + ms
  ).toISOString();

  timer = setTimeout(() => {
    void scan();
  }, ms);
}

function pauseAccess(error) {
  clearTimeout(timer);

  initialized = false;

  status.initialized = false;
  status.paused = true;
  status.state = 'access_restricted';
  status.security = 'access_restricted';
  status.lastError = error.message;
  status.nextScan = null;

  log('access_paused', {
    httpStatus: error.status || status.lastHTTP,
    reason: error.message,
    manualReviewRequired: true
  });
}

async function scan() {
  if (scanning || stopping || status.paused) {
    return;
  }

  scanning = true;
  status.state = 'scanning';
  status.nextScan = null;

  try {
    const jobs = await queryGraphQL();

    processJobs(jobs);

    status.consecutiveFailures = 0;
    status.lastError = null;
    status.security = null;
    status.state = 'monitoring';

    scheduleNext(nextInterval());
  } catch (error) {
    if (error.accessRestricted) {
      pauseAccess(error);
      return;
    }

    status.lastError = error.message;
    status.consecutiveFailures++;

    if (error.sessionExpired) {
      initialized = false;
      csrfToken = null;
      status.initialized = false;
    }

    const backoff = Math.min(
      CONFIG.maxRetryDelay,
      nextInterval() *
        Math.pow(
          2,
          Math.min(status.consecutiveFailures, 3)
        )
    );

    status.state = 'retry_wait';

    log('scan_error', {
      error: error.message,
      failures: status.consecutiveFailures,
      retryInMinutes: Math.round(backoff / 60000)
    });

    scheduleNext(backoff);
  } finally {
    scanning = false;
  }
}

// ----------------------------------------------------
// Health and jobs API
// ----------------------------------------------------

const server = http.createServer(
  (req, res) => {
    const url = new URL(
      req.url,
      `http://${CONFIG.host}:${CONFIG.port}`
    );

    res.setHeader(
      'Content-Type',
      'application/json'
    );

    res.setHeader(
      'Cache-Control',
      'no-store'
    );

    if (
      url.pathname === '/health' ||
      url.pathname === '/status'
    ) {
      res.writeHead(
        status.paused ? 503 : 200
      );

      res.end(JSON.stringify({
        ...status,
        uptimeSeconds:
          Math.floor(process.uptime()),
        websocketClients:
          wss.clients.size
      }));

      return;
    }

    if (url.pathname === '/jobs') {
      const rawLimit = Number(
        url.searchParams.get('limit') || 100
      );

      const limit = Number.isFinite(rawLimit)
        ? Math.min(
            500,
            Math.max(1, Math.floor(rawLimit))
          )
        : 100;

      const values = [...seen.values()];

      res.writeHead(200);

      res.end(JSON.stringify({
        total: values.length,
        jobs: values
          .slice(-limit)
          .reverse()
      }));

      return;
    }

    res.writeHead(404);

    res.end(JSON.stringify({
      error: 'Not found'
    }));
  }
);

function listen(instance, port, host) {
  return new Promise((resolve, reject) => {
    instance.once('error', reject);

    instance.listen(port, host, resolve);
  });
}

// ----------------------------------------------------
// Startup and graceful shutdown
// ----------------------------------------------------

async function main() {
  loadState();

  // Configuration must be complete before requests
  // are sent to the external service.
  if (!CONFIG.graphqlQuery ||
      !CONFIG.graphqlJobsPath) {
    status.state = 'configuration_required';
    status.lastError =
      'Set GRAPHQL_QUERY and GRAPHQL_JOBS_PATH';
  }

  await listen(
    server,
    CONFIG.port,
    CONFIG.host
  );

  await listen(
    wsHttp,
    CONFIG.wsPort,
    CONFIG.wsHost
  );

  log('servers_started', {
    httpPort: CONFIG.port,
    websocketPort: CONFIG.wsPort,
    mode: 'graphql'
  });

  if (!CONFIG.graphqlQuery ||
      !CONFIG.graphqlJobsPath) {
    log('configuration_required', {
      missing: [
        ...(!CONFIG.graphqlQuery
          ? ['GRAPHQL_QUERY'] : []),
        ...(!CONFIG.graphqlJobsPath
          ? ['GRAPHQL_JOBS_PATH'] : [])
      ]
    });
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
  server.close();

  log('shutdown_complete');
}

process.once('SIGINT', () => {
  void shutdown();
});

process.once('SIGTERM', () => {
  void shutdown();
});

main().catch(error => {
  log('fatal_error', {
    error: error.message
  });

  process.exitCode = 1;
  void shutdown();
});
