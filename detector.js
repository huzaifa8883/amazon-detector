
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
  httpPort: Number(E.PORT || 5002),
  wsPort: Number(E.WS_PORT || 8082),
  host: '127.0.0.1',

  csrfPath: E.CSRF_PATH || '/csrf?countryCode=UK',
  authPath: E.AUTH_PATH || '/authorize?countryCode=UK',
  graphqlPath: E.GRAPHQL_PATH || '/graphql',

  csrfHeader: E.CSRF_HEADER || 'x-csrf-token',
  authorizeEnabled: E.ENABLE_AUTHORIZE === 'true',

  jobQuery: E.GRAPHQL_JOB_QUERY || '',
  jobOperation: E.GRAPHQL_JOB_OPERATION || '',
  jobResultPath: E.GRAPHQL_JOBS_PATH || '',

  minScan: 15 * 60 * 1000,
  maxScan: 30 * 60 * 1000,

  timeout: 25000,

  alertExisting: E.ALERT_EXISTING === 'true',

  // Existing file only; never create it.
  stateFile: path.join(ROOT, 'seen_jobs.json')
};

const GEO_QUERY = `
query queryGeoInfoByAddress(
  $geoAddressQueryRequest: GeoAddressQueryRequest!
) {
  queryGeoInfoByAddress(
    geoAddressQueryRequest: $geoAddressQueryRequest
  ) {
    country
    lat
    lng
    postalCode
    label
    municipality
    region
    subRegion
    addressNumber
    __typename
  }
}
`;

const now = () => new Date().toISOString();

function log(event, details = {}) {
  console.log(JSON.stringify({
    timestamp: now(),
    service: 'amazon-uk-detector',
    event,
    ...details
  }));
}

function randomDelay(min, max) {
  return Math.floor(
    Math.random() * (max - min + 1)
  ) + min;
}

function nextInterval() {
  return randomDelay(
    CONFIG.minScan,
    CONFIG.maxScan
  );
}

function readJSON(value, fallback = {}) {
  if (!value) return fallback;
  return JSON.parse(value);
}

function endpoint(pathValue) {
  const u = new URL(pathValue, BASE_URL);

  if (
    u.protocol !== 'https:' ||
    u.hostname !== 'www.jobsatamazon.co.uk'
  ) {
    throw new Error('Invalid API host');
  }

  return u.href;
}

const URLS = {
  csrf: endpoint(CONFIG.csrfPath),
  auth: endpoint(CONFIG.authPath),
  graphql: endpoint(CONFIG.graphqlPath)
};

const status = {
  service: 'amazon-uk-hourly-detector',
  source: BASE_URL,
  mode: 'graphql',
  startedAt: now(),
  state: 'starting',

  paused: false,
  security: null,
  initialized: false,

  lastHTTP: null,
  lastScan: null,
  lastError: null,

  visibleJobs: 0,
  lastNewJobs: 0,
  totalSeen: 0,

  nextScan: null,
  websocketPort: CONFIG.wsPort
};

let stopping = false;
let scanning = false;
let timer = null;
let sessionReady = false;
let csrfToken = null;
let baselineReady = false;

const seen = new Map();

// --------------------------------------------------
// State management
// --------------------------------------------------

function loadState() {
  if (!fs.existsSync(CONFIG.stateFile)) {
    log('state_memory_only');
    return;
  }

  const data = JSON.parse(
    fs.readFileSync(CONFIG.stateFile, 'utf8')
  );

  if (Array.isArray(data.jobs)) {
    for (const job of data.jobs) {
      if (job?.jobId) {
        seen.set(String(job.jobId), job);
      }
    }
  } else if (Array.isArray(data.ids)) {
    for (const id of data.ids) {
      seen.set(String(id), {
        jobId: String(id)
      });
    }
  }

  baselineReady = true;
  status.totalSeen = seen.size;
}

function saveState() {
  // Never create a new state file.
  if (!fs.existsSync(CONFIG.stateFile)) {
    return;
  }

  fs.writeFileSync(
    CONFIG.stateFile,
    JSON.stringify({
      version: 1,
      updatedAt: now(),
      jobs: [...seen.values()]
    }, null, 2),
    { mode: 0o600 }
  );
}

// --------------------------------------------------
// Axios session and cookies
// --------------------------------------------------

const jar = new CookieJar();

const api = wrapper(axios.create({
  jar,
  withCredentials: true,
  timeout: CONFIG.timeout,
  maxRedirects: 0,
  validateStatus: () => true,

  headers: {
    Accept: 'application/json',
    'Accept-Language': 'en-GB,en;q=0.9'
  }
}));

function checkResponse(response, operation) {
  status.lastHTTP = response.status;

  if (
    response.status === 403 ||
    response.status === 429
  ) {
    const error = new Error(
      `${operation}: HTTP ${response.status}`
    );

    error.accessBlocked = true;
    error.httpStatus = response.status;
    throw error;
  }

  if (response.status === 401) {
    const error = new Error(
      `${operation}: authorization required`
    );

    error.authorizationRequired = true;
    throw error;
  }

  if (
    response.status < 200 ||
    response.status >= 300
  ) {
    throw new Error(
      `${operation}: HTTP ${response.status}`
    );
  }

  return response.data;
}

async function initializeSession() {
  status.state = 'initializing';

  const response = await api.get(URLS.csrf);

  const data = checkResponse(
    response,
    'CSRF initialization'
  );

  csrfToken =
    response.headers[
      CONFIG.csrfHeader.toLowerCase()
    ] ||
    data?.csrfToken ||
    data?.csrf ||
    null;

  // Optional. This only works when the endpoint
  // permits a noninteractive authorized session.
  if (CONFIG.authorizeEnabled) {
    const headers = csrfToken
      ? { [CONFIG.csrfHeader]: csrfToken }
      : {};

    const auth = await api.get(
      URLS.auth,
      { headers }
    );

    checkResponse(auth, 'Authorization');
  }

  sessionReady = true;
  status.initialized = true;

  log('session_initialized', {
    csrfPresent: Boolean(csrfToken)
  });
}

// --------------------------------------------------
// GraphQL
// --------------------------------------------------

async function graphql(
  query,
  operationName,
  variables
) {
  if (!sessionReady) {
    await initializeSession();
  }

  const headers = {
    'Content-Type': 'application/json',
    Accept: 'application/json'
  };

  if (csrfToken) {
    headers[CONFIG.csrfHeader] = csrfToken;
  }

  const response = await api.post(
    URLS.graphql,
    {
      operationName,
      query,
      variables
    },
    { headers }
  );

  const result = checkResponse(
    response,
    operationName
  );

  if (
    Array.isArray(result?.errors) &&
    result.errors.length
  ) {
    const errors = result.errors.map(
      e => e.message || 'GraphQL error'
    );

    throw new Error(errors.join('; '));
  }

  return result?.data;
}

async function resolveLocation() {
  if (!E.GEO_ADDRESS_REQUEST) return null;

  const variables = {
    geoAddressQueryRequest: readJSON(
      E.GEO_ADDRESS_REQUEST
    )
  };

  const result = await graphql(
    GEO_QUERY,
    'queryGeoInfoByAddress',
    variables
  );

  return result?.queryGeoInfoByAddress || null;
}

function getPath(obj, p) {
  return p.split('.').reduce(
    (value, key) => value?.[key],
    obj
  );
}

function text(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'object') return null;

  return String(value).trim() || null;
}

function normalizeJob(raw) {
  if (!raw || typeof raw !== 'object') return null;

  const jobId = text(
    raw.jobId ??
    raw.jobID ??
    raw.job_id ??
    raw.id ??
    raw.jobPostingId
  );

  const title = text(
    raw.title ??
    raw.jobTitle ??
    raw.name
  );

  if (!jobId || !title) return null;

  const location = text(
    raw.location?.label ??
    raw.location?.city ??
    raw.location ??
    raw.city ??
    raw.siteName
  );

  const postcode = text(
    raw.postcode ??
    raw.postalCode ??
    raw.location?.postalCode
  );

  const payRate = text(
    raw.payRate ??
    raw.hourlyPay ??
    raw.hourlyRate ??
    raw.compensation?.display
  );

  const shiftDetails = text(
    raw.shiftDetails ??
    raw.shift?.name ??
    raw.schedule ??
    raw.employmentType
  );

  let applyUrl = null;

  try {
    const value =
      raw.applyUrl ||
      raw.jobUrl ||
      raw.url;

    if (value) {
      const url = new URL(value, BASE_URL);

      if (
        url.protocol === 'https:' &&
        (
          url.hostname === 'jobsatamazon.co.uk' ||
          url.hostname === 'www.jobsatamazon.co.uk'
        )
      ) {
        applyUrl = url.href;
      }
    }
  } catch {}

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

async function fetchJobs() {
  if (
    !CONFIG.jobQuery ||
    !CONFIG.jobResultPath
  ) {
    throw new Error(
      'GRAPHQL_JOB_QUERY and GRAPHQL_JOBS_PATH are required'
    );
  }

  // Geographic resolution is optional.
  if (E.GEO_ADDRESS_REQUEST) {
    const geo = await resolveLocation();

    log('location_resolved', {
      country: geo?.country || null,
      postalCode: geo?.postalCode || null,
      municipality: geo?.municipality || null
    });
  }

  const data = await graphql(
    CONFIG.jobQuery,
    CONFIG.jobOperation || undefined,
    readJSON(E.GRAPHQL_JOB_VARIABLES || '{}')
  );

  // Paths are relative to the GraphQL "data" object.
  const records = getPath(
    data,
    CONFIG.jobResultPath
  );

  if (!Array.isArray(records)) {
    throw new Error(
      `No job array at ${CONFIG.jobResultPath}`
    );
  }

  const unique = new Map();

  for (const raw of records) {
    const job = normalizeJob(raw);
    if (!job) continue;

    const previous = unique.get(job.jobId);

    if (!previous) {
      unique.set(job.jobId, job);
    } else {
      for (const key of Object.keys(job)) {
        if (!previous[key] && job[key]) {
          previous[key] = job[key];
        }
      }
    }
  }

  return [...unique.values()];
}

// --------------------------------------------------
// WebSocket — port 8082
// --------------------------------------------------

const wss = new WebSocketServer({
  host: CONFIG.host,
  port: CONFIG.wsPort,
  maxPayload: 65536
});

wss.on('connection', socket => {
  log('receiver_connected');

  socket.send(JSON.stringify({
    event: 'DETECTOR_CONNECTED',
    timestamp: now()
  }));

  socket.on('error', error => {
    log('socket_error', {
      error: error.message
    });
  });
});

function broadcast(job) {
  const message = JSON.stringify({
    event: 'NEW_UK_JOB',
    timestamp: now(),
    job
  });

  for (const socket of wss.clients) {
    if (socket.readyState !== WebSocket.OPEN) continue;

    if (socket.bufferedAmount > 1048576) {
      socket.terminate();
      continue;
    }

    socket.send(message);
  }
}

// --------------------------------------------------
// Job processing
// --------------------------------------------------

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

  if (baselineReady || CONFIG.alertExisting) {
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

// --------------------------------------------------
// Scan scheduling
// --------------------------------------------------

function schedule(ms) {
  if (stopping || status.paused) return;

  clearTimeout(timer);

  status.nextScan = new Date(
    Date.now() + ms
  ).toISOString();

  timer = setTimeout(() => {
    void scan();
  }, ms);
}

async function scan() {
  if (scanning || stopping || status.paused) return;

  scanning = true;
  status.state = 'scanning';

  try {
    const jobs = await fetchJobs();

    processJobs(jobs);

    status.state = 'monitoring';
    status.lastError = null;

    log('scan_complete', {
      visible: jobs.length,
      newJobs: status.lastNewJobs
    });

    schedule(nextInterval());
  } catch (error) {
    status.lastError = error.message;

    if (error.accessBlocked) {
      status.paused = true;
      status.security = 'access_restricted';
      status.state = 'paused';
      status.nextScan = null;

      log('access_paused', {
        error: error.message,
        httpStatus: error.httpStatus
      });
    } else {
      if (error.authorizationRequired) {
        sessionReady = false;
        csrfToken = null;
        status.initialized = false;
      }

      status.state = 'retry_wait';

      log('scan_failed', {
        error: error.message
      });

      schedule(60 * 60 * 1000);
    }
  } finally {
    scanning = false;
  }
}

// --------------------------------------------------
// Health server — port 5002
// --------------------------------------------------

const server = http.createServer((req, res) => {
  const url = new URL(
    req.url,
    `http://${CONFIG.host}:${CONFIG.httpPort}`
  );

  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');

  if (
    url.pathname === '/health' ||
    url.pathname === '/status'
  ) {
    res.writeHead(status.paused ? 503 : 200);

    res.end(JSON.stringify({
      ...status,
      websocketClients: wss.clients.size,
      uptimeSeconds: Math.floor(process.uptime())
    }));

    return;
  }

  if (url.pathname === '/jobs') {
    const limit = Math.min(
      500,
      Math.max(
        1,
        Number(url.searchParams.get('limit')) || 100
      )
    );

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
  res.end(JSON.stringify({ error: 'Not found' }));
});

async function main() {
  loadState();

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(
      CONFIG.httpPort,
      CONFIG.host,
      resolve
    );
  });

  log('detector_started', {
    mode: 'graphql',
    healthPort: CONFIG.httpPort,
    websocketPort: CONFIG.wsPort
  });

  if (
    !CONFIG.jobQuery ||
    !CONFIG.jobResultPath
  ) {
    status.state = 'configuration_required';
    status.lastError =
      'Missing GraphQL job operation or result path';

    log('configuration_required');
    return;
  }

  await scan();
}

function shutdown() {
  stopping = true;
  clearTimeout(timer);

  for (const socket of wss.clients) {
    socket.terminate();
  }

  wss.close();
  server.close();
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);

main().catch(error => {
  log('fatal', { error: error.message });
  process.exitCode = 1;
});
