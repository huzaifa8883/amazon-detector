
import 'dotenv/config';

import axios from 'axios';
import { wrapper } from 'axios-cookiejar-support';
import { CookieJar } from 'tough-cookie';

import fs from 'node:fs/promises';

const E = process.env;

const BASE = E.API_BASE_URL;
const CSRF_PATH = E.CSRF_PATH || '/csrf?countryCode=UK';
const AUTH_PATH = E.AUTH_PATH || '/authorize?countryCode=UK';
const GRAPHQL_PATH = E.GRAPHQL_PATH || '/graphql';

const TIMEOUT_MS = 20000;
const POLL_MS = Math.max(
  15 * 60 * 1000,
  Number(E.POLL_MS || 900000)
);

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

const now = () => new Date().toISOString();

function log(event, details = {}) {
  console.log(JSON.stringify({
    timestamp: now(),
    service: 'amazon-api-detector',
    event,
    ...details
  }));
}

if (!BASE) {
  throw new Error('API_BASE_URL is required');
}

const baseURL = new URL(BASE);

if (
  baseURL.protocol !== 'https:' ||
  !['jobsatamazon.co.uk',
     'www.jobsatamazon.co.uk']
    .includes(baseURL.hostname)
) {
  throw new Error(
    'API_BASE_URL must be the official UK hourly domain'
  );
}

const jar = new CookieJar();

const client = wrapper(axios.create({
  baseURL: baseURL.origin,
  jar,
  withCredentials: true,
  timeout: TIMEOUT_MS,
  maxRedirects: 0,
  validateStatus: () => true,
  headers: {
    Accept: 'application/json'
  }
}));

let csrfToken = null;
let sessionReady = false;
let stopping = false;
let blocked = false;

const seen = new Set();

function assertAccess(response, operation) {
  const status = response.status;

  if (status === 403 || status === 429) {
    blocked = true;

    const error = new Error(
      `${operation}: access refused (HTTP ${status})`
    );

    error.code = 'ACCESS_REFUSED';
    throw error;
  }

  if (status >= 300 && status < 400) {
    throw new Error(
      `${operation}: redirect received; authorization may require interactive login`
    );
  }

  if (status < 200 || status >= 300) {
    const error = new Error(
      `${operation}: HTTP ${status}`
    );

    error.status = status;
    throw error;
  }

  return response.data;
}

async function initializeSession() {
  log('session_initializing');

  // Cookies are automatically retained in the cookie jar.
  const csrfResponse = await client.get(CSRF_PATH);

  const csrfData = assertAccess(
    csrfResponse,
    'CSRF initialization'
  );

  const cookie = E.CSRF_COOKIE_NAME
    ? await jar.getCookies(baseURL.origin)
    : [];

  const matchingCookie = cookie.find(
    c => c.key === E.CSRF_COOKIE_NAME
  );

  csrfToken =
    csrfResponse.headers[E.CSRF_RESPONSE_HEADER || 'x-csrf-token'] ||
    csrfData?.csrfToken ||
    csrfData?.csrf ||
    matchingCookie?.value ||
    null;

  if (E.REQUIRE_CSRF === 'true' && !csrfToken) {
    throw new Error('CSRF token not returned');
  }

  // Only works if this endpoint permits noninteractive
  // access for the current session.
  const authResponse = await client.get(AUTH_PATH, {
    headers: csrfToken
      ? {
          [E.CSRF_REQUEST_HEADER || 'x-csrf-token']:
            csrfToken
        }
      : {}
  });

  assertAccess(authResponse, 'Session authorization');

  sessionReady = true;

  log('session_initialized');
}

function normalizeJob(raw) {
  if (!raw || typeof raw !== 'object') return null;

  const jobId = String(
    raw.jobId ??
    raw.jobID ??
    raw.job_id ??
    raw.id ??
    ''
  ).trim();

  const title = String(
    raw.title ??
    raw.jobTitle ??
    raw.name ??
    ''
  ).trim();

  if (!jobId || !title) return null;

  const value = (...candidates) =>
    candidates.find(x => x !== undefined &&
      x !== null && x !== '') ?? null;

  return {
    jobId,
    title,

    location: value(
      raw.location?.city,
      raw.location?.name,
      raw.location,
      raw.city,
      raw.siteName
    ),

    postcode: value(
      raw.postcode,
      raw.postalCode,
      raw.zipCode
    ),

    payRate: value(
      raw.payRate,
      raw.hourlyPay,
      raw.compensation?.display
    ),

    shiftDetails: value(
      raw.shiftDetails,
      raw.shifts,
      raw.schedule
    ),

    applyUrl: value(
      raw.applyUrl,
      raw.jobUrl,
      raw.url
    )
  };
}

function readPath(object, dotPath) {
  return dotPath.split('.').reduce(
    (value, key) => value?.[key],
    object
  );
}

async function fetchJobs() {
  if (!sessionReady) {
    await initializeSession();
  }

  // Supply an actual authorized GraphQL operation.
  // Never guess the operation name or schema.
  const query = await fs.readFile(
    E.GRAPHQL_QUERY_FILE || './jobs.graphql',
    'utf8'
  );

  const variables = E.GRAPHQL_VARIABLES
    ? JSON.parse(E.GRAPHQL_VARIABLES)
    : {};

  const headers = {
    'Content-Type': 'application/json',
    Accept: 'application/json'
  };

  if (csrfToken) {
    headers[E.CSRF_REQUEST_HEADER || 'x-csrf-token'] =
      csrfToken;
  }

  const response = await client.post(
    GRAPHQL_PATH,
    {
      query,
      variables
    },
    { headers }
  );

  const result = assertAccess(
    response,
    'GraphQL request'
  );

  if (Array.isArray(result?.errors) &&
      result.errors.length) {
    throw new Error(
      `GraphQL returned ${result.errors.length} error(s)`
    );
  }

  const jobsPath = E.GRAPHQL_JOBS_PATH;

  if (!jobsPath) {
    throw new Error(
      'GRAPHQL_JOBS_PATH must identify the job list'
    );
  }

  const items = readPath(result, jobsPath);

  if (!Array.isArray(items)) {
    throw new Error(
      `No job array found at ${jobsPath}`
    );
  }

  return items.map(normalizeJob).filter(Boolean);
}

async function processJobs(jobs) {
  let fresh = 0;

  for (const job of jobs) {
    if (seen.has(job.jobId)) continue;

    seen.add(job.jobId);
    fresh++;

    log('new_job', {
      job
    });
  }

  log('scan_complete', {
    received: jobs.length,
    newJobs: fresh,
    seen: seen.size
  });
}

async function run() {
  let failures = 0;

  while (!stopping && !blocked) {
    try {
      const jobs = await fetchJobs();

      await processJobs(jobs);

      failures = 0;
    } catch (error) {
      log('request_failed', {
        error: error.message,
        status: error.status || null
      });

      if (error.code === 'ACCESS_REFUSED') {
        log('access_paused', {
          reason: 'Server refused API access'
        });
        break;
      }

      // Refresh an expired permitted session once
      // on a later cycle, without bypassing 403/429.
      if (error.status === 401) {
        sessionReady = false;
        csrfToken = null;
      }

      failures++;
    }

    const backoff = Math.min(
      60 * 60 * 1000,
      POLL_MS * Math.pow(2, Math.min(failures, 3))
    );

    await sleep(backoff);
  }
}

process.once('SIGTERM', () => {
  stopping = true;
});

process.once('SIGINT', () => {
  stopping = true;
});

run().catch(error => {
  log('fatal', {
    error: error.message
  });

  process.exitCode = 1;
});
