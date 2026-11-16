
import 'dotenv/config';
import { chromium } from 'playwright';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const env = process.env;

const BASE_URL = 'https://www.jobsatamazon.co.uk';
const SEARCH_URL = `${BASE_URL}/app#/jobSearch`;

const PORT = Number(env.PORT || 5002);
const WS_PORT = Number(env.WS_PORT || 8082);
const HOST = env.BIND_HOST || '127.0.0.1';

const MIN_SCAN_MS = Math.max(
  60000,
  Number(env.MIN_SCAN_MS || 60000)
);

const MAX_SCAN_MS = Math.max(
  MIN_SCAN_MS,
  Number(env.MAX_SCAN_MS || 120000)
);

const STATE_FILE = path.join(ROOT, 'seen_jobs.json');
const SCAN_TIMEOUT_MS = 30000;

const status = {
  service: 'amazon-uk-hourly-detector',
  source: SEARCH_URL,
  startedAt: new Date().toISOString(),
  browser: 'starting',
  paused: false,
  security: null,
  lastHTTP: null,
  lastScan: null,
  lastError: null,
  visibleJobs: 0,
  totalSeen: 0,
  lastNewJobs: 0,
  nextScan: null,
  pageURL: null,
  diagnostics: {}
};

let browser;
let context;
let page;
let timer;
let stopping = false;
let scanning = false;
let baselineComplete = false;

const jobs = new Map();

function log(event, details = {}) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    service: 'amazon-detector',
    event,
    ...details
  }));
}

function delay() {
  return MIN_SCAN_MS +
    Math.floor(
      Math.random() *
      (MAX_SCAN_MS - MIN_SCAN_MS + 1)
    );
}

async function loadState() {
  try {
    const data = JSON.parse(
      await fs.readFile(STATE_FILE, 'utf8')
    );

    if (!Array.isArray(data.jobs)) {
      throw Error('Invalid saved jobs format');
    }

    for (const job of data.jobs) {
      if (job.jobId) {
        jobs.set(String(job.jobId), job);
      }
    }

    baselineComplete = true;
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }

  status.totalSeen = jobs.size;
}

async function saveState() {
  const temp = STATE_FILE + '.tmp';

  await fs.writeFile(
    temp,
    JSON.stringify({
      version: 1,
      updatedAt: new Date().toISOString(),
      jobs: [...jobs.values()]
    }, null, 2),
    { mode: 0o600 }
  );

  await fs.rename(temp, STATE_FILE);
}

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 65536
});

const wsHTTP = http.createServer((req, res) => {
  res.writeHead(404).end();
});

wsHTTP.on('upgrade', (req, socket, head) => {
  if (req.url !== '/') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, client => {
    wss.emit('connection', client, req);
  });
});

wss.on('connection', socket => {
  log('receiver_connected');

  socket.on('error', error => {
    log('receiver_socket_error', {
      error: error.message
    });
  });

  socket.send(JSON.stringify({
    event: 'DETECTOR_CONNECTED',
    timestamp: new Date().toISOString()
  }));
});

function broadcast(job) {
  const message = JSON.stringify({
    event: 'NEW_UK_JOB',
    timestamp: new Date().toISOString(),
    job
  });

  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;

    if (client.bufferedAmount > 1048576) {
      client.terminate();
      continue;
    }

    client.send(message);
  }
}

async function startBrowser() {
  browser = await chromium.launch({
    headless: true,
    chromiumSandbox:
      env.CHROMIUM_NO_SANDBOX !== 'true'
  });

  context = await browser.newContext({
    viewport: {
      width: 1366,
      height: 768
    },
    locale: 'en-GB',
    timezoneId: 'Europe/London',
    acceptDownloads: false
  });

  page = await context.newPage();

  // Diagnostics only: record public endpoint response
  // metadata, never request bodies or session tokens.
  page.on('response', response => {
    const url = response.url();

    if (!url.startsWith(BASE_URL)) return;

    if (/graphql|appsync|job|search/i.test(url)) {
      status.diagnostics.lastNetworkResponse = {
        path: new URL(url).pathname,
        httpStatus: response.status(),
        at: new Date().toISOString()
      };
    }
  });

  page.on('pageerror', error => {
    status.diagnostics.lastPageError = error.message;
  });

  status.browser = 'ready';
}

async function navigateSearch() {
  const response = await page.goto(SEARCH_URL, {
    waitUntil: 'domcontentloaded',
    timeout: SCAN_TIMEOUT_MS
  });

  status.lastHTTP = response?.status() ?? null;
  status.pageURL = page.url();

  if (
    status.lastHTTP === 403 ||
    status.lastHTTP === 429
  ) {
    status.paused = true;
    status.security = 'access_restricted';
    status.browser = 'paused';
    return false;
  }

  if (
    status.lastHTTP === null ||
    status.lastHTTP < 200 ||
    status.lastHTTP >= 300
  ) {
    throw Error(
      `Navigation HTTP ${status.lastHTTP}`
    );
  }

  // SPA rendering can continue after DOMContentLoaded.
  // Wait for relevant content, but don't assume jobs exist.
  await page.waitForFunction(() => {
    const text = document.body?.innerText || '';

    return (
      document.querySelectorAll(
        '[data-job-id],[data-jobid],' +
        '[data-testid*="job-card" i],' +
        '[class*="job-card" i]'
      ).length > 0 ||
      /no jobs|no results|jobs found|job search/i.test(text)
    );
  }, { timeout: 10000 }).catch(() => {});

  return true;
}

async function checkRestriction() {
  const text = await page.locator('body')
    .innerText({ timeout: 5000 })
    .catch(() => '');

  if (
    /access denied|access restricted|captcha|unusual traffic/i
      .test(text.slice(0, 15000))
  ) {
    status.paused = true;
    status.security = 'access_restricted';
    status.browser = 'paused';
    return true;
  }

  return false;
}

async function extractJobs() {
  return page.evaluate(() => {
    const clean = value =>
      String(value || '')
        .replace(/\s+/g, ' ')
        .trim();

    const selectors = [
      '[data-job-id]',
      '[data-jobid]',
      '[data-testid*="job-card" i]',
      '[data-testid*="job-list" i] article',
      '[class*="job-card" i]',
      '[class*="jobcard" i]',
      '[class*="job-listing" i]'
    ];

    const cards = new Set(
      document.querySelectorAll(selectors.join(','))
    );

    const results = [];

    for (const card of cards) {
      const text = clean(
        card.innerText || card.textContent
      );

      if (text.length < 8) continue;

      const titleElement = card.querySelector(
        'h1,h2,h3,h4,' +
        '[data-testid*="title" i],' +
        '[class*="job-title" i]'
      );

      const title = clean(titleElement?.textContent);

      const links = Array.from(
        card.querySelectorAll('a[href]')
      );

      const jobLink = links.find(link =>
        /job|apply|detail/i.test(link.href)
      );

      const explicitId =
        card.getAttribute('data-job-id') ||
        card.getAttribute('data-jobid') ||
        card.querySelector('[data-job-id]')
          ?.getAttribute('data-job-id');

      const textId = text.match(
        /\b(?:Job\s*ID|Job\s*Reference)\s*[:#]?\s*([a-z0-9_-]+)/i
      )?.[1];

      let urlId = null;

      if (jobLink) {
        const u = new URL(jobLink.href);

        urlId =
          u.searchParams.get('jobId') ||
          u.searchParams.get('job_id') ||
          u.pathname.match(/\/jobs?\/([^/?#]+)/i)?.[1];
      }

      const jobId = clean(
        explicitId || textId || urlId
      );

      if (!jobId || !title) continue;

      const location = clean(
        card.querySelector(
          '[data-testid*="location" i],' +
          '[class*="location" i]'
        )?.textContent
      );

      const postcode = clean(
        card.querySelector(
          '[data-testid*="postcode" i],' +
          '[class*="postcode" i]'
        )?.textContent ||
        text.match(
          /\b(?:[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i
        )?.[0]
      );

      const payRate = clean(
        card.querySelector(
          '[data-testid*="pay" i],' +
          '[class*="pay-rate" i],' +
          '[class*="salary" i]'
        )?.textContent ||
        text.match(
          /£\s*\d+(?:\.\d{1,2})?\s*(?:per hour|\/hr|\/hour)/i
        )?.[0]
      );

      const shiftDetails = clean(
        card.querySelector(
          '[data-testid*="shift" i],' +
          '[class*="shift" i],' +
          '[class*="schedule" i]'
        )?.textContent
      );

      results.push({
        jobId,
        title,
        location: location || null,
        postcode: postcode || null,
        payRate: payRate || null,
        shiftDetails: shiftDetails || null,
        shifts: shiftDetails || null,
        applyUrl: jobLink?.href || null
      });
    }

    return {
      jobs: results,
      candidateCards: cards.size,
      pageTitle: document.title,
      textPreview: clean(
        document.body?.innerText
      ).slice(0, 400)
    };
  });
}

function normalizeJobs(rawJobs) {
  const normalized = new Map();

  for (const raw of rawJobs) {
    const jobId = String(raw.jobId || '').trim();
    const title = String(raw.title || '').trim();

    if (!jobId || !title) continue;

    let applyUrl = null;

    try {
      if (raw.applyUrl) {
        const url = new URL(raw.applyUrl, SEARCH_URL);

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

    const job = {
      ...raw,
      jobId,
      title,
      applyUrl
    };

    const previous = normalized.get(jobId);

    if (previous) {
      for (const key of Object.keys(job)) {
        if (!previous[key] && job[key]) {
          previous[key] = job[key];
        }
      }
    } else {
      normalized.set(jobId, job);
    }
  }

  return [...normalized.values()];
}

async function scan() {
  if (scanning || stopping || status.paused) return;

  scanning = true;
  status.browser = 'scanning';

  try {
    if (!browser?.isConnected()) {
      await context?.close().catch(() => {});
      await browser?.close().catch(() => {});
      await startBrowser();
    }

    const accessible = await navigateSearch();

    if (!accessible || await checkRestriction()) {
      log('access_restricted', {
        httpStatus: status.lastHTTP
      });
      return;
    }

    const snapshot = await extractJobs();
    const found = normalizeJobs(snapshot.jobs);

    status.diagnostics = {
      ...status.diagnostics,
      candidateCards: snapshot.candidateCards,
      pageTitle: snapshot.pageTitle,
      textPreview: snapshot.textPreview
    };

    const fresh = found.filter(
      job => !jobs.has(job.jobId)
    );

    const added = [];

    for (const job of fresh) {
      jobs.set(job.jobId, {
        ...job,
        discoveredAt: new Date().toISOString()
      });
      added.push(job.jobId);
    }

    if (added.length) {
      try {
        await saveState();
      } catch (error) {
        for (const id of added) jobs.delete(id);
        throw error;
      }
    }

    status.visibleJobs = found.length;
    status.totalSeen = jobs.size;
    status.lastNewJobs = fresh.length;
    status.lastScan = new Date().toISOString();
    status.lastError = null;
    status.browser = 'monitoring';

    if (
      baselineComplete ||
      env.ALERT_EXISTING === 'true'
    ) {
      for (const job of fresh) {
        broadcast(job);
        log('new_job_detected', {
          jobId: job.jobId,
          title: job.title,
          location: job.location
        });
      }
    } else {
      log('initial_baseline', {
        jobs: fresh.length
      });
    }

    baselineComplete = true;

    log('scan_complete', {
      jobsFound: found.length,
      newJobs: fresh.length,
      totalSeen: jobs.size,
      candidateCards: snapshot.candidateCards
    });
  } catch (error) {
    status.browser = 'error';
    status.lastError = error.message;

    log('scan_error', {
      error: error.message
    });
  } finally {
    scanning = false;
    scheduleNext();
  }
}

function scheduleNext() {
  clearTimeout(timer);

  if (stopping || status.paused) return;

  const ms = delay();

  status.nextScan = new Date(
    Date.now() + ms
  ).toISOString();

  timer = setTimeout(() => {
    void scan();
  }, ms);
}

const httpServer = http.createServer((req, res) => {
  const url = new URL(
    req.url,
    `http://${HOST}:${PORT}`
  );

  res.setHeader('content-type', 'application/json');
  res.setHeader('cache-control', 'no-store');

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
    const n = Number(url.searchParams.get('limit'));

    const limit = Number.isFinite(n) && n > 0
      ? Math.min(500, Math.floor(n))
      : 100;

    res.writeHead(200);

    res.end(JSON.stringify({
      total: jobs.size,
      jobs: [...jobs.values()]
        .slice(-limit)
        .reverse()
    }));
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

async function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, HOST, resolve);
  });
}

async function main() {
  await loadState();

  await listen(httpServer, PORT);
  await listen(wsHTTP, WS_PORT);

  log('servers_started', {
    healthPort: PORT,
    websocketPort: WS_PORT,
    searchURL: SEARCH_URL
  });

  await startBrowser();
  await scan();
}

async function shutdown() {
  if (stopping) return;

  stopping = true;
  clearTimeout(timer);

  for (const socket of wss.clients) {
    socket.terminate();
  }

  await context?.close().catch(() => {});
  await browser?.close().catch(() => {});

  wss.close();
  wsHTTP.close();
  httpServer.close();
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
  void shutdown();
});
