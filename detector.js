
import 'dotenv/config';

import { chromium } from 'playwright';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const E = process.env;

const CONFIG = {
  url: 'https://www.jobsatamazon.co.uk/app#/jobSearch',
  httpPort: Number(E.PORT || 5002),
  wsPort: Number(E.WS_PORT || 8082),
  host: E.BIND_HOST || '127.0.0.1',

  minScan: 15 * 60 * 1000,
  maxScan: 30 * 60 * 1000,

  minDelay: 3000,
  maxDelay: 7000,

  minCooldown: 60 * 60 * 1000,
  maxCooldown: 120 * 60 * 1000,

  navigationTimeout: 45000,
  headless: E.HEADLESS !== 'false',
  noSandbox: E.CHROMIUM_NO_SANDBOX === 'true',

  stateFile: path.join(ROOT, 'seen_jobs.json')
};

const now = () => new Date().toISOString();

const random = (min, max) =>
  Math.floor(Math.random() * (max - min + 1)) + min;

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

function log(event, details = {}) {
  console.log(JSON.stringify({
    time: now(),
    service: 'amazon-detector',
    event,
    ...details
  }));
}

const state = {
  service: 'amazon-uk-hourly-detector',
  startedAt: now(),
  source: CONFIG.url,
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
  cooldownUntil: null,
  pageURL: null,
  diagnostics: {}
};

let browser = null;
let context = null;
let page = null;
let timer = null;

let jobs = new Map();
let baselineComplete = false;
let stopping = false;
let scanning = false;

async function loadState() {
  try {
    const saved = JSON.parse(
      await fs.readFile(CONFIG.stateFile, 'utf8')
    );

    if (!Array.isArray(saved.jobs)) {
      throw new Error('Invalid state file');
    }

    jobs = new Map(
      saved.jobs
        .filter(job => job?.jobId)
        .map(job => [String(job.jobId), job])
    );

    baselineComplete = true;
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }

  state.totalSeen = jobs.size;
}

async function saveState() {
  const temp = CONFIG.stateFile + '.tmp';

  await fs.writeFile(
    temp,
    JSON.stringify({
      version: 1,
      updatedAt: now(),
      jobs: [...jobs.values()]
    }, null, 2),
    { mode: 0o600 }
  );

  await fs.rename(temp, CONFIG.stateFile);
}

// WebSocket signal server
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

  wss.handleUpgrade(req, socket, head, client => {
    wss.emit('connection', client, req);
  });
});

wss.on('connection', socket => {
  log('receiver_connected');

  socket.on('error', error => {
    log('socket_error', {
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

  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;

    if (client.bufferedAmount > 1024 * 1024) {
      client.terminate();
      continue;
    }

    client.send(payload);
  }
}

// Browser management
async function startBrowser() {
  browser = await chromium.launch({
    headless: CONFIG.headless,
    chromiumSandbox: !CONFIG.noSandbox
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

  page.on('pageerror', error => {
    state.diagnostics.lastPageError = error.message;
  });

  state.browser = 'ready';
}

async function closeBrowser() {
  await context?.close().catch(() => {});
  await browser?.close().catch(() => {});

  context = null;
  browser = null;
  page = null;
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
      '[class*="job-card" i]',
      '[class*="jobcard" i]',
      '[class*="job-listing" i]'
    ].join(',');

    const cards = [
      ...document.querySelectorAll(selectors)
    ];

    const result = [];

    for (const card of cards) {
      const text = clean(
        card.innerText || card.textContent
      );

      const title = clean(
        card.querySelector(
          'h1,h2,h3,h4,' +
          '[data-testid*="title" i],' +
          '[class*="job-title" i]'
        )?.textContent
      );

      const link = card.querySelector('a[href]');

      const jobId = clean(
        card.getAttribute('data-job-id') ||
        card.getAttribute('data-jobid') ||
        card.querySelector('[data-job-id]')
          ?.getAttribute('data-job-id') ||
        text.match(
          /(?:Job ID|Job Reference)\s*[:#]?\s*([\w-]+)/i
        )?.[1]
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
        )?.textContent
      );

      const payRate = clean(
        card.querySelector(
          '[data-testid*="pay" i],' +
          '[class*="pay-rate" i],' +
          '[class*="salary" i]'
        )?.textContent ||
        text.match(
          /£\s*\d+(?:\.\d{1,2})?\s*(?:per hour|\/hr)/i
        )?.[0]
      );

      const shiftDetails = clean(
        card.querySelector(
          '[data-testid*="shift" i],' +
          '[class*="shift" i],' +
          '[class*="schedule" i]'
        )?.textContent
      );

      result.push({
        jobId,
        title,
        location: location || null,
        postcode: postcode || null,
        payRate: payRate || null,
        shiftDetails: shiftDetails || null,
        shifts: shiftDetails || null,
        applyUrl: link?.href || null
      });
    }

    return {
      jobs: result,
      candidateCards: cards.length,
      title: document.title,
      preview: clean(document.body?.innerText).slice(0, 350)
    };
  });
}

// Pause when access is explicitly refused
function pauseForRestriction(reason, httpCode = null) {
  clearTimeout(timer);

  const cooldown = random(
    CONFIG.minCooldown,
    CONFIG.maxCooldown
  );

  state.paused = true;
  state.browser = 'paused';
  state.security = reason;
  state.lastHTTP = httpCode ?? state.lastHTTP;

  state.cooldownUntil = new Date(
    Date.now() + cooldown
  ).toISOString();

  state.nextScan = null;

  log('access_restricted', {
    reason,
    httpCode,
    suggestedCooldownUntil: state.cooldownUntil,
    requiresManualResume: true
  });
}

function scheduleNext(delayMs = null) {
  clearTimeout(timer);

  if (stopping || state.paused) return;

  const ms = delayMs ?? random(
    CONFIG.minScan,
    CONFIG.maxScan
  );

  state.nextScan = new Date(
    Date.now() + ms
  ).toISOString();

  log('next_scan_scheduled', {
    nextScan: state.nextScan,
    minutes: Math.round(ms / 60000)
  });

  timer = setTimeout(() => {
    void scan();
  }, ms);
}

async function scan() {
  if (stopping || scanning || state.paused) return;

  scanning = true;
  state.browser = 'scanning';
  state.nextScan = null;

  let transientFailure = false;

  try {
    if (!browser?.isConnected()) {
      await closeBrowser();
      await startBrowser();
    }

    // Spacing between application actions
    await sleep(random(
      CONFIG.minDelay,
      CONFIG.maxDelay
    ));

    const response = await page.goto(CONFIG.url, {
      waitUntil: 'domcontentloaded',
      timeout: CONFIG.navigationTimeout
    });

    state.lastHTTP = response?.status() ?? null;
    state.pageURL = page.url();

    if (
      state.lastHTTP === 403 ||
      state.lastHTTP === 429
    ) {
      pauseForRestriction(
        'access_restricted',
        state.lastHTTP
      );
      return;
    }

    if (
      !response ||
      !response.ok()
    ) {
      throw new Error(
        `Website HTTP ${state.lastHTTP}`
      );
    }

    // Allow the SPA to render its public search results.
    await sleep(random(
      CONFIG.minDelay,
      CONFIG.maxDelay
    ));

    const body = await page.locator('body')
      .innerText({ timeout: 5000 })
      .catch(() => '');

    if (
      /access denied|access restricted|captcha|unusual traffic/i
        .test(body.slice(0, 15000))
    ) {
      pauseForRestriction(
        'restriction_page'
      );
      return;
    }

    const snapshot = await extractJobs();

    const found = new Map();

    for (const job of snapshot.jobs) {
      const id = String(job.jobId || '').trim();
      if (!id || !job.title) continue;

      // Restrict job links to official hourly domain.
      if (job.applyUrl) {
        try {
          const u = new URL(job.applyUrl, CONFIG.url);
          const valid =
            u.protocol === 'https:' &&
            (
              u.hostname === 'jobsatamazon.co.uk' ||
              u.hostname === 'www.jobsatamazon.co.uk'
            );

          job.applyUrl = valid ? u.href : null;
        } catch {
          job.applyUrl = null;
        }
      }

      found.set(id, { ...job, jobId: id });
    }

    const newJobs = [
      ...found.values()
    ].filter(job => !jobs.has(job.jobId));

    const ids = [];

    for (const job of newJobs) {
      jobs.set(job.jobId, {
        ...job,
        discoveredAt: now()
      });

      ids.push(job.jobId);
    }

    if (ids.length > 0) {
      try {
        await saveState();
      } catch (error) {
        for (const id of ids) jobs.delete(id);
        throw error;
      }
    }

    state.visibleJobs = found.size;
    state.totalSeen = jobs.size;
    state.lastNewJobs = newJobs.length;
    state.lastScan = now();
    state.lastError = null;
    state.security = null;
    state.browser = 'monitoring';

    state.diagnostics = {
      candidateCards: snapshot.candidateCards,
      pageTitle: snapshot.title,
      textPreview: snapshot.preview
    };

    if (
      baselineComplete ||
      E.ALERT_EXISTING === 'true'
    ) {
      for (const job of newJobs) {
        broadcast(job);

        log('new_job_detected', {
          jobId: job.jobId,
          title: job.title,
          location: job.location
        });
      }
    } else {
      log('initial_baseline', {
        jobs: newJobs.length
      });
    }

    baselineComplete = true;

    log('scan_complete', {
      found: found.size,
      newJobs: newJobs.length,
      totalSeen: jobs.size
    });
  } catch (error) {
    transientFailure = true;

    state.browser = 'error';
    state.lastError = error.message;

    log('scan_failed', {
      error: error.message
    });
  } finally {
    scanning = false;

    if (!stopping && !state.paused) {
      // Network errors get a much longer retry interval.
      scheduleNext(
        transientFailure
          ? random(
              CONFIG.minCooldown,
              CONFIG.maxCooldown
            )
          : null
      );
    }
  }
}

// Health and job APIs
const healthServer = http.createServer((req, res) => {
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
    res.writeHead(state.paused ? 503 : 200);

    res.end(JSON.stringify({
      ...state,
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
  res.end(JSON.stringify({
    error: 'Not found'
  }));
});

async function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, CONFIG.host, resolve);
  });
}

async function main() {
  await loadState();

  await listen(healthServer, CONFIG.httpPort);
  await listen(wsHttp, CONFIG.wsPort);

  log('servers_started', {
    httpPort: CONFIG.httpPort,
    wsPort: CONFIG.wsPort,
    source: CONFIG.url
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

  await closeBrowser();

  wss.close();
  wsHttp.close();
  healthServer.close();

  log('shutdown_complete');
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

main().catch(error => {
  log('fatal', { error: error.message });
  process.exitCode = 1;
  void shutdown();
});
