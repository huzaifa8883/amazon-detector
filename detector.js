
import 'dotenv/config';
import { chromium } from 'playwright';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const E = process.env;

const SITE = 'https://www.jobsatamazon.co.uk';
const JOB_URL = E.JOB_URL || `${SITE}/app#/jobSearch`;

const PORT = Number(E.PORT || 5002);
const WS_PORT = Number(E.WS_PORT || 8082);
const HOST = E.BIND_HOST || '127.0.0.1';

const MIN_SCAN = Math.max(60000, Number(E.MIN_SCAN_MS || 60000));
const MAX_SCAN = Math.max(MIN_SCAN, Number(E.MAX_SCAN_MS || 120000));

const STATE_FILE = path.join(ROOT, 'seen_jobs.json');

const log = (event, details = {}) => {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    service: 'amazon-uk-hourly-detector',
    event,
    ...details
  }));
};

function validateTarget(value) {
  const url = new URL(value);

  if (url.protocol !== 'https:' ||
      !['jobsatamazon.co.uk', 'www.jobsatamazon.co.uk']
        .includes(url.hostname)) {
    throw new Error('Only jobsatamazon.co.uk URLs are allowed');
  }

  return url.href;
}

const TARGET = validateTarget(JOB_URL);

const status = {
  service: 'amazon-uk-hourly-detector',
  startedAt: new Date().toISOString(),
  source: TARGET,
  browser: 'starting',
  paused: false,
  security: null,
  lastHTTP: null,
  lastScan: null,
  lastError: null,
  visibleJobs: 0,
  totalSeen: 0,
  lastNewJobs: 0,
  nextScan: null
};

let jobs = new Map();
let baselineComplete = false;
let browser = null;
let context = null;
let page = null;
let timer = null;
let stopping = false;
let scanning = false;

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function interval() {
  return MIN_SCAN +
    Math.floor(Math.random() * (MAX_SCAN - MIN_SCAN + 1));
}

async function loadState() {
  try {
    const saved = JSON.parse(
      await fs.readFile(STATE_FILE, 'utf8')
    );

    if (!Array.isArray(saved.jobs)) {
      throw new Error('Invalid saved state');
    }

    jobs = new Map(
      saved.jobs
        .filter(x => x?.jobId)
        .map(x => [String(x.jobId), x])
    );

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
      savedAt: new Date().toISOString(),
      jobs: [...jobs.values()]
    }, null, 2),
    { mode: 0o600 }
  );

  await fs.rename(temp, STATE_FILE);
}

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${HOST}:${PORT}`);

  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');

  if (url.pathname === '/health' ||
      url.pathname === '/status') {
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
      Math.max(1, Number(url.searchParams.get('limit')) || 100)
    );

    const records = [...jobs.values()]
      .slice(-limit)
      .reverse();

    res.writeHead(200);
    res.end(JSON.stringify({
      total: jobs.size,
      jobs: records
    }));
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

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

  socket.on('error', err => {
    log('receiver_socket_error', { error: err.message });
  });

  socket.send(JSON.stringify({
    event: 'DETECTOR_CONNECTED',
    timestamp: new Date().toISOString()
  }));
});

function broadcast(job) {
  const data = JSON.stringify({
    event: 'NEW_UK_JOB',
    timestamp: new Date().toISOString(),
    job
  });

  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;

    if (client.bufferedAmount > 1024 * 1024) {
      client.terminate();
      continue;
    }

    client.send(data);
  }
}

async function startBrowser() {
  browser = await chromium.launch({
    headless: E.HEADLESS !== 'false',
    chromiumSandbox: E.CHROMIUM_NO_SANDBOX !== 'true'
  });

  context = await browser.newContext({
    locale: 'en-GB',
    timezoneId: 'Europe/London',
    viewport: { width: 1366, height: 768 },
    acceptDownloads: false
  });

  page = await context.newPage();
  status.browser = 'ready';
}

async function extractVisibleJobs() {
  return await page.evaluate(() => {
    const clean = value =>
      String(value || '').replace(/\s+/g, ' ').trim();

    const selector = [
      '[data-job-id]',
      '[data-jobid]',
      '[data-testid*="job-card" i]',
      '[class*="job-card" i]',
      '[class*="jobcard" i]',
      '[class*="job-listing" i]'
    ].join(',');

    const cards = [...document.querySelectorAll(selector)];
    const result = [];

    for (const card of cards) {
      const text = clean(card.innerText || card.textContent);

      const title = clean(
        card.querySelector(
          'h1,h2,h3,h4,[data-testid*="title" i],' +
          '[class*="job-title" i]'
        )?.textContent
      );

      const link = card.querySelector('a[href]');

      const url = link?.href || '';

      const jobId = clean(
        card.getAttribute('data-job-id') ||
        card.getAttribute('data-jobid') ||
        card.querySelector('[data-job-id]')
          ?.getAttribute('data-job-id') ||
        text.match(
          /(?:Job\s*ID|Job\s*Reference)\s*[:#]?\s*([\w-]+)/i
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
          /£\s*\d+(?:\.\d{1,2})?\s*(?:\/\s*hr|per hour)/i
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
        applyUrl: url || null
      });
    }

    return result;
  });
}

function normalizeJob(job) {
  const jobId = String(job.jobId || '').trim();
  const title = String(job.title || '').trim();

  if (!jobId || !title) return null;

  let applyUrl = null;

  try {
    if (job.applyUrl) {
      const url = new URL(job.applyUrl, TARGET);

      if (
        url.protocol === 'https:' &&
        ['jobsatamazon.co.uk', 'www.jobsatamazon.co.uk']
          .includes(url.hostname)
      ) {
        applyUrl = url.href;
      }
    }
  } catch {}

  return {
    jobId,
    title,
    location: job.location || null,
    postcode: job.postcode || null,
    payRate: job.payRate || null,
    shiftDetails: job.shiftDetails || null,
    shifts: job.shiftDetails || null,
    applyUrl
  };
}

async function scan() {
  if (scanning || stopping || status.paused) return;

  scanning = true;
  status.browser = 'scanning';
  status.nextScan = null;

  try {
    if (!browser?.isConnected()) {
      await context?.close().catch(() => {});
      await browser?.close().catch(() => {});
      await startBrowser();
    }

    const response = await page.goto(TARGET, {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });

    const code = response?.status() ?? 0;
    status.lastHTTP = code;

    if (code === 403 || code === 429) {
      status.paused = true;
      status.security = 'access_restricted';
      status.browser = 'paused';

      log('access_restricted', { httpStatus: code });
      return;
    }

    if (code < 200 || code >= 300) {
      throw new Error(`Website HTTP ${code}`);
    }

    await page.waitForTimeout(2000);

    const bodyText = await page.locator('body')
      .innerText({ timeout: 5000 })
      .catch(() => '');

    if (/access denied|access restricted|captcha|unusual traffic/i
        .test(bodyText.slice(0, 15000))) {
      status.paused = true;
      status.security = 'access_restricted';
      status.browser = 'paused';
      return;
    }

    const extracted = await extractVisibleJobs();
    const unique = new Map();

    for (const raw of extracted) {
      const job = normalizeJob(raw);
      if (job) unique.set(job.jobId, job);
    }

    const newJobs = [...unique.values()].filter(
      job => !jobs.has(job.jobId)
    );

    // Persist before sending signals.
    const added = [];

    for (const job of newJobs) {
      jobs.set(job.jobId, {
        ...job,
        discoveredAt: new Date().toISOString()
      });
      added.push(job.jobId);
    }

    try {
      await saveState();
    } catch (error) {
      for (const id of added) jobs.delete(id);
      throw error;
    }

    status.visibleJobs = unique.size;
    status.totalSeen = jobs.size;
    status.lastNewJobs = newJobs.length;
    status.lastScan = new Date().toISOString();
    status.lastError = null;
    status.browser = 'monitoring';

    if (baselineComplete || E.ALERT_EXISTING === 'true') {
      for (const job of newJobs) {
        broadcast(job);
        log('new_job_detected', {
          jobId: job.jobId,
          title: job.title,
          location: job.location
        });
      }
    } else {
      log('initial_baseline', { jobs: newJobs.length });
    }

    baselineComplete = true;

    log('scan_complete', {
      visible: unique.size,
      newJobs: newJobs.length,
      total: jobs.size
    });
  } catch (error) {
    status.browser = 'error';
    status.lastError = error.message;
    log('scan_error', { error: error.message });
  } finally {
    scanning = false;
  }
}

async function loop() {
  while (!stopping && !status.paused) {
    await scan();

    if (stopping || status.paused) break;

    const delay = interval();

    status.nextScan = new Date(
      Date.now() + delay
    ).toISOString();

    await wait(delay);
  }
}

async function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, HOST, resolve);
  });
}

async function main() {
  await loadState();

  await listen(httpServer, PORT);
  await listen(wsHttp, WS_PORT);

  log('servers_ready', {
    healthPort: PORT,
    websocketPort: WS_PORT,
    target: TARGET
  });

  await startBrowser();
  await loop();
}

async function shutdown() {
  if (stopping) return;
  stopping = true;

  clearTimeout(timer);

  for (const client of wss.clients) {
    client.terminate();
  }

  await context?.close().catch(() => {});
  await browser?.close().catch(() => {});

  wss.close();
  wsHttp.close();
  httpServer.close();
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

main().catch(error => {
  log('fatal', { error: error.message });
  process.exitCode = 1;
  void shutdown();
});
