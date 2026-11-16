import 'dotenv/config';
import { chromium } from 'playwright';
import { WebSocketServer, WebSocket } from 'ws';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const env = process.env;
const cfg = {
  target: env.JOB_URL || 'https://www.jobsatamazon.co.uk/',
  mode: env.SOURCE_MODE || 'browser',
  feed: env.PUBLIC_FEED_URL || '',
  bind: env.BIND_HOST || '127.0.0.1',
  port: Number(env.PORT || 5002),
  wsHost: env.WS_HOST || '127.0.0.1',
  wsPort: Number(env.WS_PORT || 8080),
  headless: env.HEADLESS !== 'false',
  noSandbox: env.CHROMIUM_NO_SANDBOX === 'true',
  minMs: Number(env.MIN_SCAN_MS || 60000),
  maxMs: Number(env.MAX_SCAN_MS || 120000),
  timeoutMs: Number(env.REQUEST_TIMEOUT_MS || 30000),
  alertExisting: env.ALERT_EXISTING === 'true',
  file: path.resolve(ROOT, env.STATE_FILE || 'seen_jobs.json')
};
if (!['browser', 'json'].includes(cfg.mode)) throw new Error('SOURCE_MODE must be browser or json');
if (!Number.isSafeInteger(cfg.minMs) || cfg.minMs < 10000 || !Number.isSafeInteger(cfg.maxMs) || cfg.maxMs < cfg.minMs) throw new Error('Invalid scan intervals');
if (cfg.mode === 'json') {
  const u = new URL(cfg.feed);
  if (u.protocol !== 'https:') throw new Error('PUBLIC_FEED_URL must be HTTPS');
}
const stamp = () => new Date().toISOString();
const log = (event, details = {}) => console.log(JSON.stringify({ at: stamp(), service: 'amazon-detector', event, ...details }));
const text = v => String(v ?? '').replace(/\s+/g, ' ').trim();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const randomMs = () => cfg.minMs + Math.floor(Math.random() * (cfg.maxMs - cfg.minMs + 1));

let seen = new Set();
let firstScan = true;
let browser, context, page;
let stopping = false;
let scanTimer;
let resolveSleep;
let activeScan = false;
const status = {
  service: 'amazon-detector', startedAt: stamp(), source: cfg.mode,
  state: 'starting', paused: false, pauseReason: null, lastHTTP: null,
  lastScan: null, lastError: null, lastCount: 0, newJobs: 0,
  totalSeen: 0, nextScan: null, failures: 0
};

async function loadState() {
  try {
    const saved = JSON.parse(await fs.readFile(cfg.file, 'utf8'));
    if (!Array.isArray(saved.ids)) throw new Error('Invalid saved IDs');
    seen = new Set(saved.ids.map(String));
    firstScan = false;
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
  status.totalSeen = seen.size;
}
async function saveState() {
  await fs.mkdir(path.dirname(cfg.file), { recursive: true, mode: 0o700 });
  const tmp = cfg.file + '.tmp';
  await fs.writeFile(tmp, JSON.stringify({ version: 1, savedAt: stamp(), ids: [...seen] }, null, 2), { mode: 0o600 });
  await fs.rename(tmp, cfg.file);
}
function normalize(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const title = text(raw.title || raw.jobTitle || raw.name);
  let applyUrl = null;
  try {
    const url = new URL(raw.applyUrl || raw.jobUrl || raw.url || '', cfg.target);
    if (url.protocol === 'https:' && (url.hostname === 'jobsatamazon.co.uk' || url.hostname.endsWith('.jobsatamazon.co.uk'))) applyUrl = url.href;
  } catch {}
  const jobId = text(raw.jobId || raw.job_id || raw.id || raw.jobPostingId ||
    (applyUrl ? new URL(applyUrl).searchParams.get('jobId') || new URL(applyUrl).pathname.match(/\/jobs?\/([^/?#]+)/i)?.[1] : ''));
  if (!jobId || !title) return null;
  const shiftDetails = text(raw.shiftDetails || raw.shifts || raw.schedule || raw.employmentType) || null;
  return {
    jobId, title,
    location: text(raw.location || raw.city || raw.siteLocation) || null,
    postcode: text(raw.postcode || raw.postalCode || raw.zipCode) || null,
    payRate: text(raw.payRate || raw.hourlyPay || raw.salary || raw.compensation) || null,
    shiftDetails, shifts: shiftDetails, applyUrl
  };
}
function uniqueJobs(list) {
  const found = new Map();
  for (const raw of list) {
    const job = normalize(raw);
    if (!job) continue;
    if (!found.has(job.jobId)) found.set(job.jobId, job);
    else {
      const old = found.get(job.jobId);
      for (const [key, value] of Object.entries(job)) if (!old[key] && value) old[key] = value;
    }
  }
  return [...found.values()];
}
async function openBrowser() {
  browser = await chromium.launch({ headless: cfg.headless, chromiumSandbox: !cfg.noSandbox });
  context = await browser.newContext({ locale: 'en-GB', timezoneId: 'Europe/London', viewport: { width: 1366, height: 768 }, acceptDownloads: false });
  page = await context.newPage();
}
async function closeBrowser() {
  await context?.close().catch(() => {});
  await browser?.close().catch(() => {});
  context = null; browser = null; page = null;
}
function restricted(msg) { const e = new Error(msg); e.accessRestricted = true; return e; }
async function getBrowserJobs() {
  if (!browser?.isConnected()) { await closeBrowser(); await openBrowser(); }
  const response = await page.goto(cfg.target, { waitUntil: 'domcontentloaded', timeout: cfg.timeoutMs });
  const code = response?.status() || 0;
  status.lastHTTP = code;
  if (code === 403 || code === 429) throw restricted(`HTTP ${code} access restricted`);
  if (code < 200 || code >= 300) throw new Error(`Page HTTP ${code}`);
  await page.waitForTimeout(1500);
  const body = await page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
  if (/access restricted|access denied|captcha|unusual traffic/i.test(body.slice(0, 15000))) throw restricted('Restriction page detected');
  return page.evaluate(() => {
    const clean = s => String(s || '').replace(/\s+/g, ' ').trim();
    const cards = new Set(document.querySelectorAll('[data-job-id],[data-testid*="job-card" i],[class*="job-card" i],[class*="jobcard" i],[class*="job-listing" i],article'));
    for (const a of document.querySelectorAll('a[href]')) {
      if (/\/jobs?\/|jobId=|job_id=/i.test(a.href)) cards.add(a.closest('article,li,[role="listitem"]') || a.parentElement);
    }
    const result = [];
    for (const card of cards) {
      if (!card) continue;
      const link = card.querySelector('a[href*="/job"],a[href*="jobId="]');
      const title = clean(card.querySelector('h1,h2,h3,h4,[data-testid*="title" i],[class*="job-title" i]')?.textContent || link?.textContent);
      const body = clean(card.innerText);
      const jobId = clean(card.getAttribute('data-job-id') || card.querySelector('[data-job-id]')?.getAttribute('data-job-id') || body.match(/Job\s*ID\s*[:#]?\s*([\w-]+)/i)?.[1]);
      if (!title || !jobId) continue;
      result.push({
        jobId, title,
        location: clean(card.querySelector('[data-testid*="location" i],[class*="location" i]')?.textContent),
        postcode: clean(card.querySelector('[data-testid*="postcode" i],[class*="postcode" i]')?.textContent),
        payRate: clean(card.querySelector('[data-testid*="pay" i],[class*="pay-rate" i],[class*="salary" i]')?.textContent || body.match(/£\s*\d+(?:\.\d{1,2})?\s*(?:per\s*hour|\/\s*hour|\/\s*hr)/i)?.[0]),
        shiftDetails: clean(card.querySelector('[data-testid*="shift" i],[class*="shift" i],[class*="schedule" i]')?.textContent),
        applyUrl: link?.href || null
      });
    }
    return result;
  });
}
async function getJsonJobs() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), cfg.timeoutMs);
  try {
    const response = await fetch(cfg.feed, { headers: { Accept: 'application/json' }, signal: controller.signal });
    status.lastHTTP = response.status;
    if (response.status === 403 || response.status === 429) throw restricted(`Feed HTTP ${response.status}`);
    if (!response.ok) throw new Error(`Feed HTTP ${response.status}`);
    const data = await response.json();
    const jobs = Array.isArray(data) ? data : data.jobs ?? data.items;
    if (!Array.isArray(jobs)) throw new Error('Feed response needs a jobs/items array');
    return jobs;
  } finally { clearTimeout(timeout); }
}

const sockets = new WebSocketServer({ host: cfg.wsHost, port: cfg.wsPort, maxPayload: 65536 });
sockets.on('connection', ws => {
  ws.on('error', err => log('websocket_error', { error: err.message }));
  ws.send(JSON.stringify({ event: 'DETECTOR_CONNECTED', timestamp: stamp() }));
});
sockets.on('error', err => log('websocket_server_error', { error: err.message }));
function broadcast(payload) {
  const encoded = JSON.stringify(payload);
  for (const client of sockets.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    if (client.bufferedAmount > 1024 * 1024) { client.terminate(); continue; }
    client.send(encoded, err => { if (err) log('ws_delivery_failed', { error: err.message }); });
  }
}
async function telegram(job) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);
  try {
    const message = [`New Amazon UK hourly job`, job.title, job.location || 'Unknown location', job.payRate || 'Pay not stated', job.shifts || 'Shift not stated', job.applyUrl || ''].join('\n');
    const res = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: message, disable_web_page_preview: true }), signal: controller.signal
    });
    if (!res.ok) throw new Error(`Telegram HTTP ${res.status}`);
  } catch (err) { log('telegram_error', { error: err.message }); }
  finally { clearTimeout(timeout); }
}
async function processJobs(raw) {
  const list = uniqueJobs(raw);
  status.lastCount = list.length;
  const fresh = list.filter(job => !seen.has(job.jobId));
  if (!fresh.length) { firstScan = false; return; }
  for (const job of fresh) seen.add(job.jobId);
  try { await saveState(); }
  catch (err) { for (const job of fresh) seen.delete(job.jobId); throw err; }
  status.totalSeen = seen.size;
  status.newJobs += fresh.length;
  if (firstScan && !cfg.alertExisting) {
    firstScan = false;
    log('baseline_created', { count: fresh.length });
    return;
  }
  firstScan = false;
  for (const job of fresh) {
    broadcast({ event: 'NEW_UK_JOB', timestamp: stamp(), job });
    log('new_job', { jobId: job.jobId, title: job.title, location: job.location });
    void telegram(job);
  }
}
async function scan() {
  if (stopping || status.paused || activeScan) return;
  activeScan = true;
  status.state = 'scanning';
  try {
    const jobs = cfg.mode === 'json' ? await getJsonJobs() : await getBrowserJobs();
    await processJobs(jobs);
    status.lastScan = stamp();
    status.failures = 0;
    status.lastError = null;
    status.state = 'monitoring';
    log('scan_complete', { found: status.lastCount, totalSeen: status.totalSeen });
  } catch (err) {
    status.failures++;
    status.lastError = err.message;
    status.state = err.accessRestricted ? 'paused' : 'retry_wait';
    if (err.accessRestricted) { status.paused = true; status.pauseReason = 'access_restricted'; }
    log('scan_failed', { error: err.message, failures: status.failures });
  } finally { activeScan = false; }
}
async function loop() {
  while (!stopping && !status.paused) {
    await scan();
    if (stopping || status.paused) break;
    const backoff = Math.pow(2, Math.min(Math.max(0, status.failures - 1), 5));
    const ms = Math.min(15 * 60 * 1000, randomMs() * backoff);
    status.nextScan = new Date(Date.now() + ms).toISOString();
    await new Promise(resolve => {
      resolveSleep = resolve;
      scanTimer = setTimeout(() => { resolveSleep = null; resolve(); }, ms);
    });
  }
  status.nextScan = null;
}
const health = http.createServer((req, res) => {
  const pathname = new URL(req.url, `http://${cfg.bind}:${cfg.port}`).pathname;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  if (pathname !== '/health' && pathname !== '/status') {
    res.writeHead(404).end(JSON.stringify({ error: 'Not found' })); return;
  }
  const ok = status.state === 'monitoring' || status.state === 'scanning';
  res.writeHead(ok ? 200 : 503).end(JSON.stringify({
    ...status, websocketClients: sockets.clients.size,
    uptimeSeconds: Math.round(process.uptime()),
    memoryMB: Math.round(process.memoryUsage().rss / 1024 / 1024)
  }));
});
let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true; stopping = true;
  clearTimeout(scanTimer);
  resolveSleep?.();
  log('shutdown', { signal });
  for (const socket of sockets.clients) socket.terminate();
  await closeBrowser();
  await Promise.all([
    new Promise(resolve => sockets.close(resolve)),
    new Promise(resolve => health.close(resolve))
  ]);
  process.exit(0);
}
process.once('SIGINT', () => void shutdown('SIGINT'));
process.once('SIGTERM', () => void shutdown('SIGTERM'));
async function main() {
  await loadState();
  await new Promise((resolve, reject) => {
    health.once('error', reject);
    health.listen(cfg.port, cfg.bind, resolve);
  });
  log('ready', { health: `${cfg.bind}:${cfg.port}`, websocket: `${cfg.wsHost}:${cfg.wsPort}` });
  if (cfg.mode === 'browser') await openBrowser();
  await loop();
}
main().catch(async err => {
  log('fatal', { error: err.message });
  await closeBrowser();
  process.exitCode = 1;
  health.close();
  sockets.close();
});
