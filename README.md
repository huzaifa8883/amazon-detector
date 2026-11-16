# Amazon UK Hourly Detector — standalone Node.js

This is a self-contained **public-job monitoring service**, not the original Chrome extension. It provides Playwright browser extraction or an explicitly authorized normalized JSON feed, persistent seen-job IDs, optional Telegram alerts, localhost WebSocket on port 8080, and health/status JSON on port 5002.

**Important:** `https://www.jobsatamazon.co.uk/` returned HTTP 403 from the user's VPS during earlier tests. This package does NOT bypass restrictions or include a verified Amazon AppSync endpoint. A 403/429 pauses collection until access is legitimately restored. DOM selectors are generic and may need adaptation to an accessible public jobs page. Missing source fields remain null. This package does not apply for jobs or submit accounts.

## Install (Ubuntu)

```bash
cd /var/www/amazon-bot-system/amazon-detector
cp .env.example .env
npm install
npx playwright install --with-deps chromium
npm run check
```

Use a dedicated unprivileged Linux user with Chromium sandbox enabled whenever possible. If already running under root, Chromium may refuse to launch with sandbox enabled; avoid disabling sandbox except in an isolated, trusted test environment.

## PM2

To replace an old process (verify its command with `pm2 describe amazon-detector` first):

```bash
pm2 stop amazon-detector
pm2 delete amazon-detector
pm2 start detector.js --name amazon-detector --time --restart-delay 10000
pm2 save
pm2 logs amazon-detector --lines 40
```

Health: `curl -i http://127.0.0.1:5002/health`
WebSocket: `ws://127.0.0.1:8080` (loopback only; Receiver must implement `NEW_UK_JOB`).

## Feed shape

If you possess a **permitted** normalized JSON feed, choose `SOURCE_MODE=json`, `PUBLIC_FEED_URL=https://...` with either an array or `{ "jobs": [...] }`. Each job must provide `jobId` and `title`. Other supported fields: `location`, `postcode`, `payRate`, `shiftDetails`, `applyUrl`. Only URLs under jobsatamazon.co.uk are accepted as application links.

First successful scan forms a baseline when `ALERT_EXISTING=false`. De-duplication uses `seen_jobs.json`; do not delete it unless you explicitly want to reset the known-job history. WebSocket signals are best-effort and not replayed if a receiver disconnects; for reliable alerts, add durable outbox delivery and acknowledgements.
