# v0.5.8 — Start Monitoring immediate refresh

- `src/background.js`: validates selected tab and security challenge status; on a user-initiated Start Monitoring action reloads once immediately, records the pending refresh, and lets `DETECTOR_PAGE_READY` begin the existing UK-clock scheduler and scan. If the reload fails, monitoring is stopped safely. No schedule/profile/receiver changes.
- `popup.js`: clarifies that the initial page refresh has been requested.
- `popup.html`, `manifest.json`: update visible and package version to 0.5.8.
- `test_start_refresh.mjs`: dynamic mocked Chrome runtime tests for one startup reload, resume fixed UK schedule, loading/application tab guards, CAPTCHA and missing content-script checks, rollback on Chrome reload errors.
- `test_sequence.mjs`: retained v0.5.7 sequence and scheduler tests; manifest expectation updated.

No changes to `src/core.js`, `src/content.js`, `src/availability.js`, `src/presence_transport.js`, `src/safety.js`, Oracle API, credentials, or job identification.
