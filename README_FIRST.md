# Amazon Jobs Detector v0.5.8 — Immediate refresh on Start Monitoring

This builds on the working v0.5.7 sequence-recovery release. There are no Oracle server changes.

## Requested new behaviour

When you click **Start Monitoring** on an already loaded, supported Amazon Jobs listings page:

1. Check that the Oracle key/profile is valid and the tab is a supported public listings page (not an application page).
2. Ask the existing page content script to perform a read-only CAPTCHA/access-restriction safety check.
3. If safe, **reload the monitored tab once immediately**, without waiting for the detector's next assigned UK second.
4. After the new page loads, start MutationObserver and normal card detection automatically.
5. Continue refreshing at the *unchanged* Oracle-assigned 10-second seconds of each UK-clock minute (e.g., detector-01: 01, 11, 21, 31, 41, 51). Missed schedule slots are skipped, not replayed.

The initial refresh is counted as a successful refresh only after the freshly loaded page sends its `DETECTOR_PAGE_READY` message. No early card scan is sent from the old page before that reload.

**Safety:** The detector will not perform a blind reload when it cannot verify the absence of CAPTCHA or access restrictions. If the content script isn't active after updating the extension, manually reload the Amazon Jobs tab *once*, inspect it, then click Start Monitoring. If the tab is still loading, wait until it finishes. A failed initial reload leaves monitoring stopped, not running silently.

## Install on your existing Detector-01 Chrome profile

1. Stop Monitoring (if currently running).
2. Back up the current unpacked extension folder (the one Chrome is already using).
3. Extract this ZIP into the **same unpacked folder**, replacing source files. Do not remove or re-add the extension; do not delete browser storage or change the detector API key.
4. In `chrome://extensions`, use **Reload** on the existing Amazon Jobs Detector. Verify version `0.5.8`.
5. If Chrome says the security check listener is missing, manually reload the Amazon Jobs tab once and inspect the page for CAPTCHA. Then return to the detector popup.
6. Confirm the assigned detector-01 Oracle profile and UK seconds, then click **Start Monitoring**.
7. Confirm the page reloads once immediately; `Successful refreshes` increments after page load and subsequent refreshes occur only on your fixed seconds.

Keep one Chrome installation for each Oracle detector key; do not use the same key on multiple computers.

## Unchanged

- v0.5.7 persistent millisecond observation sequences and `out_of_order` recovery (Oracle had accepted `unchanged` sequence `1793805505994`).
- Job ID calculation including distance (distinct Quick Ship warehouse cards remain distinct).
- MutationObserver, card parsing, complete-scan presence evidence, Oracle observation/presence/heartbeat API, CAPTCHA safety, fixed UK refresh times, no new permissions.
- Receiver services, existing Oracle history and 90-day audit, private/public Telegram settings.

## Verification

- `node test_start_refresh.mjs` (mocked service worker + Chrome APIs)
- `node test_sequence.mjs` (sequence / scheduling regression assertions)
- `python3 test_registry_sequence.py` (isolated Oracle sequence regression simulation)
- `node --check src/background.js` and `node --check popup.js`

Local tests do not replace a real Chrome + live Amazon page check. No live Oracle VM was changed during packaging.
