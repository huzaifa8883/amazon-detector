# Low-latency detector review (13 November 2026)

This package is a **conservative improvement**, not a promise of sub-second end-to-end discovery. The original detector already watches live DOM mutations; this version removes an extra 90 ms wait before starting a scan when the page itself changes. Two stability scans still run to reduce false positives. Existing Oracle-controlled refresh schedules, server validation, reliable observation queue, and protections against security challenges are unchanged.

## Architecture discovered
- Detector `src/content.js`: MutationObserver -> evidence message -> settled `DETECTOR_SNAPSHOT` -> MV3 background.
- Detector `src/background.js`: validates and queues observations -> authenticated HTTPS POST `/v1/detectors/observations`; also uploads presence evidence separately.
- Receiver `src/signal_worker.js` and `src/signal_socket.js`: existing authenticated WSS -> offscreen document -> background durable journal -> acknowledgement.
- The server-side Oracle observation validation/broadcast code was not present in these ZIPs. It can add uncontrolled delay.

## Safe upgrade path
1. Prefer an authorized server event feed or publish/subscribe notification from the first-party jobs API over requesting more frequently. Browser extensions cannot invent a push feed if the site does not provide one.
2. Avoid monkey-patching page fetch/XHR/WebSocket and avoid injecting fake mouse/scroll events as camouflage. Chrome's `webRequest` does not expose HTTPS response bodies; CDP interception is not suitable as a general production extension design.
3. For observable page updates, the included change starts scanning on the next task (`schedule(0)`) rather than 90 ms after a mutation. Keep the two-stage 135 ms stability condition; it prevents unstable/skeleton cards becoming confirmed jobs.
4. Preserve the existing authenticated persistent WSS receiver and ACK-after-durable-persist design. Do not delete backpressure, deduplication or retries for claimed latency gains.
5. Instrument latency with monotonic timestamps locally: `domObserved`, `snapshotValidated`, `queuePersisted`, `serverAccepted`, `broadcastSent`, `receiverReceived`, `journalPersisted`, `ackSent`. Correlate with opaque event IDs; never emit auth tokens. `performance.now()` comparisons only work within the same execution context; use synchronized server clocks / monotonic round trips for cross-device diagnostics.
6. The existing server's decision to broadcast only validated observations must remain the authoritative source. DOM mutation evidence alone is deliberately labeled unconfirmed and must not trigger actions.
7. Respect 429, Retry-After, CAPTCHAs and permission boundaries. Rate jitter only belongs in **backoff on failures**, not as fake human activity or WAF avoidance.

## Important limitation
Sub-second response **after the website has updated its DOM** is a measurable target, but near-instant discovery of new server-side jobs is impossible if the page has neither received an update nor offers an authorized push stream. The server component and a live integration test are necessary to validate true end-to-end latency.
