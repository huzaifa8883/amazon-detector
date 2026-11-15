# Passive DOM detector (0.6.0)

- Start Monitoring verifies the current content script and sends DETECTOR_BEGIN; it does **not** reload the tab.
- On start, an initial snapshot of rendered cards is taken; MutationObserver handles subsequent DOM updates.
- No scheduled refresh timer, refresh message handler or tabs.reload call remains.
- Existing event-triggered 135 ms stabilization and follow-up verification remain to prevent false positives. They do not request/refresh Amazon pages.
- Oracle delivery retries, 30-second service alarms and periodic profile refresh still exist; those talk to the **Oracle endpoint**, not Amazon.
- Amazon might not update an idle page. If so, no new job is observable; use a user-initiated navigation to update listings.
- To upgrade in Chrome, unpack this ZIP and use Load unpacked on the extension folder. For an already-open tab, manually reload once to ensure the new content script is injected, then Start Monitoring.
- Do not mistake the legacy UI's historical refresh counters for automatic refresh; new automatic refreshes are disabled.
