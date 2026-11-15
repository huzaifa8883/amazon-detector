# v0.6.1 — passive job-card selector compatibility

Changes: accept individual job-link cards without Type/Pay/Duration labels; discover job links within generated React DIV wrappers; use semantic title and location attributes; accept explicitly structured job cards with title and location even when extra details are missing. Existing synthetic identity and confidence/coverage checks remain.

No Amazon polling/reload is added. Observations are limited to the current DOM, and the Oracle validation/delivery rules remain unchanged.

Limitations: not verified against your specific live account/page. The page must expose a title and a location (or an explicit location attribute) for an Oracle-reportable job. If the visible card has no location, it is intentionally not fabricated. The extension does not know when the server has new jobs until the site renders them.

Install: unpack ZIP; Chrome Extensions > Developer mode > Load unpacked; remove/disable older detector; reload your open Amazon Jobs page once manually after updating.
