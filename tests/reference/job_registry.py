"""Amazon job observation registry for the isolated detector intake staging service.

Security boundary: only already-public job fields. Caller is responsible for
separately authenticating detector identity and ensuring complete snapshots.
This module MUST NOT be pointed at the production private relay queue database.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

KNOWN_PUBLIC_HOSTS = ("amazon.jobs", "hiring.amazon.com")
CANONICAL_KEYS = ("title", "location", "employment_type", "pay", "weekly_hours", "schedule", "duration", "visible_card_text")
REVERSION_CONFIRM_SECONDS = 5.0
MAX_FUTURE_SKEW_SECONDS = 300


def canonical_text(value: str) -> str:
    """Keep meaningful spelling, punctuation, numbers and case; ignore layout whitespace."""
    if not isinstance(value, str):
        raise ValueError("job text must be a string")
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", value).replace("\u00a0", " ")).strip()


def _utc_seconds(raw: str) -> float:
    if not isinstance(raw, str):
        raise ValueError("observed_at must be ISO-8601 string")
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("observed_at requires timezone")
    return dt.timestamp()


def official_job_url(raw: str | None) -> str | None:
    if raw is None or not raw.strip():
        return None
    url = urlsplit(raw.strip())
    host = (url.hostname or "").lower()
    if (url.scheme != "https" or url.username or url.password or url.port not in (None, 443)
        or not (host == "amazon.jobs" or host.endswith(".amazon.jobs") or host == "hiring.amazon.com")):
        raise ValueError("official_url must be a validated HTTPS Amazon Jobs URL")
    qs = [(k, v) for k, v in parse_qsl(url.query, keep_blank_values=True)
          if not (k.lower().startswith("utm_") or k.lower() in {"ref", "ref_", "source"})]
    return urlunsplit(("https", host, url.path.rstrip("/") or "/", urlencode(sorted(qs)), ""))


def _key_for(job: dict, url: str | None) -> str:
    raw = job.get("job_id")
    if raw is not None and canonical_text(raw):
        jid = canonical_text(raw)
        if len(jid) > 150:
            raise ValueError("job_id too long")
        return "amazon-job-id:" + jid
    if url:
        return "amazon-job-url:" + url
    raise ValueError("job identity requires job_id or official_url")


def _validate_payload(observation: dict, received_at: float) -> tuple[str, str, dict, float, int | None, str]:
    if not isinstance(observation, dict):
        raise ValueError("observation must be object")
    detector = observation.get("detector_id")
    if not isinstance(detector, str) or not re.fullmatch(r"detector-(?:0[1-9]|10)", detector):
        raise ValueError("invalid detector_id")
    observed = _utc_seconds(observation.get("observed_at"))
    if observed > received_at + MAX_FUTURE_SKEW_SECONDS:
        raise ValueError("detector timestamp too far in future")
    seq = observation.get("sequence")
    if seq is not None and (type(seq) is not int or seq < 0):
        raise ValueError("sequence must be a nonnegative integer")
    if observation.get("render_complete") is not True:
        raise ValueError("incomplete/unconfirmed card snapshot")
    job = observation.get("job")
    if not isinstance(job, dict):
        raise ValueError("job must be object")
    allowed = set(CANONICAL_KEYS) | {"job_id", "official_url"}
    if any(k not in allowed for k in job):
        raise ValueError("unknown/private job fields rejected")
    url = official_job_url(job.get("official_url"))
    key = _key_for(job, url)
    fields = {k: canonical_text(job.get(k, "") or "") for k in CANONICAL_KEYS}
    if not fields["title"] or not fields["location"] or len(fields["visible_card_text"]) < 8:
        raise ValueError("job card appears incomplete")
    if any(len(fields[k]) > (12000 if k == "visible_card_text" else 500) for k in CANONICAL_KEYS):
        raise ValueError("job fields exceed limit")
    fields["official_url"] = url
    fields["job_id"] = canonical_text(job.get("job_id") or "")
    digest = hashlib.sha256(json.dumps({k: fields[k] for k in CANONICAL_KEYS},
                  ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return detector, key, fields, observed, seq, digest


def _text_difference(old: dict, new: dict) -> dict:
    fields = {}
    for key in CANONICAL_KEYS:
        if old.get(key) != new.get(key):
            fields[key] = {"before": old.get(key, ""), "after": new.get(key, "")}
    left = old.get("visible_card_text", "").split()
    right = new.get("visible_card_text", "").split()
    changes = []
    for op, i, j, k, m in SequenceMatcher(None, left, right, autojunk=False).get_opcodes():
        if op != "equal":
            changes.append({"kind": op, "before": " ".join(left[i:j]), "after": " ".join(right[k:m])})
    return {"changed_fields": fields, "word_changes": changes[:40]}


@dataclass(frozen=True)
class Outcome:
    status: str
    job_key: str
    version: int | None
    fingerprint: str
    diff: dict | None = None


class Registry:
    """Offline staging DB; each observation is atomically classified and queued."""
    def __init__(self, path: str | Path):
        self.path = Path(path)
        if self.path.exists() and self.path.is_dir():
            raise ValueError("database path is a directory")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_key TEXT PRIMARY KEY, job_id TEXT, official_url TEXT,
            current_fingerprint TEXT NOT NULL, current_version INTEGER NOT NULL,
            snapshot_json TEXT NOT NULL, first_seen REAL NOT NULL,
            last_seen REAL NOT NULL, changed_observed_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS versions (
            job_key TEXT NOT NULL, version INTEGER NOT NULL,
            fingerprint TEXT NOT NULL, snapshot_json TEXT NOT NULL,
            detector_id TEXT NOT NULL, received_at REAL NOT NULL,
            observed_at REAL NOT NULL, diff_json TEXT,
            PRIMARY KEY(job_key,version), FOREIGN KEY(job_key) REFERENCES jobs(job_key)
        );
        CREATE TABLE IF NOT EXISTS detector_state (
            detector_id TEXT PRIMARY KEY, sequence INTEGER,
            last_seen REAL NOT NULL, accepted_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS detector_job_state (
            detector_id TEXT NOT NULL, job_key TEXT NOT NULL,
            last_current_fingerprint TEXT NOT NULL, last_seen REAL NOT NULL,
            PRIMARY KEY(detector_id, job_key)
        );
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, detector_id TEXT NOT NULL,
            job_key TEXT NOT NULL, fingerprint TEXT NOT NULL,
            observed_at REAL NOT NULL, received_at REAL NOT NULL,
            sequence INTEGER, outcome TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reversion_candidates (
            job_key TEXT NOT NULL, fingerprint TEXT NOT NULL,
            first_received REAL NOT NULL, first_observed REAL NOT NULL,
            detector_id TEXT NOT NULL, confirmations INTEGER NOT NULL,
            PRIMARY KEY(job_key,fingerprint)
        );
        CREATE TABLE IF NOT EXISTS ingested_events (
            detector_id TEXT NOT NULL, event_id TEXT NOT NULL,
            payload_hash TEXT NOT NULL, status TEXT NOT NULL,
            job_key TEXT NOT NULL, version INTEGER,
            fingerprint TEXT NOT NULL, diff_json TEXT,
            received_at REAL NOT NULL,
            PRIMARY KEY(detector_id, event_id)
        );
        CREATE TABLE IF NOT EXISTS receiver_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_key TEXT NOT NULL,
            version INTEGER NOT NULL, kind TEXT NOT NULL, created_at REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            UNIQUE(job_key, version)
        );
        CREATE TABLE IF NOT EXISTS public_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_key TEXT NOT NULL,
            version INTEGER NOT NULL, kind TEXT NOT NULL, created_at REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            UNIQUE(job_key, version)
        );
        ''')
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def close(self):
        self.db.close()

    def _log_observation(self, detector, key, digest, observed, now, seq, status):
        self.db.execute('''INSERT INTO observations(detector_id,job_key,fingerprint,observed_at,received_at,sequence,outcome)
             VALUES(?,?,?,?,?,?,?)''', (detector,key,digest,observed,now,seq,status))

    def observe(self, obs: dict, *, received_at: float | None = None, event_id: str | None = None, payload_hash: str | None = None) -> Outcome:
        now = time.time() if received_at is None else received_at
        detector, key, fields, observed, seq, digest = _validate_payload(obs, now)
        if event_id is not None and (not isinstance(event_id, str) or not event_id or not payload_hash):
            raise ValueError("event id and payload hash required together")
        self.db.execute('BEGIN IMMEDIATE')
        try:
            if event_id is not None:
                previous = self.db.execute('''SELECT * FROM ingested_events
                    WHERE detector_id=? AND event_id=?''', (detector, event_id)).fetchone()
                if previous is not None:
                    if previous['payload_hash'] != payload_hash:
                        raise ValueError("event ID reused with changed payload")
                    self.db.commit()
                    return Outcome('duplicate_event', previous['job_key'],
                        previous['version'], previous['fingerprint'],
                        json.loads(previous['diff_json']) if previous['diff_json'] else None)
            state = self.db.execute('SELECT sequence FROM detector_state WHERE detector_id=?', (detector,)).fetchone()
            if seq is not None and state and state['sequence'] is not None and seq <= state['sequence']:
                self._log_observation(detector,key,digest,observed,now,seq,'out_of_order')
                result = Outcome('out_of_order', key, None, digest)
            else:
                self.db.execute('''INSERT INTO detector_state(detector_id,sequence,last_seen,accepted_count)
                   VALUES(?,?,?,1) ON CONFLICT(detector_id) DO UPDATE SET
                   sequence=CASE WHEN excluded.sequence IS NOT NULL THEN excluded.sequence ELSE detector_state.sequence END,
                   last_seen=excluded.last_seen, accepted_count=detector_state.accepted_count+1''',
                   (detector,seq,now))
                row = self.db.execute('SELECT * FROM jobs WHERE job_key=?', (key,)).fetchone()
                prior_detector = self.db.execute('''SELECT last_current_fingerprint FROM detector_job_state
                     WHERE detector_id=? AND job_key=?''',(detector,key)).fetchone()
                if row is None:
                    kind, ver, diff = 'new_job', 1, None
                    self._accept_version(key,fields,digest,detector,now,observed,ver,kind,diff)
                    result = Outcome(kind, key,ver,digest)
                elif digest == row['current_fingerprint']:
                    self.db.execute('UPDATE jobs SET last_seen=? WHERE job_key=?', (now,key))
                    result = Outcome('unchanged', key,row['current_version'],digest)
                elif observed <= row['changed_observed_at']:
                    result = Outcome('stale', key,row['current_version'],digest)
                else:
                    old = json.loads(row['snapshot_json'])
                    prior = self.db.execute('SELECT 1 FROM versions WHERE job_key=? AND fingerprint=? LIMIT 1', (key,digest)).fetchone()
                    if prior is not None:
                        # A detector that has not seen the latest confirmed content
                        # must not undo it using an old cached snapshot.
                        if not prior_detector or prior_detector['last_current_fingerprint'] != row['current_fingerprint']:
                            result = Outcome('stale_known_version',key,row['current_version'],digest)
                        elif seq is not None:
                            # The reporting detector has observed the current
                            # version before, and supplies a newer monotonic
                            # sequence. Treat this as a credible real reversal.
                            ver = row['current_version'] + 1
                            diff = _text_difference(old, fields)
                            self._accept_version(key,fields,digest,detector,now,observed,ver,'job_changed',diff)
                            result = Outcome('job_changed',key,ver,digest,diff)
                        else:
                            # No sequence available: require a second fresh
                            # observation to avoid oscillations from caches.
                            candidate = self.db.execute('''SELECT * FROM reversion_candidates
                                  WHERE job_key=? AND fingerprint=?''',(key,digest)).fetchone()
                            if candidate is None:
                                self.db.execute('''INSERT INTO reversion_candidates VALUES(?,?,?,?,?,?)''',
                                    (key,digest,now,observed,detector,1))
                                result = Outcome('reversion_pending',key,row['current_version'],digest)
                            elif (now - candidate['first_received'] >= REVERSION_CONFIRM_SECONDS
                                  and observed > candidate['first_observed']):
                                ver = row['current_version']+1
                                diff = _text_difference(old,fields)
                                self._accept_version(key,fields,digest,detector,now,observed,ver,'job_changed',diff)
                                result = Outcome('job_changed',key,ver,digest,diff)
                            else:
                                result = Outcome('reversion_pending',key,row['current_version'],digest)
                    else:
                        ver = row['current_version']+1
                        diff = _text_difference(old,fields)
                        self._accept_version(key,fields,digest,detector,now,observed,ver,'job_changed',diff)
                        result = Outcome('job_changed',key,ver,digest,diff)
                if result.status in ('new_job', 'job_changed', 'unchanged'):
                    self.db.execute('''INSERT INTO detector_job_state VALUES(?,?,?,?)
                       ON CONFLICT(detector_id,job_key) DO UPDATE SET
                       last_current_fingerprint=excluded.last_current_fingerprint,
                       last_seen=excluded.last_seen''',(detector,key,digest,now))
                self._log_observation(detector,key,digest,observed,now,seq,result.status)
            if event_id is not None:
                self.db.execute('''INSERT INTO ingested_events(detector_id,event_id,payload_hash,status,job_key,version,fingerprint,diff_json,received_at) VALUES (?,?,?,?,?,?,?,?,?)''',
                    (detector,event_id,payload_hash,result.status,result.job_key,result.version,
                     result.fingerprint,json.dumps(result.diff) if result.diff else None,now))
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    def _accept_version(self,key,fields,digest,detector,now,observed,ver,kind,diff):
        snapshot = json.dumps(fields,sort_keys=True,ensure_ascii=False)
        self.db.execute('''INSERT INTO jobs(job_key,job_id,official_url,current_fingerprint,current_version,snapshot_json,first_seen,last_seen,changed_observed_at)
          VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(job_key) DO UPDATE SET
          job_id=excluded.job_id, official_url=excluded.official_url,
          current_fingerprint=excluded.current_fingerprint, current_version=excluded.current_version,
          snapshot_json=excluded.snapshot_json, last_seen=excluded.last_seen,
          changed_observed_at=excluded.changed_observed_at''',
          (key,fields['job_id'],fields['official_url'],digest,ver,snapshot,now,now,observed))
        self.db.execute('''INSERT INTO versions VALUES(?,?,?,?,?,?,?,?)''',
                        (key,ver,digest,snapshot,detector,now,observed,
                         json.dumps(diff,ensure_ascii=False) if diff else None))
        self.db.execute('DELETE FROM reversion_candidates WHERE job_key=?', (key,))
        for table in ('receiver_outbox','public_outbox'):
            self.db.execute(f'INSERT INTO {table}(job_key,version,kind,created_at) VALUES(?,?,?,?)',
                            (key,ver,kind,now))

    def counts(self) -> dict[str,int]:
        return {table:self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                for table in ('jobs','versions','observations','receiver_outbox','public_outbox')}
