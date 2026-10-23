"""Accounting discovery, durable delivery state, and private log snapshots."""
from __future__ import annotations

import contextlib
import datetime as dt
import fcntl
import glob
import hashlib
import html
import http.client
import json
import logging
import os
from pathlib import Path
import pwd
import re
import sqlite3
import stat
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

from .classify import classify as classify_failure
from . import chains

LOG = logging.getLogger(__name__)
FAILURES = {"FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE"}
WATCHED = FAILURES | {"CANCELLED"}  # Job-script watchdogs call scancel.
FIELDS = ("Cluster", "DBIndex", "JobID", "JobIDRaw", "JobName", "User", "State",
          "ExitCode", "DerivedExitCode", "Submit", "Start", "End", "Elapsed",
          "Partition", "Account", "NodeList", "NNodes", "NTasks", "ReqMem",
          "Restarts", "WorkDir", "StdOut", "StdErr")
SEP = "\x1f"
GIST_URL = re.compile(r"https://gist\.github\.com/[A-Za-z0-9-]+/[0-9a-f]+")


def open_log(path: Path):
    # Nonblocking open prevents a replaced log path/FIFO from wedging the watcher.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise OSError("log is not a regular file")
    return os.fdopen(fd, "rb")


def stamp(epoch: float) -> str:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def epoch(value: str) -> float:
    return dt.datetime.fromisoformat(value).replace(tzinfo=dt.timezone.utc).timestamp()


def private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError(f"directory must be owned by you, mode 0700, and not a symlink: {path}")


def read_secret(path: Path) -> str:
    # Inspect the descriptor, avoiding a check/open race and symlink traversal.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("webhook file must be a regular file owned by you with mode 0600")
        secret = stream.read(4097).strip()
    url = urllib.parse.urlsplit(secret)
    if (len(secret) > 4096 or url.scheme != "https" or url.hostname not in
            {"hooks.slack.com", "hooks.slack-gov.com"} or url.port not in (None, 443)
            or url.username or url.password or url.query or url.fragment
            or not url.path.startswith("/services/")):
        raise ValueError("invalid Slack incoming webhook URL")
    return secret


def query(config: dict, start: float, end: float) -> list[dict]:
    user = pwd.getpwuid(os.getuid()).pw_name
    command = [config["sacct"], "--local", "--allocations", "--array", "--duplicates",
               "--noheader", "--parsable2", f"--delimiter={SEP}", f"--user={user}",
               f"--starttime={stamp(start)}", f"--endtime={stamp(end)}",
               "--state=" + ",".join(sorted(WATCHED)),
               "--format=" + ",".join(f"{field}%16384" for field in FIELDS)]
    # Normalize timestamps and prevent inherited SACCT_* filters from hiding jobs.
    env = {k: v for k, v in os.environ.items() if not k.startswith("SACCT_")}
    env.update(TZ="UTC", SLURM_TIME_FORMAT="%Y-%m-%dT%H:%M:%S", SLURM_BITSTR_LEN="0")
    try:
        result = subprocess.run(command, env=env, text=True, encoding="utf-8",
                                errors="strict", capture_output=True,
                                timeout=config["sacct_timeout"], check=True)
    except (subprocess.SubprocessError, UnicodeError, OSError) as error:
        # Never print subprocess output (job names and metadata may be sensitive).
        raise RuntimeError("sacct failed; check local Slurm availability and supported fields/options") from error
    jobs = []
    for line in result.stdout.split("\n"):
        if not line:
            continue
        values = line.split(SEP)
        if len(values) != len(FIELDS) or any(len(v) >= 16384 for v in values):
            raise ValueError("malformed or truncated sacct record; scan cursor preserved")
        job = dict(zip(FIELDS, (v.strip() for v in values)))
        if not job["State"]:
            raise ValueError("empty accounting state")
        job["State"] = job["State"].split()[0].rstrip("+")
        if job["State"] not in WATCHED or "." in job["JobIDRaw"]:
            continue
        if not re.fullmatch(r"\d+(?:_\d+|\+\d+)?", job["JobID"]):
            raise ValueError("sacct did not expand array tasks")
        if not job["Cluster"] or not job["Submit"] or job["User"] != user:
            raise ValueError("incomplete accounting identity or unexpected user")
        if not job["JobIDRaw"].isdigit():
            raise ValueError("invalid raw allocation ID")
        epoch(job["Submit"])
        if job["End"] in ("", "Unknown", "None"):
            continue  # Will be reconsidered by overlap/deep scans.
        epoch(job["End"])
        jobs.append(job)
    return jobs


def identity(job: dict) -> str:
    index = job["DBIndex"]
    parts = [job["Cluster"], index] if index.isdigit() and int(index) else [
        job["Cluster"], job["JobIDRaw"], job["Submit"]]
    return hashlib.sha256(json.dumps(parts).encode()).hexdigest()


def resolve(pattern: str, job: dict) -> tuple[list[Path], int, str]:
    """Expand known tokens, safely glob only fan-out tokens; never shell-expand."""
    if pattern in ("", "Unknown", "None"):
        return [], 1, "accounting has no output path"
    if pattern == "/dev/null":
        return [], 0, "redirected to /dev/null"
    array = re.fullmatch(r"(\d+)_(\d+)", job["JobID"])
    values = {"j": job["JobIDRaw"], "J": job["JobIDRaw"], "u": job["User"],
              "x": job["JobName"], "r": job["Restarts"],
              "A": array[1] if array else job["JobIDRaw"],
              "a": array[2] if array else "4294967294",
              "b": str(int(array[2]) % 10) if array else "4"}
    expression, matcher, expected = "", "", 1
    fanout = False
    axes = set()
    if "\\" in pattern:
        pattern = pattern.replace("\\", "")
        tokens = [(pattern, None)]
    else:
        tokens = []
        pos = 0
        for match in re.finditer(r"%(\d*)(.)", pattern):
            tokens.append((pattern[pos:match.start()], None))
            tokens.append((match[2], match[1]))
            pos = match.end()
        tokens.append((pattern[pos:], None))
        if "%" in pattern[pos:]:
            return [], 1, "unresolved filename token"
    for token, width in tokens:
        if width is None:
            literal = token
        elif token == "%":
            literal = "%"
        elif token in values:
            literal = values[token]
            if width and literal.isdigit():
                literal = literal.zfill(min(int(width), 10))
        elif token in {"N", "n", "t"}:
            fanout = True
            axes.add("task" if token == "t" else "node")
            count = job["NTasks" if token == "t" else "NNodes"]
            expected = max(expected, int(count) if count.isdigit() and int(count) else 2**63)
            expression += "*"
            matcher += r"[^/]+" if token == "N" else r"\d+"
            continue
        else:
            return [], 1, f"unsupported filename token %{token}"
        expression += glob.escape(literal)
        matcher += re.escape(literal)
    if not Path(pattern).is_absolute():
        work = job["WorkDir"]
        if not Path(work).is_absolute():
            return [], 1, "accounting has no absolute working directory"
        expression = glob.escape(work.rstrip("/")) + "/" + expression
        matcher = re.escape(work.rstrip("/")) + "/" + matcher
    if len(axes) > 1:
        # NTasks does not describe the placement of each rank on each node.
        expected = 2**63
    paths = [Path(p) for p in glob.glob(expression) if re.fullmatch(matcher, p)]
    return sorted(paths), expected, "fan-out discovery" if fanout else ""


def inspect_logs(job: dict) -> tuple[list[Path], list[dict], bool, str]:
    paths, details, complete = set(), [], True
    fingerprints = []
    for field in ("StdOut", "StdErr"):
        found, expected, note = resolve(job[field], job)
        detail = {"stream": field, "pattern": job[field], "paths": [str(p) for p in found],
                  "expected_minimum": expected, "note": note}
        details.append(detail)
        if len(found) < expected:
            complete = False
        paths.update(found)
    regular = []
    for path in sorted(paths):
        try:
            with open_log(path) as stream:
                info = os.fstat(stream.fileno())
            fingerprints.append((str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns))
            regular.append(path)
        except OSError:
            complete = False
            details.append({"path": str(path), "note": "unavailable or not a regular file"})
    signature = hashlib.sha256(json.dumps(fingerprints).encode()).hexdigest()
    return regular, details, complete, signature


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def copy_log(directory: Path, path: Path, number: int) -> dict:
    temporary = directory / f"log-{number:04d}.tmp"
    digest = hashlib.sha256()
    with open_log(path) as source, temporary.open("wb") as dest:
        before = os.fstat(source.fileno())
        # Copy precisely the size observed at open, avoiding an unbounded growing file.
        remaining = before.st_size
        while remaining:
            chunk = source.read(min(1024 * 1024, remaining))
            if not chunk:
                raise OSError("log shrank during snapshot")
            dest.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
        after = os.fstat(source.fileno())
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise OSError("log changed during snapshot")
        dest.flush()
        os.fsync(dest.fileno())
    target = directory / f"{digest.hexdigest()}.txt"
    os.replace(temporary, target)
    return {"source": str(path), "copy": target.name, "bytes": before.st_size,
            "sha256": digest.hexdigest()}


def report(root: Path, key: str, job: dict, paths: list[Path], details: list[dict],
           complete: bool, now: float, classification: dict | None = None,
           cancellation: dict | None = None) -> tuple[bool, bool]:
    directory = root / "reports" / key
    private_dir(directory)
    previous = []
    try:
        previous = json.loads((directory / "report.json").read_text())["logs"]
    except FileNotFoundError:
        pass
    copies, errors = [], []
    for number, path in enumerate(paths):
        try:
            copy = copy_log(directory, path, number)
            copies.append(copy | {"snapshot_utc": stamp(now), "current": True})
        except OSError as error:
            errors.append({"source": str(path), "error": type(error).__name__})
    sources = {copy["source"] for copy in copies}
    copies.extend(copy | {"current": False} for copy in previous if copy["source"] not in sources)
    complete = complete and not errors
    atomic_json(directory / "report.json", {"job": job, "logs": copies,
                "discovery": details, "copy_errors": errors,
                "classification": classification,
                "afterok_chain": cancellation,
                "complete": complete, "snapshot_utc": stamp(now)})
    return complete, bool(errors)


def slack_text(job: dict, classification: dict | None = None,
               cancellation: dict | None = None, gist_url: str | None = None) -> str:
    summary = (f"*Slurm {job['State']}: {job['Cluster']} job {job['JobID']} ({job['JobName'][:200]})*\n"
               f"*Partition:* {job['Partition'][:100]}\n"
               f"*Time:* {job['Start']} → {job['End']} UTC ({job['Elapsed']})\n"
               f"*Exit:* {job['ExitCode']}\n")
    if classification:
        summary += (f"*Failure:* {classification['category']} — "
                    f"{classification['explanation']}\n")
    if cancellation:
        counts = {}
        for target in cancellation["targets"]:
            counts[target["status"]] = counts.get(target["status"], 0) + 1
        if counts or cancellation["error"]:
            state = "discovery/cancellation retry pending" if cancellation["error"] else cancellation["status"]
            summary += f"*Afterok chain:* {counts or 'unknown'}; {state}\n"
    if gist_url:
        summary += f"*Report and logs:* {gist_url}\n"
    return summary[:2900]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def post(url: str, text: str, timeout: float) -> tuple[str, float]:
    escaped = html.escape(text, quote=False)
    formatted = GIST_URL.sub(lambda match: f"<{match[0]}|Open secret gist>", escaped)
    payload = json.dumps({"text": escaped, "blocks": [{"type": "section",
        "text": {"type": "mrkdwn", "text": formatted, "verbatim": True}}],
        "unfurl_links": False, "unfurl_media": False}).encode()
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
            body = response.read(128)
            return ("sent", 0) if response.status == 200 and body.strip() == b"ok" else ("uncertain", 0)
    except urllib.error.HTTPError as error:
        if error.code == 429:
            try:
                delay = max(1, float(error.headers.get("Retry-After", "60")))
                if not delay < float("inf"):
                    delay = 60
            except ValueError:
                delay = 60
            return "pending", delay
        return ("pending", 0) if 500 <= error.code < 600 else ("blocked", 0)
    except (OSError, urllib.error.URLError, http.client.HTTPException):
        return "uncertain", 0


def gist_description(key: str) -> str:
    return f"slurm-watcher event {key}"


def run_gh(config: dict, arguments: list[str]) -> str:
    env = os.environ.copy()
    env.update(GH_PROMPT_DISABLED="1", NO_COLOR="1")
    try:
        result = subprocess.run([config["gh"], *arguments], env=env, text=True,
                                encoding="utf-8", errors="strict", capture_output=True,
                                timeout=config["gist_timeout"], check=True)
    except (subprocess.SubprocessError, UnicodeError, OSError) as error:
        raise RuntimeError("GitHub CLI request failed") from error
    return result.stdout


def find_gist(config: dict, key: str) -> str | None:
    description = gist_description(key)
    expression = f'.[] | select(.description == "{description}") | .html_url'
    output = run_gh(config, ["api", "--paginate", "gists?per_page=100",
                             "--jq", expression])
    urls = [line.strip() for line in output.splitlines() if line.strip()]
    if any(GIST_URL.fullmatch(url) is None for url in urls):
        raise RuntimeError("GitHub CLI returned an invalid gist URL")
    if len(set(urls)) > 1:
        raise RuntimeError("multiple gists exist for one watcher event")
    return urls[0] if urls else None


def ensure_gist(config: dict, root: Path, key: str) -> str:
    existing = find_gist(config, key)
    if existing:
        return existing
    directory = root / "reports" / key
    manifest_path = directory / "report.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        names = list(dict.fromkeys(item["copy"] for item in manifest["logs"]))
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RuntimeError("report manifest is unavailable or invalid") from error
    files = [manifest_path]
    for name in names:
        path = directory / name
        if (not isinstance(name, str) or Path(name).name != name or
                path.parent != directory or not path.is_file()):
            raise RuntimeError("report manifest references an invalid log copy")
        files.append(path)
    arguments = ["gist", "create", "--desc", gist_description(key),
                 *(str(path) for path in files)]
    try:
        output = run_gh(config, arguments)
    except RuntimeError:
        recovered = find_gist(config, key)
        if recovered:
            return recovered
        raise
    urls = [line.strip() for line in output.splitlines() if GIST_URL.fullmatch(line.strip())]
    if len(urls) != 1:
        raise RuntimeError("GitHub CLI did not return one gist URL")
    return urls[0]


DEFAULTS = {"state_dir": "~/.slurm-failures", "webhook_file": "~/.config/slurm-failure-watcher/webhook",
            "sacct": "sacct", "sacct_timeout": 120, "slack_timeout": 20,
            "gh": "gh", "gist_timeout": 120,
            "squeue": "squeue", "scancel": "scancel", "slurm_command_timeout": 120,
            "chain_scan_seconds": 60, "chain_watch_seconds": 86400,
            "overlap_seconds": 86400, "deep_scan_seconds": 86400, "settle_seconds": 60,
            "log_wait_seconds": 900, "retry_base_seconds": 60, "retry_max_seconds": 3600,
            "max_deliveries": 100}


class Watcher:
    def __init__(self, config: dict):
        unknown = config.keys() - DEFAULTS.keys()
        if unknown:
            raise ValueError(f"unknown configuration keys: {sorted(unknown)}")
        self.config = DEFAULTS | config
        for key, default in DEFAULTS.items():
            value = self.config[key]
            if isinstance(default, int) and (type(value) is not int or value <= 0):
                raise ValueError(f"{key} must be a positive integer")
            if isinstance(default, str) and (not isinstance(value, str) or not value):
                raise ValueError(f"{key} must be a nonempty string")
        self.root = Path(self.config["state_dir"]).expanduser().absolute()

    @contextlib.contextmanager
    def database(self):
        private_dir(self.root)
        lockfd = os.open(self.root / "watcher.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(lockfd, "w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield None
                return
            db = sqlite3.connect(self.root / "state.sqlite3", timeout=30)
            db.row_factory = sqlite3.Row
            if db.execute("PRAGMA user_version").fetchone()[0] not in (0, 1, 2, 3, 4):
                db.close()
                raise ValueError("unsupported state schema version")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, metadata TEXT NOT NULL, discovered REAL NOT NULL,
                    signature TEXT, stable_since REAL, report_done INTEGER NOT NULL DEFAULT 0,
                    report_attempts INTEGER NOT NULL DEFAULT 0, report_next REAL NOT NULL DEFAULT 0,
                    report_error TEXT, slack_status TEXT NOT NULL DEFAULT 'pending',
                    slack_attempts INTEGER NOT NULL DEFAULT 0, slack_next REAL NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS report_due ON jobs(report_next) WHERE report_done=0;
                CREATE INDEX IF NOT EXISTS slack_due ON jobs(slack_next) WHERE slack_status='pending';
            """)
            # Migrate existing v1 installs without resetting baseline or Slack state.
            with db:
                if "classification" not in {row[1] for row in db.execute("PRAGMA table_info(jobs)")}:
                    db.execute("ALTER TABLE jobs ADD COLUMN classification TEXT NOT NULL DEFAULT '{}'")
                    db.execute("UPDATE jobs SET report_done=0,report_next=0")
                columns = {row[1] for row in db.execute("PRAGMA table_info(jobs)")}
                if "gist_url" not in columns:
                    db.execute("ALTER TABLE jobs ADD COLUMN gist_url TEXT")
                    db.execute("ALTER TABLE jobs ADD COLUMN gist_attempts INTEGER NOT NULL DEFAULT 0")
                    db.execute("ALTER TABLE jobs ADD COLUMN gist_error TEXT")
                db.execute("PRAGMA user_version=4")
            chains.install_schema(db)
            try:
                yield db
            finally:
                db.close()

    def backoff(self, attempts: int) -> int:
        return min(self.config["retry_max_seconds"],
                   self.config["retry_base_seconds"] * 2 ** min(attempts, 16))

    def run(self, now: float | None = None, retry: str | None = None) -> bool:
        now = time.time() if now is None else now
        config = self.config
        with self.database() as db:
            if db is None:
                LOG.info("another watcher holds the lock; skipping")
                return True
            with db:
                db.execute("INSERT OR IGNORE INTO meta VALUES ('initialized', ?)", (int(now),))
                # A crash after the send marker cannot establish whether Slack accepted it.
                db.execute("UPDATE jobs SET slack_status='uncertain' WHERE slack_status='sending'")
                if retry:
                    db.execute("UPDATE jobs SET slack_status='pending', slack_next=0 WHERE slack_status=?", (retry,))
            meta = dict(db.execute("SELECT key, value FROM meta"))
            baseline = meta["initialized"]
            deep = now - meta.get("deep_scan", 0) >= config["deep_scan_seconds"]
            start = baseline if deep else max(baseline, meta.get("scan", baseline) - config["overlap_seconds"])
            healthy = True
            try:
                jobs = query(config, start, now)
                with db:
                    for job in jobs:
                        if epoch(job["End"]) <= baseline:
                            continue
                        key = identity(job)
                        existing = db.execute("SELECT metadata FROM jobs WHERE id=?", (key,)).fetchone()
                        data = json.dumps(job, sort_keys=True)
                        if existing and existing[0] != data:
                            db.execute("UPDATE jobs SET metadata=?, report_done=0, report_next=0 WHERE id=?", (data, key))
                        else:
                            db.execute("INSERT OR IGNORE INTO jobs(id,metadata,discovered) VALUES (?,?,?)", (key, data, now))
                    db.execute("INSERT OR REPLACE INTO meta VALUES ('scan', ?)", (now,))
                    if deep:
                        db.execute("INSERT OR REPLACE INTO meta VALUES ('deep_scan', ?)", (now,))
            except (RuntimeError, ValueError) as error:
                LOG.error("accounting scan failed: %s", error)
                healthy = False
            # Failure policy is independent of classification and all delivery work.
            if not chains.reconcile(db, config, now, self.backoff):
                LOG.error("afterok chain cancellation needs retry; inspect --status and reports")
                healthy = False
            rows = db.execute("""SELECT * FROM jobs WHERE
                (report_done=0 AND report_next<=?) OR (slack_status='pending' AND slack_next<=?)
                ORDER BY MIN(CASE WHEN report_done=0 THEN report_next ELSE 1e99 END,
                    CASE WHEN slack_status='pending' THEN slack_next ELSE 1e99 END), discovered LIMIT ?""",
                (now, now, config["max_deliveries"])).fetchall()
            for row in rows:
                job, key = json.loads(row["metadata"]), row["id"]
                cancellation = chains.summary(db, key)
                paths, details, available, signature = inspect_logs(job)
                since = row["stable_since"] if signature == row["signature"] else now
                complete = available and since is not None and now - since >= config["settle_seconds"]
                classification = classify_failure(job, paths, open_log)
                classification["provisional"] = not complete
                with db:
                    db.execute("UPDATE jobs SET signature=?,stable_since=?,classification=? WHERE id=?",
                               (signature, since, json.dumps(classification), key))
                    if (job["State"] == "CANCELLED" and classification["watchdog_detected"]
                            and row["slack_status"] == "ignored"):
                        db.execute("UPDATE jobs SET slack_status='pending',slack_next=0 WHERE id=?", (key,))
                if not row["report_done"] and row["report_next"] <= now:
                    error = None
                    written = False
                    try:
                        written, copy_errors = report(self.root, key, job, paths, details, complete, now,
                                                      classification, cancellation)
                        if copy_errors:
                            error = "LogCopyError"
                            healthy = False
                            complete = False
                    except OSError as exc:
                        error = type(exc).__name__  # No paths or source contents in service logs.
                        healthy = False
                    attempts = row["report_attempts"] + 1
                    with db:
                        db.execute("""UPDATE jobs SET report_done=?,report_attempts=?,report_next=?,
                            report_error=? WHERE id=?""", (int(written), attempts,
                            now + self.backoff(attempts - 1), error, key))
                if row["slack_status"] == "pending" and row["slack_next"] <= now:
                    if job["State"] == "CANCELLED" and not classification["watchdog_detected"]:
                        status = "ignored" if complete and now - row["discovered"] >= config["log_wait_seconds"] else "pending"
                        with db:
                            db.execute("UPDATE jobs SET slack_status=?,slack_next=? WHERE id=?",
                                       (status, now + config["retry_base_seconds"], key))
                        continue
                    gate = db.execute("SELECT value FROM meta WHERE key='slack_not_before'").fetchone()
                    if gate and gate[0] > now:
                        with db:
                            db.execute("UPDATE jobs SET slack_next=? WHERE id=?", (gate[0], key))
                        continue
                    if not complete and now - row["discovered"] < config["log_wait_seconds"]:
                        with db:
                            db.execute("UPDATE jobs SET slack_next=? WHERE id=?", (now + config["retry_base_seconds"], key))
                        continue
                    try:
                        url = read_secret(Path(config["webhook_file"]).expanduser())
                    except (OSError, ValueError):
                        LOG.error("webhook credential unavailable or invalid; delivery remains pending")
                        with db:
                            db.execute("UPDATE jobs SET slack_next=? WHERE id=?", (now + config["retry_max_seconds"], key))
                        healthy = False
                        continue
                    gist_url = row["gist_url"]
                    if not gist_url:
                        try:
                            gist_url = ensure_gist(config, self.root, key)
                        except RuntimeError as error:
                            attempts = row["gist_attempts"] + 1
                            with db:
                                db.execute("""UPDATE jobs SET gist_attempts=?,gist_error=?,slack_next=?
                                    WHERE id=?""", (attempts, type(error).__name__,
                                    now + self.backoff(attempts - 1), key))
                            LOG.error("secret gist creation failed; Slack delivery remains pending")
                            healthy = False
                            continue
                        with db:
                            db.execute("""UPDATE jobs SET gist_url=?,gist_attempts=gist_attempts+1,
                                gist_error=NULL WHERE id=?""", (gist_url, key))
                    text = slack_text(job, classification, cancellation, gist_url)
                    with db:
                        db.execute("UPDATE jobs SET slack_status='sending',slack_attempts=slack_attempts+1 WHERE id=?", (key,))
                    status, delay = post(url, text, config["slack_timeout"])
                    with db:
                        db.execute("UPDATE jobs SET slack_status=?,slack_next=? WHERE id=?", (status,
                            now + max(delay, self.backoff(row["slack_attempts"])), key))
                        db.execute("INSERT OR REPLACE INTO meta VALUES ('slack_not_before', ?)",
                                   (now + max(1, delay),))
                    if status != "sent":
                        LOG.warning("Slack event %s delivery status: %s", key[:16], status)
                        healthy = False
            return healthy
