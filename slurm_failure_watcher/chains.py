"""Durable, per-user cancellation of transitive afterok descendants."""
from __future__ import annotations

import datetime as dt
import json
import os
import pwd
import re
import subprocess

JOB_ID = re.compile(r"\d+(?:_\d+)?(?:\+\d+)?")
CLUSTER = re.compile(r"[A-Za-z0-9_.-]+")
FORMAT = "%i|%A|%V|%u|%E"


def environment():
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("SQUEUE_", "SCANCEL_")) and key != "SLURM_CLUSTERS"}
    env.update(TZ="UTC", SLURM_TIME_FORMAT="%Y-%m-%dT%H:%M:%S", SLURM_BITSTR_LEN="0")
    return env


def run_command(command, timeout):
    try:
        return subprocess.run(command, env=environment(), capture_output=True, text=True,
                              encoding="utf-8", errors="strict", timeout=timeout, check=True)
    except (OSError, subprocess.SubprocessError, UnicodeError) as error:
        # Command output may contain sensitive metadata; retain only a stable error label.
        raise RuntimeError(f"Slurm chain command failed ({type(error).__name__})") from error


def afterok(dependencies: str) -> set[str]:
    """Parse typed clauses, retaining OR edges as well as AND edges.

    squeue can annotate IDs with '(unfulfilled)' or '(failed)'. Never extract
    arbitrary numbers: offsets, job states, and other dependency types aren't edges.
    """
    if dependencies.upper() in {"", "NULL", "(NULL)", "N/A"}:
        return set()
    found = set()
    for clause in re.split(r"[,?]", dependencies):
        parts = clause.strip().split(":")
        if parts[0].lower() != "afterok":
            continue
        if len(parts) < 2:
            raise ValueError("invalid afterok dependency")
        for value in parts[1:]:
            match = re.fullmatch(r"(\d+(?:_\d+)?(?:\+\d+)?)(?:\([A-Za-z_ ]+\))?", value)
            if not match:
                raise ValueError("unsupported or truncated afterok dependency")
            found.add(match[1])
    return found


def snapshot(config: dict, cluster: str) -> dict[str, dict]:
    if not CLUSTER.fullmatch(cluster):
        raise ValueError("invalid accounting cluster name")
    user = pwd.getpwuid(os.getuid()).pw_name
    result = run_command([config["squeue"], f"--clusters={cluster}", "--local", "--noheader",
                          "--array", f"--user={user}", f"--format={FORMAT}"],
                         config["slurm_command_timeout"])
    jobs = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        fields = [value.strip() for value in line.split("|")]
        if len(fields) != 5:
            raise ValueError("malformed squeue record")
        job_id, raw, submitted, owner, dependencies = fields
        if not JOB_ID.fullmatch(job_id) or not raw.isdigit() or owner != user:
            raise ValueError("invalid squeue identity or ownership")
        dt.datetime.strptime(submitted, "%Y-%m-%dT%H:%M:%S")
        if job_id in jobs:
            raise ValueError("duplicate squeue identity")
        jobs[job_id] = {"id": job_id, "raw": raw, "submit": submitted,
                        "dependencies": dependencies, "afterok": sorted(afterok(dependencies))}
    return jobs


def node(job: dict) -> dict:
    return {"id": job["JobID"], "raw": job["JobIDRaw"], "submit": job["Submit"]}


def same_job(left: dict, right: dict) -> bool:
    return all(left[key] == right[key] for key in ("id", "raw", "submit"))


def references(dependency: str, ancestor: dict) -> bool:
    # As in cancel-slurm-chain.sh, heterogenous offsets refer to the allocation group.
    dep = dependency.split("+", 1)[0]
    display = ancestor["id"].split("+", 1)[0]
    raw = ancestor["raw"]
    if dep in {display, raw}:
        return True
    if "_" in display:
        # afterok on the aggregate array fails if ANY task fails; don't include siblings.
        return dep == display.split("_", 1)[0]
    return dep.startswith(display + "_")


def usable(ancestor: dict, queue: dict[str, dict], by_raw: dict[str, list[dict]]) -> bool:
    current = queue.get(ancestor["id"])
    if current and not same_job(current, ancestor):
        return False  # ID has been reused or this is a different requeue attempt.
    for current in by_raw.get(ancestor["raw"], []):
        if not same_job(current, ancestor):
            return False
    return True


def descendants(root: dict, saved: list[dict], queue: dict[str, dict]) -> list[dict]:
    by_raw, reverse, arrays = {}, {}, {}
    for current in queue.values():
        by_raw.setdefault(current["raw"], []).append(current)
        for dependency in current["afterok"]:
            dependency = dependency.split("+", 1)[0]
            reverse.setdefault(dependency, set()).add(current["id"])
            if "_" in dependency:
                arrays.setdefault(dependency.split("_", 1)[0], set()).add(current["id"])
    ancestors = [item for item in [root] + saved if usable(item, queue, by_raw)]
    selected = {}
    # Persisted ancestors keep deeper dependents reachable after their parent vanishes.
    for ancestor in ancestors:
        current = queue.get(ancestor["id"])
        if (ancestor is not root and current and same_job(current, ancestor)
                and current["dependencies"] == ancestor.get("dependencies")):
            selected[current["id"]] = current
    for ancestor in ancestors:
        display = ancestor["id"].split("+", 1)[0]
        aliases = {display, ancestor["raw"]}
        if "_" in display:
            aliases.add(display.split("_", 1)[0])
        related = set().union(*(reverse.get(alias, set()) for alias in aliases))
        if "_" not in display:
            related.update(arrays.get(display, set()))
        for identifier in sorted(related):
            current = queue[identifier]
            if current["id"] == root["id"] or current["id"] in selected:
                continue
            if any(references(dep, ancestor) for dep in current["afterok"]):
                selected[current["id"]] = current
                ancestors.append(current)
    return list(selected.values())


def install_schema(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS chain_roots (
            event TEXT PRIMARY KEY, watch_until REAL NOT NULL, next_scan REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'watching', attempts INTEGER NOT NULL DEFAULT 0, error TEXT);
        CREATE TABLE IF NOT EXISTS chain_members (
            event TEXT NOT NULL, target TEXT NOT NULL, submitted TEXT NOT NULL,
            metadata TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned',
            attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
            PRIMARY KEY(event,target,submitted));
        CREATE INDEX IF NOT EXISTS chain_due ON chain_roots(next_scan) WHERE status='watching';
    """)


def summary(db, event: str) -> dict:
    root = db.execute("SELECT * FROM chain_roots WHERE event=?", (event,)).fetchone()
    if root is None:
        return {}
    return {"status": root["status"], "next_scan": root["next_scan"], "error": root["error"],
            "targets": [dict(row) for row in db.execute("""SELECT target,submitted,status,attempts,error
                FROM chain_members WHERE event=? ORDER BY target,submitted""", (event,))]}


def report_state(value: dict) -> dict:
    return {key: item for key, item in value.items() if key != "next_scan"}


def refresh_report(db, event: str, before: dict):
    if report_state(summary(db, event)) != report_state(before):
        db.execute("UPDATE jobs SET report_done=0,report_next=0 WHERE id=?", (event,))


def reconcile(db, config: dict, now: float, backoff) -> bool:
    """Persist complete plans before side effects, then revalidate and request cancellation.

    Cancellation is repeatable. An acknowledgment is 'requested', not proof that a
    job is gone. On a later snapshot disappearance is recorded as 'gone'.
    """
    with db:
        db.execute("""INSERT OR IGNORE INTO chain_roots(event,watch_until)
            SELECT id, discovered + ? FROM jobs""", (config["chain_watch_seconds"],))
    roots = db.execute("""SELECT chain_roots.*,jobs.metadata FROM chain_roots JOIN jobs
        ON jobs.id=chain_roots.event WHERE chain_roots.status='watching' AND next_scan<=?
        ORDER BY next_scan,event LIMIT ?""", (now, config["max_deliveries"])).fetchall()
    groups = {}
    for root in roots:
        groups.setdefault(json.loads(root["metadata"])["Cluster"], []).append(root)
    healthy = True
    for cluster, group in groups.items():
        previous = {root["event"]: summary(db, root["event"]) for root in group}
        try:
            queue = snapshot(config, cluster)
            for root in group:
                event = root["event"]
                saved = [json.loads(row[0]) for row in db.execute("""SELECT metadata FROM chain_members
                    WHERE event=? AND status NOT IN ('replaced','detached')""", (event,))]
                planned = descendants(node(json.loads(root["metadata"])), saved, queue)
                with db:
                    for target in planned:
                        db.execute("""INSERT INTO chain_members(event,target,submitted,metadata)
                            VALUES (?,?,?,?) ON CONFLICT(event,target,submitted) DO UPDATE SET
                            metadata=excluded.metadata, status=CASE WHEN chain_members.status IN
                            ('gone','replaced','detached') THEN 'planned' ELSE chain_members.status END""",
                            (event, target["id"], target["submit"], json.dumps(target)))
            # No scancel until every full plan for this snapshot is durable.
            fresh = snapshot(config, cluster) if any(db.execute("""SELECT 1 FROM chain_members
                WHERE event=? AND status IN ('planned','requested') LIMIT 1""", (root["event"],)).fetchone()
                for root in group) else queue
            targets, members = {}, []
            for root in group:
                event = root["event"]
                rows = db.execute("""SELECT * FROM chain_members
                    WHERE event=? AND status IN ('planned','requested')""", (event,)).fetchall()
                with db:
                    for member in rows:
                        planned = json.loads(member["metadata"])
                        current = fresh.get(member["target"])
                        state = ("gone" if current is None else "replaced" if not same_job(current, planned)
                                 else "detached" if current["dependencies"] != planned["dependencies"] else None)
                        if state:
                            db.execute("""UPDATE chain_members SET status=?,error=NULL
                                WHERE event=? AND target=? AND submitted=?""",
                                (state, event, member["target"], member["submitted"]))
                        else:
                            targets[current["id"]] = current
                            members.append(member)
            ordered = sorted(targets)
            for offset in range(0, len(ordered), 100):
                batch = ordered[offset:offset + 100]
                # Pending heterogeneous components must be cancelled as a whole group.
                ids = sorted({target.split("+", 1)[0] for target in batch})
                batch_members = [member for member in members if member["target"] in batch]
                with db:
                    for member in batch_members:
                        db.execute("""UPDATE chain_members SET attempts=attempts+1 WHERE
                            event=? AND target=? AND submitted=?""",
                            (member["event"], member["target"], member["submitted"]))
                error = None
                try:
                    run_command([config["scancel"], f"--clusters={cluster}", "--quiet",
                                 f"--user={pwd.getpwuid(os.getuid()).pw_name}", *ids],
                                config["slurm_command_timeout"])
                except RuntimeError as exc:
                    error = str(exc)
                    healthy = False
                with db:
                    for member in batch_members:
                        db.execute("""UPDATE chain_members SET status=?,error=? WHERE
                            event=? AND target=? AND submitted=?""", ("planned" if error else "requested",
                            error, member["event"], member["target"], member["submitted"]))
            with db:
                for root in group:
                    event = root["event"]
                    pending_error = db.execute("""SELECT error FROM chain_members WHERE event=?
                        AND status='planned' AND error IS NOT NULL LIMIT 1""", (event,)).fetchone()
                    active = db.execute("""SELECT 1 FROM chain_members WHERE event=?
                        AND status IN ('planned','requested') LIMIT 1""", (event,)).fetchone()
                    status = "complete" if now >= root["watch_until"] and not active else "watching"
                    delay = backoff(root["attempts"]) if pending_error else config["chain_scan_seconds"]
                    db.execute("""UPDATE chain_roots SET status=?,next_scan=?,attempts=?,error=?
                        WHERE event=?""", (status, now + delay, root["attempts"] + 1 if pending_error else 0,
                        pending_error[0] if pending_error else None, event))
                    refresh_report(db, event, previous[event])
        except (RuntimeError, ValueError) as error:
            healthy = False
            with db:
                for root in group:
                    db.execute("""UPDATE chain_roots SET next_scan=?,attempts=attempts+1,error=?
                        WHERE event=?""", (now + backoff(root["attempts"]), str(error), root["event"]))
                    refresh_report(db, root["event"], previous[root["event"]])
    return healthy
