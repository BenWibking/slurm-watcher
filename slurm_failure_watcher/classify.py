"""Versioned deterministic rules; diagnoses are evidence-based hypotheses, not traces.

Quokka's output functions print a start marker without a matching finish marker.
A later coarse step or output phase is therefore important negative evidence.
Streams are analysed independently: their filesystem mtimes do not order lines.
"""
from __future__ import annotations

import os
from pathlib import Path
import re

RULESET = "quokka-v1"
START = re.compile(r"^(?:\d+:\s*)?(?:DiagPlotfile:\s*)?Writing\s+(plotfile|checkpoint)\s+(.+?)\s*$", re.I)
FINISH = re.compile(r"^(?:Write (?:small )?(plotfile|checkpoint) time\s*=|"
                    r"(?:Finished|Completed) writing (plotfile|checkpoint)\b)", re.I)
PROGRESS = re.compile(r"^(?:Coarse\s+STEP\s+\d+\b|\[STEP\s+\d+\]|STEP\s*=\s*\d+\b|"
                      r"Simulation ended at\b|AMReX \(.*\) finalized)", re.I)
WATCHDOG = re.compile(r"^WATCHDOG:\s+No log writes for (\d+)s\.", re.I)
NEVER_STARTED = re.compile(r"^WATCHDOG:\s+Log file never created after (\d+)s\.", re.I)
TIME_LIMIT = re.compile(r"(?:slurmstepd|srun):.*(?:DUE TO TIME LIMIT|TIME LIMIT exceeded)", re.I)
INFRASTRUCTURE = re.compile(r"^(?:WATCHDOG:|slurmstepd:|srun:|slurm_script:|"
                            r"\s*\d+:\s*(?:slurmstepd:|srun:))", re.I)
# These are sub-write messages, NOT completion of the whole plotfile/checkpoint.
WRITE_DETAIL = re.compile(r"^(?:\d+::)?(?:VisMF::Write|NFilesIter::)", re.I)
RUN_START = re.compile(r"^(?:Initializing AMReX \(|Broadcasting container image to all compute nodes\.\.\.)")
OOM = re.compile(r"(?:Detected \d+ oom_kill event|oom-kill|Out of memory: Killed process|"
                 r"HIP out of memory|CUDA error: out of memory|std::bad_alloc)", re.I)
IO_ERROR = re.compile(r"(?:No space left on device|Disk quota exceeded|Input/output error|"
                      r"Stale file handle|Read-only file system)", re.I)
ASSERTION = re.compile(r"(?:Assertion .+ failed|AMReX.*Assertion|amrex::Abort::|"
                       r"SIGSEGV|Segmentation fault|SIGFPE|Floating point exception)", re.I)


def evidence(path: Path, number: int, line: str) -> dict:
    return {"source": str(path), "line": number, "text": line[:400]}


def classify(job: dict, paths: list[Path], opener) -> dict:
    """Stream full files with bounded memory and bounded evidence storage.

    Each file is limited to its size at open so an actively growing log cannot
    keep the scan alive forever. Oversized lines are skipped, never partly matched.
    """
    candidates, watchdogs, never_started, errors, oom, aborts = [], [], [], [], [], []
    time_limits = []
    coverage, completions = [], set()

    def remember(bucket, item):
        # Keep the latest signature per stream, without merging stream chronology.
        bucket[:] = [old for old in bucket if old["source"] != item["source"]] + [item]

    for path in paths:
        pending, tail_is_write, number = None, False, 0
        info = {"source": str(path), "bytes_scanned": 0, "oversized_lines": 0, "complete": False}
        try:
            with opener(path) as stream:
                before = os.fstat(stream.fileno())
                remaining = before.st_size
                while remaining:
                    raw = stream.readline(min(65537, remaining))
                    if not raw:
                        break
                    remaining -= len(raw)
                    info["bytes_scanned"] += len(raw)
                    number += 1
                    if len(raw) > 65536:
                        info["oversized_lines"] += 1
                        tail_is_write = False
                        while remaining and not raw.endswith(b"\n"):
                            raw = stream.readline(min(65537, remaining))
                            if not raw:
                                break
                            remaining -= len(raw)
                            info["bytes_scanned"] += len(raw)
                        continue
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line:
                        continue
                    item = evidence(path, number, line)
                    if RUN_START.match(line):
                        # The supplied Quokka scripts may append logs across requeues.
                        # Discard older-attempt evidence at a recognized run boundary.
                        for bucket in (watchdogs, never_started, errors, oom, aborts, time_limits):
                            bucket[:] = [old for old in bucket if old["source"] != str(path)]
                        completions = {pair for pair in completions if pair[0] != str(path)}
                        pending, tail_is_write = None, False
                    if OOM.search(line):
                        remember(oom, item)
                    if IO_ERROR.search(line):
                        remember(errors, item)
                    if ASSERTION.search(line):
                        remember(aborts, item)
                    match = WATCHDOG.match(line)
                    if match:
                        remember(watchdogs, item | {"idle_seconds": int(match[1])})
                    if NEVER_STARTED.match(line):
                        remember(never_started, item)
                    if TIME_LIMIT.search(line):
                        remember(time_limits, item)
                    start = START.match(line)
                    if start:
                        pending = item | {"kind": start[1].lower(), "target": start[2][:300]}
                        tail_is_write = True
                    elif FINISH.match(line):
                        match = FINISH.match(line)
                        kind = next(g for g in match.groups() if g).lower()
                        completions.add((str(path), kind))
                        if pending and pending["kind"] == kind:
                            pending, tail_is_write = None, False
                    elif PROGRESS.match(line):
                        pending, tail_is_write = None, False
                    elif INFRASTRUCTURE.match(line) or (pending and WRITE_DETAIL.match(line)):
                        pass
                    else:
                        # Unknown application output prevents a strong terminal-phase inference.
                        tail_is_write = False
                after = os.fstat(stream.fileno())
                info["complete"] = (remaining == 0 and not info["oversized_lines"] and
                    (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns))
        except OSError as error:
            info["error"] = type(error).__name__
        coverage.append(info)
        if pending and tail_is_write:
            candidates.append(pending)

    state = job["State"]
    base = {"ruleset": RULESET, "coverage": coverage, "watchdog_detected": bool(watchdogs or never_started)}

    def result(category, confidence, rule, explanation, items=()):
        return base | {"category": category, "confidence": confidence, "rule": rule,
                       "explanation": explanation, "evidence": list(items)[:8]}

    if state in {"OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE"}:
        return result({"OUT_OF_MEMORY": "out_of_memory", "NODE_FAIL": "node_failure",
                       "BOOT_FAIL": "boot_failure", "DEADLINE": "deadline"}[state],
                      "accounting", "accounting-state", f"Slurm recorded {state}.")
    if oom:
        return result("out_of_memory", "log_evidence", "explicit-oom", "Explicit memory exhaustion in logs.", oom)
    if errors:
        return result("filesystem_error", "log_evidence", "explicit-filesystem-error",
                      "Explicit filesystem error; this is stronger evidence than an unfinished write marker.", errors)
    if aborts:
        return result("application_crash", "log_evidence", "explicit-application-crash",
                      "Explicit assertion, abort, or crash signal in logs.", aborts)
    timeout = state == "TIMEOUT" or bool(watchdogs or time_limits)
    # Do not join events from separate streams into an invented chronology. A finish
    # marker in another stream is conflicting evidence, so suppress the write rule.
    if not all(item["complete"] for item in coverage):
        candidates = []
    if candidates and timeout:
        candidate = candidates[-1]
        other_stream_finish = any(source != candidate["source"] and kind == candidate["kind"]
                                  for source, kind in completions)
        if not other_stream_finish:
            return result("suspected_output_write_stall", "probable" if watchdogs else "possible",
                          "watchdog-terminal-write" if watchdogs else "timeout-terminal-write",
                          f"Last recognized application phase was {candidate['kind']} write to {candidate['target']}; "
                          "no later completion or timestep progress was observed. This does not prove a blocked filesystem syscall.",
                          [candidate] + watchdogs + time_limits)
    if watchdogs or never_started:
        return result("watchdog_no_output", "log_evidence", "watchdog-no-output",
                      "Job script detected missing log progress; no terminal output-write phase was established.",
                      watchdogs + never_started)
    if state == "TIMEOUT":
        return result("timeout", "accounting", "timeout-without-write-evidence",
                      "Walltime expired; available logs do not establish an output-write stall.")
    if state == "CANCELLED":
        return result("cancelled", "accounting", "cancelled-without-watchdog",
                      "No job-script watchdog evidence found; Slack notification is suppressed.")
    return result("application_failure", "accounting", "failed-unclassified",
                  "Job failed without a more specific deterministic signature.")
