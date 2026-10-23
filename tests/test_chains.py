import json
import os
from pathlib import Path
import pwd
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from slurm_failure_watcher import chains as ch
from slurm_failure_watcher import core as c
from test_watcher import job


def queued(identifier, dependencies="(null)", raw=None, submitted=None):
    return {"id": identifier, "raw": raw or identifier.split("_", 1)[0].split("+", 1)[0],
            "submit": submitted or c.stamp(950), "dependencies": dependencies,
            "afterok": sorted(ch.afterok(dependencies))}


class Graph(unittest.TestCase):
    def test_typed_clauses_and_annotations(self):
        self.assertEqual(ch.afterok("afterok:12(unfulfilled):13(failed),afterany:99,after:88+5"), {"12", "13"})
        self.assertEqual(ch.afterok("afterok:12?afterok:13?afternotok:99"), {"12", "13"})
        self.assertEqual(ch.afterok("singleton"), set())
        self.assertEqual(ch.afterok("NULL"), set())
        for value in ("afterok", "afterok:12...", "afterok:12:garbage"):
            with self.assertRaises(ValueError):
                ch.afterok(value)

    def test_full_transitive_branching_plan(self):
        queue = {node["id"]: node for node in [queued("43", "afterok:42"),
            queued("44", "afterok:43"), queued("45", "afterok:42"),
            queued("46", "afterany:42"), queued("47", "afternotok:42"),
            queued("48", "afterok:999?afterok:44")]}
        self.assertEqual({node["id"] for node in ch.descendants(ch.node(job()), [], queue)},
                         {"43", "44", "45", "48"})

    def test_cycle_does_not_loop_or_reinclude_root(self):
        queue = {"43": queued("43", "afterok:44:42"), "44": queued("44", "afterok:43")}
        self.assertEqual(len(ch.descendants(ch.node(job()), [], queue)), 2)

    def test_array_element_failure_breaks_aggregate_but_not_sibling(self):
        root = ch.node(job(JobID="42_3", JobIDRaw="99"))
        queue = {node["id"]: node for node in [queued("50", "afterok:42"),
            queued("51", "afterok:42_3"), queued("52", "afterok:99"),
            queued("53", "afterok:42_4"), queued("54", "aftercorr:42")]}
        self.assertEqual({node["id"] for node in ch.descendants(root, [], queue)}, {"50", "51", "52"})

    def test_whole_array_and_heterogeneous_references(self):
        self.assertTrue(ch.references("42_3", ch.node(job())))
        self.assertTrue(ch.references("42+1", ch.node(job(JobID="42+0"))))
        self.assertFalse(ch.references("420", ch.node(job())))

    def test_recycled_root_is_not_followed(self):
        queue = {"42": queued("42", submitted=c.stamp(2000)), "43": queued("43", "afterok:42")}
        self.assertEqual(ch.descendants(ch.node(job()), [], queue), [])

    def test_saved_missing_intermediate_remains_an_ancestor(self):
        queue = {"44": queued("44", "afterok:43")}
        self.assertEqual([node["id"] for node in ch.descendants(ch.node(job()),
                         [queued("43", "afterok:42")], queue)], ["44"])


class Commands(unittest.TestCase):
    @patch.object(ch, "run_command")
    def test_snapshot_syntax_and_ownership(self, run):
        user = pwd.getpwuid(os.getuid()).pw_name
        run.return_value = subprocess.CompletedProcess([], 0,
            f"43|43|{c.stamp(950)}|{user}|afterok:42(unfulfilled)\n", "")
        queue = ch.snapshot(c.DEFAULTS, "example-cluster")
        self.assertEqual(queue["43"]["afterok"], ["42"])
        args = run.call_args.args[0]
        self.assertIn("--clusters=example-cluster", args)
        self.assertIn("--array", args)
        self.assertIn(f"--user={user}", args)
        self.assertIn("--format=%i|%A|%V|%u|%E", args)

    @patch.object(ch, "run_command")
    def test_snapshot_fails_closed(self, run):
        for line in ("43|43|bad time|other|afterok:42", "43_[1-3]|43|date|other|afterok:42", "bad record"):
            run.return_value = subprocess.CompletedProcess([], 0, line, "")
            with self.assertRaises(ValueError):
                ch.snapshot(c.DEFAULTS, "example-cluster")
        with self.assertRaises(ValueError):
            ch.snapshot(c.DEFAULTS, "example-cluster,other")

    @patch.dict(os.environ, {"SQUEUE_STATES": "RUNNING", "SCANCEL_SIGNAL": "STOP", "SLURM_CLUSTERS": "other"})
    @patch.object(ch.subprocess, "run")
    def test_command_environment_and_timeout(self, run):
        ch.run_command(["squeue"], 25)
        env = run.call_args.kwargs["env"]
        self.assertNotIn("SQUEUE_STATES", env)
        self.assertNotIn("SCANCEL_SIGNAL", env)
        self.assertNotIn("SLURM_CLUSTERS", env)
        self.assertEqual(env["TZ"], "UTC")
        self.assertEqual(run.call_args.kwargs["timeout"], 25)
        run.side_effect = subprocess.TimeoutExpired("squeue", 25)
        with self.assertRaises(RuntimeError):
            ch.run_command(["squeue"], 25)


class DurableCancellation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.w = c.Watcher({"state_dir": self.temp.name, "chain_scan_seconds": 1,
                            "chain_watch_seconds": 20, "retry_base_seconds": 1, "settle_seconds": 1})
        self.accounting = patch.object(c, "query", return_value=[]).start()
        self.queue = patch.object(ch, "snapshot", return_value={}).start()
        self.command = patch.object(ch, "run_command").start()
        self.post = patch.object(c, "post", return_value=("sent", 0)).start()
        self.gist = patch.object(c, "ensure_gist",
                                 return_value="https://gist.github.com/test/abc123").start()
        self.addCleanup(patch.stopall)
        self.w.run(now=1000)

    def run_failure(self, state="FAILED"):
        self.accounting.return_value = [job(State=state)]
        return self.w.run(now=1101)

    def members(self):
        with self.w.database() as db:
            return [dict(row) for row in db.execute("SELECT * FROM chain_members ORDER BY target")]

    def test_entire_plan_is_committed_before_scancel(self):
        self.queue.return_value = {"43": queued("43", "afterok:42"), "44": queued("44", "afterok:43")}

        def check(command, timeout):
            import sqlite3
            with sqlite3.connect(Path(self.temp.name) / "state.sqlite3") as db:
                rows = db.execute("SELECT target,status FROM chain_members ORDER BY target").fetchall()
                self.assertEqual(rows, [("43", "planned"), ("44", "planned")])
            self.assertEqual(command[-2:], ["43", "44"])
            self.assertIn("--clusters=test", command)
            self.assertIn(f"--user={pwd.getpwuid(os.getuid()).pw_name}", command)

        self.command.side_effect = check
        self.run_failure()
        self.assertEqual({row["status"] for row in self.members()}, {"requested"})
        self.assertEqual(self.queue.call_count, 2)
        self.post.assert_not_called()

    def test_all_failure_causes_and_manual_cancel_trigger_policy(self):
        self.accounting.return_value = [job(State=state, JobID=str(40+i), JobIDRaw=str(40+i), DBIndex=str(i+1))
                                        for i, state in enumerate(sorted(c.WATCHED))]
        self.queue.return_value = {str(140+i): queued(str(140+i), f"afterok:{40+i}") for i in range(len(c.WATCHED))}
        self.w.run(now=1101)
        self.assertEqual(len(self.members()), len(c.WATCHED))
        self.assertEqual(self.command.call_count, 1)

    def test_invalid_queue_never_cancels_and_does_not_block_report(self):
        self.queue.side_effect = ValueError("bad queue")
        self.assertFalse(self.run_failure())
        self.command.assert_not_called()
        self.assertTrue(list((Path(self.temp.name) / "reports").glob("*/report.json")))
        with self.w.database() as db:
            self.assertEqual(db.execute("SELECT error FROM chain_roots").fetchone()[0], "bad queue")

    def test_cancellation_failure_retries_even_when_accounting_is_down(self):
        self.queue.return_value = {"43": queued("43", "afterok:42")}
        self.command.side_effect = RuntimeError("unavailable")
        self.assertFalse(self.run_failure())
        self.assertEqual(self.members()[0]["status"], "planned")
        self.accounting.side_effect = RuntimeError("offline")
        self.command.side_effect = None
        self.w.run(now=1103)
        self.assertEqual(self.command.call_count, 2)
        self.assertEqual(self.members()[0]["status"], "requested")

    def test_second_snapshot_revalidates_identity_and_dependency(self):
        initial = {"43": queued("43", "afterok:42"), "44": queued("44", "afterok:43"),
                   "45": queued("45", "afterok:42")}
        fresh = {"43": queued("43", "afterok:42", submitted=c.stamp(2000)),
                 "44": queued("44", "afterok:999")}
        self.queue.side_effect = [initial, fresh]
        self.run_failure()
        self.command.assert_not_called()
        self.assertEqual([row["status"] for row in self.members()], ["replaced", "detached", "gone"])

    def test_missing_intermediate_and_late_deeper_descendant(self):
        self.queue.return_value = {"43": queued("43", "afterok:42")}
        self.run_failure()
        self.queue.return_value = {"44": queued("44", "afterok:43")}
        self.w.run(now=1103)
        self.assertEqual(self.command.call_args.args[0][-1], "44")
        self.assertEqual([row["status"] for row in self.members()], ["gone", "requested"])
        self.queue.return_value = {}
        self.w.run(now=1105)
        self.assertEqual({row["status"] for row in self.members()}, {"gone"})

    def test_crash_after_request_is_safely_reconciled(self):
        self.queue.return_value = {"43": queued("43", "afterok:42")}
        self.command.side_effect = SystemExit("simulated crash")
        with self.assertRaises(SystemExit):
            self.run_failure()
        self.assertEqual(self.members()[0]["status"], "planned")
        self.queue.return_value = {}
        self.command.side_effect = None
        self.w.run(now=1103)
        self.assertEqual(self.members()[0]["status"], "gone")
        self.assertEqual(self.command.call_count, 1)

    def test_watch_window_closes_but_pending_targets_keep_retrying(self):
        self.queue.return_value = {"43": queued("43", "afterok:42")}
        self.run_failure()
        self.w.run(now=1200)
        with self.w.database() as db:
            self.assertEqual(db.execute("SELECT status FROM chain_roots").fetchone()[0], "watching")
        self.queue.return_value = {}
        self.w.run(now=1202)
        with self.w.database() as db:
            self.assertEqual(db.execute("SELECT status FROM chain_roots").fetchone()[0], "complete")

    def test_empty_snapshots_do_not_force_full_log_refresh(self):
        self.run_failure()
        self.w.run(now=1103)
        with patch.object(c, "report") as report:
            self.w.run(now=1105)
            report.assert_not_called()

    def test_reappearing_target_can_be_retried(self):
        queue = {"43": queued("43", "afterok:42")}
        self.queue.return_value = queue
        self.run_failure()
        self.queue.return_value = {}
        self.w.run(now=1103)
        self.queue.return_value = queue
        self.w.run(now=1105)
        self.assertEqual(self.command.call_count, 2)
        self.assertEqual(self.members()[0]["status"], "requested")

    def test_no_historical_roots_on_initialization(self):
        self.accounting.return_value = [job(End=c.stamp(999))]
        self.w.run(now=1101)
        self.queue.assert_not_called()
        self.command.assert_not_called()

    def test_partial_batch_failure_keeps_successful_requests(self):
        self.queue.return_value = {str(200+i): queued(str(200+i), "afterok:42") for i in range(205)}
        self.command.side_effect = [RuntimeError("unavailable"), None, None]
        self.assertFalse(self.run_failure())
        self.assertEqual(self.command.call_count, 3)
        self.assertEqual(sum(row["status"] == "planned" for row in self.members()), 100)
        self.assertEqual(sum(row["status"] == "requested" for row in self.members()), 105)

    def test_crash_during_revalidation_leaves_durable_plan(self):
        self.queue.side_effect = [{"43": queued("43", "afterok:42")}, RuntimeError("offline")]
        self.assertFalse(self.run_failure())
        self.command.assert_not_called()
        self.assertEqual(self.members()[0]["status"], "planned")
        self.queue.side_effect = None
        self.queue.return_value = {"43": queued("43", "afterok:42")}
        self.w.run(now=1103)
        self.command.assert_called_once()

    def test_shared_descendant_is_cancelled_once_per_snapshot(self):
        self.accounting.return_value = [job(), job(JobID="41", JobIDRaw="41", DBIndex="124")]
        self.queue.return_value = {"43": queued("43", "afterok:41:42")}
        self.w.run(now=1101)
        self.command.assert_called_once()
        self.assertEqual(self.command.call_args.args[0][-1], "43")
        self.assertEqual(len(self.members()), 2)


if __name__ == "__main__":
    unittest.main()
