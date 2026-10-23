import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from slurm_failure_watcher.classify import classify
from slurm_failure_watcher import core as c
from test_watcher import job


class Classification(unittest.TestCase):
    def analyze(self, text, state="TIMEOUT", stderr=None):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "stdout"
            path.write_bytes(text.encode() if isinstance(text, str) else text)
            paths = [path]
            if stderr is not None:
                path = Path(name) / "stderr"
                path.write_text(stderr)
                paths.append(path)
            return classify(job(State=state), paths, c.open_log)

    def test_timeout_terminal_plotfile(self):
        result = self.analyze("Coarse STEP 42 at t = 1 starts ...\nWriting plotfile plt0000042\n"
                              "slurmstepd: error: *** JOB 42 CANCELLED DUE TO TIME LIMIT ***\n")
        self.assertEqual(result["category"], "suspected_output_write_stall")
        self.assertEqual(result["confidence"], "possible")
        self.assertEqual(result["evidence"][0]["target"], "plt0000042")
        self.assertEqual(result["evidence"][0]["line"], 2)

    def test_real_script_watchdog_checkpoint_cancelled(self):
        result = self.analyze("Writing checkpoint chk042\n"
            "WATCHDOG: No log writes for 900s. Collecting backtraces.\n"
            "WATCHDOG: Collecting backtraces into logs/watchdog_backtraces_42_20000101\n"
            "srun: Job step aborted: Waiting up to 32 seconds for job step to finish.\n", "CANCELLED")
        self.assertEqual(result["rule"], "watchdog-terminal-write")
        self.assertEqual(result["confidence"], "probable")
        self.assertEqual(result["evidence"][1]["idle_seconds"], 900)

    def test_subwrite_messages_do_not_complete_entire_output(self):
        result = self.analyze("Writing checkpoint chk0000010\n0::VisMF::Write: done Level_0/Cell\n")
        self.assertEqual(result["category"], "suspected_output_write_stall")

    def test_next_coarse_step_clears_output_phase(self):
        result = self.analyze("Writing plotfile plt0000042\nCoarse STEP 43 at t = 1 starts ...\n")
        self.assertEqual(result["category"], "timeout")

    def test_later_checkpoint_replaces_completed_plotfile_phase(self):
        result = self.analyze("Writing plotfile plt42\nWriting checkpoint chk42\n")
        self.assertEqual(result["evidence"][0]["kind"], "checkpoint")

    def test_explicit_completion_suppresses_stall(self):
        for text in ("Writing plotfile plt42\nWrite plotfile time = 1 seconds\n",
                     "Writing checkpoint chk42\nCompleted writing checkpoint chk42\n"):
            with self.subTest(text=text):
                self.assertEqual(self.analyze(text)["category"], "timeout")

    def test_old_completion_does_not_clear_new_same_stream_write(self):
        result = self.analyze("Writing plotfile plt41\nWrite plotfile time = 1 seconds\nWriting plotfile plt42\n",
                              stderr="slurmstepd: error: DUE TO TIME LIMIT\n")
        self.assertEqual(result["category"], "suspected_output_write_stall")

    def test_conflicting_completion_in_other_stream_is_conservative(self):
        result = self.analyze("Writing plotfile plt42\n", stderr="Completed writing plotfile plt42\n")
        self.assertEqual(result["category"], "timeout")

    def test_unknown_application_output_prevents_stall_claim(self):
        result = self.analyze("Writing plotfile plt42\nNow doing chemistry\n")
        self.assertEqual(result["category"], "timeout")

    def test_non_timeout_failure_with_write_marker_is_not_hang(self):
        self.assertEqual(self.analyze("Writing checkpoint chk42\n", "FAILED")["category"], "application_failure")

    def test_explicit_resource_and_io_errors_take_precedence(self):
        for state, text, category in (
            ("OUT_OF_MEMORY", "Writing plotfile plt42\n", "out_of_memory"),
            ("NODE_FAIL", "Writing checkpoint chk42\n", "node_failure"),
            ("BOOT_FAIL", "", "boot_failure"),
            ("DEADLINE", "", "deadline"),
            ("TIMEOUT", "Writing plotfile plt42\nNo space left on device\n", "filesystem_error"),
            ("FAILED", "Detected 1 oom_kill event in StepId=42.batch\n", "out_of_memory"),
            ("TIMEOUT", "Writing checkpoint chk42\namrex::Abort::failure\n", "application_crash"),
        ):
            with self.subTest(state=state, category=category):
                self.assertEqual(self.analyze(text, state)["category"], category)

    def test_watchdog_alone_and_missing_startup_are_not_output_stalls(self):
        for text in ("WATCHDOG: No log writes for 900s. Collecting backtraces.\n",
                     "WATCHDOG: Log file never created after 900s. Collecting backtraces.\n"):
            result = self.analyze(text, "CANCELLED")
            self.assertTrue(result["watchdog_detected"])
            self.assertEqual(result["category"], "watchdog_no_output")

    def test_long_binary_logs_have_bounded_evidence_and_no_tail_only_scan(self):
        text = b"Writing plotfile old\nCoarse STEP 43 at t = 1 starts ...\n"
        text += b"x" * 100000 + b"\n" + b"slurmstepd: error: DUE TO TIME LIMIT\n"
        result = self.analyze(text)
        self.assertEqual(result["category"], "timeout")
        self.assertEqual(result["coverage"][0]["oversized_lines"], 1)
        self.assertLess(len(json.dumps(result)), 3000)

    def test_no_logs_is_generic_timeout(self):
        self.assertEqual(classify(job(State="TIMEOUT"), [], c.open_log)["category"], "timeout")

    def test_write_at_end_of_large_log_is_found(self):
        result = self.analyze(("progress\n" * 100000) + "Writing plotfile plt42\n")
        self.assertEqual(result["category"], "suspected_output_write_stall")

    def test_appended_previous_attempt_does_not_supply_current_failure_evidence(self):
        result = self.analyze("Detected 1 oom_kill event\nWriting plotfile old\n"
                              "WATCHDOG: No log writes for 900s. Collecting backtraces.\n"
                              "Initializing AMReX (26.10)...\nWriting checkpoint new\n")
        self.assertEqual(result["category"], "suspected_output_write_stall")
        self.assertEqual(result["confidence"], "possible")
        self.assertFalse(result["watchdog_detected"])

    def test_new_script_start_resets_prior_output_stall(self):
        result = self.analyze("Writing checkpoint old\nWATCHDOG: No log writes for 900s. Collecting backtraces.\n"
                              "Broadcasting container image to all compute nodes...\nSBCAST failed!\n", "FAILED")
        self.assertEqual(result["category"], "application_failure")
        self.assertFalse(result["watchdog_detected"])


class Integration(unittest.TestCase):
    def setUp(self):
        mock = patch.object(c.chains, "snapshot", return_value={})
        mock.start()
        self.addCleanup(mock.stop)

    def test_watchdog_cancellation_alert_and_persisted_diagnosis(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            secret = root / "secret"
            secret.write_text("https://hooks.slack.com/services/test/test/test")
            secret.chmod(0o600)
            log = root / "output"
            log.write_text("Writing checkpoint chk42\nWATCHDOG: No log writes for 900s. Collecting backtraces.\n")
            w = c.Watcher({"state_dir": str(root / "state"), "webhook_file": str(secret),
                           "settle_seconds": 1, "retry_base_seconds": 1, "log_wait_seconds": 5})
            with patch.object(c, "query", return_value=[]) as query, patch.object(c, "post", return_value=("sent", 0)) as post:
                w.run(now=1000)
                query.return_value = [job(State="CANCELLED", StdOut=str(log), StdErr=str(log))]
                w.run(now=1101)
                w.run(now=1103)
                post.assert_called_once()
                self.assertIn("suspected_output_write_stall", post.call_args.args[1])
                with w.database() as db:
                    row = db.execute("SELECT * FROM jobs").fetchone()
                    diagnosis = json.loads(row["classification"])
                self.assertEqual(diagnosis["rule"], "watchdog-terminal-write")
                report = json.loads(next((w.root / "reports").glob("*/report.json")).read_text())
                self.assertEqual(report["classification"], diagnosis)

    def test_manual_cancellation_never_posts(self):
        with tempfile.TemporaryDirectory() as name:
            w = c.Watcher({"state_dir": name, "settle_seconds": 1,
                           "retry_base_seconds": 1, "log_wait_seconds": 5})
            with patch.object(c, "query", return_value=[]) as query, patch.object(c, "post") as post:
                w.run(now=1000)
                query.return_value = [job(State="CANCELLED")]
                for now in (1101, 1103, 1110, 1200):
                    w.run(now=now)
                post.assert_not_called()
                with w.database() as db:
                    self.assertEqual(db.execute("SELECT slack_status FROM jobs").fetchone()[0], "ignored")

    def test_cancelled_job_with_late_visible_watchdog_log(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            secret = root / "secret"
            secret.write_text("https://hooks.slack.com/services/test/test/test")
            secret.chmod(0o600)
            log = root / "late"
            w = c.Watcher({"state_dir": str(root / "state"), "webhook_file": str(secret),
                           "settle_seconds": 1, "retry_base_seconds": 1, "log_wait_seconds": 5})
            with patch.object(c, "query", return_value=[]) as query, patch.object(c, "post", return_value=("sent", 0)) as post:
                w.run(now=1000)
                query.return_value = [job(State="CANCELLED", StdOut=str(log), StdErr=str(log))]
                w.run(now=1101)
                w.run(now=1120)
                post.assert_not_called()
                log.write_text("Writing plotfile plt42\nWATCHDOG: No log writes for 900s. Collecting backtraces.\n")
                w.run(now=1125)
                w.run(now=1130)
                post.assert_called_once()
                self.assertIn("watchdog-terminal-write", post.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
