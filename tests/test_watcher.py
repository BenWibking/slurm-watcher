import io
import json
import os
from pathlib import Path
import pwd
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

from slurm_failure_watcher import core as c


def job(**changes):
    data = dict.fromkeys(c.FIELDS, "")
    data.update(Cluster="test", DBIndex="123", JobID="42", JobIDRaw="42",
                JobName="test job", User=pwd.getpwuid(os.getuid()).pw_name,
                State="FAILED", ExitCode="1:0", Submit=c.stamp(900),
                Start=c.stamp(950), End=c.stamp(1100), Restarts="0", NNodes="1",
                NTasks="1", WorkDir="/tmp", StdOut="/dev/null", StdErr="/dev/null")
    data.update(changes)
    return data


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.secret = self.root / "secret"
        self.secret.write_text("https://hooks.slack.com/services/TEST/TEST/test")
        self.secret.chmod(0o600)
        self.w = c.Watcher({"state_dir": str(self.root / "state"),
                            "webhook_file": str(self.secret), "settle_seconds": 1,
                            "log_wait_seconds": 10, "retry_base_seconds": 1})
        self.query = patch.object(c, "query", return_value=[]).start()
        self.post = patch.object(c, "post", return_value=("sent", 0)).start()
        patch.object(c.chains, "snapshot", return_value={}).start()
        self.addCleanup(patch.stopall)
        self.w.run(now=1000)

    def row(self):
        with self.w.database() as db:
            return dict(db.execute("SELECT * FROM jobs").fetchone())

    def discover(self, data=None):
        self.query.return_value = [data or job()]
        self.w.run(now=1101)

    def test_first_initialization_ignores_history_even_if_late(self):
        self.query.return_value = [job(End=c.stamp(999)), job(DBIndex="124", End=c.stamp(1000))]
        self.w.run(now=1200)
        with self.w.database() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0)
        self.post.assert_not_called()

    def test_settle_dedup_and_full_report(self):
        log = self.root / "output"
        content = b"begin\x00" + b"x" * 100000 + b"end\xff"
        log.write_bytes(content)
        self.discover(job(StdOut=str(log), StdErr=str(log)))
        self.post.assert_not_called()
        self.w.run(now=1103)
        self.w.run(now=1200)
        self.assertEqual(self.post.call_count, 1)
        row = self.row()
        self.assertEqual(row["slack_status"], "sent")
        self.assertEqual(row["report_done"], 1)
        directory = self.w.root / "reports" / row["id"]
        manifest = json.loads((directory / "report.json").read_text())
        self.assertEqual(len(manifest["logs"]), 1)
        self.assertEqual((directory / manifest["logs"][0]["copy"]).read_bytes(), content)
        self.assertLessEqual(len(self.post.call_args.args[1]), 2900)

    def test_log_delay_and_late_accounting_metadata(self):
        log = self.root / "late"
        self.discover(job(StdOut=str(log)))
        self.w.run(now=1112)
        self.assertEqual(self.row()["slack_status"], "sent")
        self.assertEqual(self.row()["report_done"], 0)
        log.write_text("late full log")
        self.w.run(now=1115)
        self.w.run(now=1120)
        self.assertEqual(self.row()["report_done"], 1)
        self.assertEqual(self.post.call_count, 1)
        self.query.return_value = [job(StdOut=str(log), ReqMem="1G")]
        self.w.run(now=1125)
        manifest = json.loads(next((self.w.root / "reports").glob("*/report.json")).read_text())
        self.assertEqual(manifest["job"]["ReqMem"], "1G")

    def test_report_failure_does_not_block_slack(self):
        self.discover()
        with patch.object(c, "report", side_effect=OSError("disk full")):
            self.w.run(now=1103)
        self.assertEqual(self.row()["slack_status"], "sent")
        self.assertEqual(self.row()["report_done"], 0)
        self.w.run(now=1200)
        self.assertEqual(self.row()["report_done"], 1)
        self.assertEqual(self.post.call_count, 1)

    def test_slack_retry_does_not_repeat_finished_report(self):
        self.discover()
        self.post.return_value = ("pending", 50)
        self.w.run(now=1103)
        self.assertEqual(self.row()["report_done"], 1)
        self.w.run(now=1120)
        self.assertEqual(self.post.call_count, 1)
        self.post.return_value = ("sent", 0)
        with patch.object(c, "report") as report:
            self.w.run(now=1160)
            report.assert_not_called()
        self.assertEqual(self.row()["slack_status"], "sent")

    def test_ambiguous_delivery_requires_explicit_retry(self):
        self.discover()
        self.post.return_value = ("uncertain", 0)
        self.w.run(now=1103)
        self.w.run(now=1200)
        self.assertEqual(self.post.call_count, 1)
        self.post.return_value = ("sent", 0)
        self.w.run(now=1201, retry="uncertain")
        self.assertEqual(self.post.call_count, 2)

    def test_crash_after_send_marker_is_held(self):
        self.discover()
        with self.w.database() as db, db:
            db.execute("UPDATE jobs SET slack_status='sending'")
        self.w.run(now=1200)
        self.assertEqual(self.row()["slack_status"], "uncertain")
        self.post.assert_not_called()

    def test_scan_failure_preserves_cursor_but_retries_deliveries(self):
        self.discover()
        self.query.side_effect = RuntimeError("offline")
        self.assertFalse(self.w.run(now=1200))
        self.assertEqual(self.row()["slack_status"], "sent")
        with self.w.database() as db:
            self.assertEqual(dict(db.execute("SELECT * FROM meta"))["scan"], 1101)

    def test_overlap_downtime_and_deep_scan(self):
        self.w.config["overlap_seconds"] = 10
        self.w.config["deep_scan_seconds"] = 100
        self.w.run(now=1050)
        self.assertEqual(self.query.call_args.args[1], 1000)
        self.w.run(now=1060)
        self.assertEqual(self.query.call_args.args[1], 1040)
        self.w.run(now=2000)
        self.assertEqual(self.query.call_args.args[1], 1000)
        self.query.return_value = [job(End=c.stamp(1100))]
        self.w.run(now=2101)
        self.assertIsNotNone(self.row())

    def test_process_lock_skips_all_work(self):
        self.query.reset_mock()
        with self.w.database():
            self.assertTrue(c.Watcher(self.w.config).run(now=1200))
        self.query.assert_not_called()

    def test_distinct_arrays_and_database_entries(self):
        self.query.return_value = [job(JobID="42_1", DBIndex="1"),
                                   job(JobID="42_2", DBIndex="2"),
                                   job(DBIndex="3", Submit=c.stamp(1001))]
        self.w.run(now=1101)
        with self.w.database() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0], 3)

    def test_missing_secret_retries_without_marking_sending(self):
        self.discover()
        self.secret.unlink()
        self.w.run(now=1103)
        self.assertEqual(self.row()["slack_status"], "pending")
        self.assertEqual(self.row()["slack_attempts"], 0)

    def test_global_rate_limit_survives_next_invocation(self):
        self.query.return_value = [job(DBIndex="1"), job(DBIndex="2")]
        self.w.run(now=1101)
        self.post.return_value = ("pending", 120)
        self.w.run(now=1103)
        self.assertEqual(self.post.call_count, 1)
        self.w.run(now=1150)
        self.assertEqual(self.post.call_count, 1)
        self.post.return_value = ("sent", 0)
        self.w.run(now=1225)
        self.assertEqual(self.post.call_count, 2)
        self.w.run(now=1227)
        self.assertEqual(self.post.call_count, 3)

    def test_lock_excludes_another_process(self):
        script = ("import fcntl,os,sys; "
                  "f=open(sys.argv[1], 'w'); "
                  "fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)")
        import sys
        with self.w.database():
            child = subprocess.run([sys.executable, "-c", script, str(self.w.root / "watcher.lock")],
                                   capture_output=True, timeout=10)
        self.assertNotEqual(child.returncode, 0)
        self.assertIn(b"BlockingIOError", child.stderr)

    def test_copy_failure_keeps_other_available_logs(self):
        first, second = self.root / "a", self.root / "b"
        first.write_bytes(b"available")
        second.write_bytes(b"unreadable")
        directory = self.w.root / "reports" / "test"
        real = c.copy_log

        def copy(directory, path, number):
            if path == second:
                raise PermissionError("denied")
            return real(directory, path, number)

        with patch.object(c, "copy_log", side_effect=copy):
            self.assertEqual(c.report(self.w.root, "test", job(), [first, second], [], True, 1200),
                             (False, True))
        manifest = json.loads((directory / "report.json").read_text())
        self.assertEqual(len(manifest["logs"]), 1)
        self.assertEqual(len(manifest["copy_errors"]), 1)
        self.assertFalse(manifest["complete"])

    def test_interrupted_manifest_keeps_prior_snapshot_intact(self):
        log = self.root / "log"
        log.write_bytes(b"original")
        c.report(self.w.root, "test", job(), [log], [], True, 1200)
        directory = self.w.root / "reports" / "test"
        original = (directory / "report.json").read_bytes()
        log.write_bytes(b"replacement")
        with patch.object(c, "atomic_json", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                c.report(self.w.root, "test", job(), [log], [], True, 1300)
        self.assertEqual((directory / "report.json").read_bytes(), original)
        metadata = json.loads(original)
        self.assertEqual((directory / metadata["logs"][0]["copy"]).read_bytes(), b"original")

    def test_refresh_preserves_last_copy_when_source_disappears(self):
        log = self.root / "log"
        log.write_bytes(b"retained")
        c.report(self.w.root, "test", job(), [log], [], True, 1200)
        log.unlink()
        c.report(self.w.root, "test", job(), [], [], False, 1300)
        directory = self.w.root / "reports" / "test"
        manifest = json.loads((directory / "report.json").read_text())
        self.assertFalse(manifest["logs"][0]["current"])
        self.assertEqual((directory / manifest["logs"][0]["copy"]).read_bytes(), b"retained")

    def test_excerpts_preserve_final_bytes_of_both_streams(self):
        paths = [self.root / "stdout", self.root / "stderr"]
        for i, path in enumerate(paths):
            path.write_text("x" * 10000 + f"FINAL-{i}")
        text = c.slack_text(job(), "test", self.w.root, paths, True, 500)
        self.assertIn("FINAL-0", text)
        self.assertIn("FINAL-1", text)
        self.assertLessEqual(len(text), 2900)

    def test_future_schema_rejected(self):
        with self.w.database() as db:
            db.execute("PRAGMA user_version=99")
        with self.assertRaises(ValueError):
            self.w.run(now=1200)

    def test_v1_migration_preserves_baseline_and_sent_slack(self):
        import sqlite3
        if sqlite3.sqlite_version_info < (3, 35):
            self.skipTest("fixture downgrade needs SQLite DROP COLUMN")
        self.discover()
        self.w.run(now=1103)
        with self.w.database() as db, db:
            db.execute("ALTER TABLE jobs DROP COLUMN classification")
            db.execute("PRAGMA user_version=1")
        self.w.run(now=1200)
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(self.row()["slack_status"], "sent")
        self.assertEqual(json.loads(self.row()["classification"])["ruleset"], "quokka-v1")
        with self.w.database() as db:
            self.assertEqual(dict(db.execute("SELECT * FROM meta"))["initialized"], 1000)
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 3)


class Accounting(unittest.TestCase):
    @patch.object(c.subprocess, "run")
    def test_command_and_states(self, run):
        for state in c.FAILURES:
            data = job(State=state + "+")
            run.return_value = subprocess.CompletedProcess([], 0, c.SEP.join(data[f] for f in c.FIELDS) + "\n", "")
            result = c.query(c.DEFAULTS, 1000, 2000)
            self.assertEqual(result[0]["State"], state)
        args = run.call_args.args[0]
        for flag in ("--local", "--allocations", "--array", "--duplicates", "--parsable2"):
            self.assertIn(flag, args)
        self.assertIn("--starttime=" + c.stamp(1000), args)
        self.assertIn("--endtime=" + c.stamp(2000), args)
        self.assertEqual(run.call_args.kwargs["env"]["TZ"], "UTC")

    @patch.object(c.subprocess, "run")
    def test_malformed_records_fail_closed(self, run):
        for output in ("bad|record\n", c.SEP.join("" for _ in c.FIELDS)):
            run.return_value = subprocess.CompletedProcess([], 0, output, "")
            with self.assertRaises(ValueError):
                c.query(c.DEFAULTS, 1000, 2000)

    @patch.object(c.subprocess, "run", side_effect=subprocess.TimeoutExpired("sacct", 1))
    def test_timeout(self, run):
        with self.assertRaises(RuntimeError):
            c.query(c.DEFAULTS, 1000, 2000)

    @patch.object(c.subprocess, "run")
    def test_steps_nonfailures_and_unknown_end_are_excluded(self, run):
        data = [job(JobIDRaw="42.batch"), job(State="COMPLETED"), job(End="Unknown")]
        run.return_value = subprocess.CompletedProcess([], 0,
            "\n".join(c.SEP.join(j[f] for f in c.FIELDS) for j in data), "")
        self.assertEqual(c.query(c.DEFAULTS, 1000, 2000), [])

    @patch.object(c.subprocess, "run")
    def test_pipes_and_padded_columns(self, run):
        data = job(JobName="name|with|pipes", StdOut="/tmp/out|file")
        run.return_value = subprocess.CompletedProcess([], 0,
            c.SEP.join(" " + data[f] + " " for f in c.FIELDS), "")
        self.assertEqual(c.query(c.DEFAULTS, 1000, 2000)[0], data)

    def test_fallback_identity_tracks_recycled_ids(self):
        self.assertNotEqual(c.identity(job(DBIndex="0")),
                            c.identity(job(DBIndex="0", Submit=c.stamp(901))))
        self.assertNotEqual(c.identity(job()), c.identity(job(Cluster="other")))


class Paths(unittest.TestCase):
    def test_patterns_and_glob_literal(self):
        with tempfile.TemporaryDirectory() as directory:
            data = job(JobID="42_17", JobIDRaw="99", WorkDir=directory, JobName="a*[b]")
            path = Path(directory) / "42-0017-7-99-99-a*[b]-%-0.out"
            path.touch()
            found, count, note = c.resolve("%A-%04a-%b-%j-%J-%x-%%-%r.out", data)
            self.assertEqual(found, [path])
            self.assertEqual(count, 1)

    def test_node_and_rank_fanout(self):
        with tempfile.TemporaryDirectory() as directory:
            data = job(WorkDir=directory, NNodes="2", NTasks="2")
            for name in ("node-00", "node-01", "node-no", "host-n1", "host-n2"):
                (Path(directory) / name).touch()
            self.assertEqual(len(c.resolve("node-%02n", data)[0]), 2)
            self.assertEqual(len(c.resolve("host-%N", data)[0]), 2)
            self.assertEqual(c.resolve("node-%t", data)[1], 2)

    def test_unknown_pattern_is_explicit(self):
        self.assertIn("unsupported", c.resolve("foo-%s", job())[2])
        self.assertEqual(c.resolve("/dev/null", job())[1], 0)
        self.assertEqual(c.resolve("", job())[1], 1)

    def test_mixed_fanout_cannot_claim_completeness(self):
        with tempfile.TemporaryDirectory() as directory:
            data = job(WorkDir=directory, StdOut="%N-%t", NNodes="1", NTasks="1")
            (Path(directory) / "host-0").touch()
            self.assertFalse(c.inspect_logs(data)[2])

    def test_fifo_is_not_opened_as_a_blocking_log(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fifo"
            os.mkfifo(path)
            with self.assertRaises(OSError):
                c.open_log(path)

    def test_nonarray_and_literal_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            data = job(WorkDir=directory)
            path = Path(directory) / "%j"
            path.touch()
            self.assertEqual(c.resolve("\\%j", data)[0], [path])
            path = Path(directory) / "42-4294967294"
            path.touch()
            self.assertEqual(c.resolve("%A-%a", data)[0], [path])


class HttpAndSecurity(unittest.TestCase):
    @patch.object(c.urllib.request, "build_opener")
    def test_http_success_and_plain_text(self, opener):
        response = opener.return_value.open.return_value.__enter__.return_value
        response.status, response.read.return_value = 200, b"ok"
        self.assertEqual(c.post("https://example.test", "<@U123> ```", 1), ("sent", 0))
        request = opener.return_value.open.call_args.args[0]
        payload = json.loads(request.data)
        self.assertEqual(payload["blocks"][0]["text"]["type"], "plain_text")
        self.assertIn("&lt;", payload["text"])

    @patch.object(c.urllib.request, "build_opener")
    def test_http_error_classification(self, opener):
        for code, expected in ((429, "pending"), (500, "pending"), (403, "blocked"), (302, "blocked")):
            opener.return_value.open.side_effect = urllib.error.HTTPError(
                "https://secret", code, "error", {"Retry-After": "123"}, io.BytesIO())
            status, delay = c.post("https://example.test", "x", 1)
            self.assertEqual(status, expected)
            if code == 429:
                self.assertEqual(delay, 123)
        opener.return_value.open.side_effect = urllib.error.URLError("secret URL")
        self.assertEqual(c.post("https://example.test", "x", 1)[0], "uncertain")

    def test_secure_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "webhook"
            path.write_text("https://hooks.slack.com/services/a/b/c")
            path.chmod(0o600)
            self.assertTrue(c.read_secret(path).startswith("https:"))
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                c.read_secret(path)
            path.chmod(0o600)
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                c.read_secret(link)
            path.write_text("https://evil.example/services/a/b/c")
            with self.assertRaises(ValueError):
                c.read_secret(path)

    def test_configuration_validation(self):
        for config in ({"excerpt_chars": 3000}, {"sacct_timeout": 0}, {"typo": 1}, {"settle_seconds": True}):
            with self.assertRaises(ValueError):
                c.Watcher(config)


if __name__ == "__main__":
    unittest.main()
