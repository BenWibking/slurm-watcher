# Slurm failure watcher

This Python tool checks your Slurm jobs and sends failure summaries to Slack.
It also cancels jobs that depend on a failed job through an `afterok` dependency.
An `afterok` dependency permits a job to start only after the specified job succeeds.

Run the watcher on a login node. Each run checks once and exits.
Use cron or a user systemd timer to run it again.
Do not submit the watcher with `sbatch`.

## Requirements

- Python 3.11 or later. The tool uses only the Python standard library.
- Access to `sacct`, `squeue`, and `scancel` as your normal user.
- Read access to job logs from the login node.
- Outbound HTTPS access to a Slack incoming webhook.
- Site permission to run a periodic process on a login node.

Administrator access and Slurm configuration changes are not necessary.

## Quick start

### 1. Install and configure

Run these commands from this repository on the login node:

```sh
python3 --version
python3 -m venv ~/.local/venvs/slurm-failure-watcher
~/.local/venvs/slurm-failure-watcher/bin/python -m pip install .
install -d -m 700 ~/.config/slurm-failure-watcher ~/.slurm-failures
install -m 600 examples/config.json ~/.config/slurm-failure-watcher/config.json
command -v sacct
command -v squeue
command -v scancel
```

Use a different Python executable if `python3` is older than version 3.11.
Edit the installed `config.json` file.
Set `sacct`, `squeue`, and `scancel` to the absolute paths shown by `command -v`.

If Slurm needs environment modules, use a private wrapper script to load them.
Configure cron or systemd to run that script.
These schedulers do not load your interactive shell settings.

### 2. Configure Slack

Create an incoming webhook for the intended channel.
Follow [Slack's webhook instructions](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/).
A webhook URL is a credential that permits messages to be sent to that channel.

Use a trusted text editor to put the URL on one line in this file:

```text
~/.config/slurm-failure-watcher/webhook
```

Set its permissions:

```sh
chmod 600 ~/.config/slurm-failure-watcher/webhook
```

Keep the URL outside this repository.
Do not put it in shell commands, shell history, or configuration JSON.
The watcher rejects symbolic links and credentials that other users can read.

### 3. Initialize the watcher

Run the watcher once, then check its status:

```sh
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher --status
```

The first run saves the current UTC second before it queries Slurm.
Only jobs that end after that second are eligible.
Jobs that are already running can produce alerts if they fail later.
Jobs that end during the saved second are excluded.

Keep the state directory when you upgrade.
If you delete it, the watcher loses pending alerts and its record of previous deliveries.
It also sets a new initial start time.

### 4. Schedule checks

Use one login node and one scheduler for each user and cluster.
Select one of the following methods, as permitted by your site.

For cron, open `crontab -e` and add these lines.
Replace `/home/USER` with your home directory:

```cron
MAILTO=""
* * * * * /home/USER/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher >> /home/USER/.slurm-failures/watcher.log 2>&1
```

Keep `watcher.log` private and rotate it periodically.

For systemd, copy the example units and enable the timer:

```sh
mkdir -p ~/.config/systemd/user
cp examples/slurm-failure-watcher.service examples/slurm-failure-watcher.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now slurm-failure-watcher.timer
systemctl --user list-timers slurm-failure-watcher.timer
journalctl --user -u slurm-failure-watcher.service
loginctl show-user "$USER" -p Linger
```

User services can stop when you log out.
The `Linger` setting controls whether user services can continue without a login session.
Use `loginctl enable-linger` only if your site permits it.
Confirm that your selected scheduler can contact Slurm and continues after logout and reboot.

### 5. Test the installation

After initialization, submit a small test job that fails.
The [example test job](examples/smoke-test-failure.sbatch) exits with code 42:

```sh
sbatch examples/smoke-test-failure.sbatch
```

Wait for the next checks. Confirm that Slack receives one failure summary.
Check `--status` and the local report under `~/.slurm-failures/reports/`.
If you use job arrays, repeat the test with your site's array output pattern.

## Alerts and automatic cancellation

The watcher checks `BOOT_FAIL`, `CANCELLED`, `DEADLINE`, `FAILED`, `NODE_FAIL`, `OUT_OF_MEMORY`, and `TIMEOUT`.
It processes job allocations, rather than individual `.batch` or `.extern` steps.
It does not alert on preempted jobs or failed steps within a `COMPLETED` allocation.

Ordinary cancellations do not produce Slack alerts.
A cancelled job needs explicit watchdog evidence in its logs to produce an alert.
A watchdog is a job-script check that cancels a job after a period without log output.

Every recorded failure starts cancellation of its full `afterok` dependency chain.
This includes ordinary cancellations, even when Slack sends no alert.
Jobs connected only through other dependency types remain active:

```text
42 FAILED
 ├─ afterok:42 → 43 → afterok:43 → 44    cancel 43 and 44
 └─ afterany:42 → 45                    keep 45
```

An OR expression such as `afterok:42?afterany:99` still connects the job to job 42.
The watcher cancels that job, even if the other dependency could permit it to start.
It can also cancel a connected job that has already started.
Slurm array and heterogeneous-job rules can cause a request to cancel a larger job group.

The watcher saves its cancellation plan and checks job identities before it calls `scancel`.
It retries failed requests and checks that requested jobs disappear from the queue.
Missing logs or failed Slack delivery do not stop these retries.
These checks cannot remove every race between a queue check and a cancellation request.
Jobs that disappear before the first queue snapshot can hide further dependent jobs.
Test cancellation with small test jobs before you use it for production work.

## Reports and privacy

By default, reports and log copies stay in `~/.slurm-failures/`, with directory mode `0700`.
Each event has a report at `reports/<event SHA256>/report.json` within that directory.
The report contains Slurm metadata, failure evidence, cancellation results, and references to log copies.
Log copies preserve the original bytes and have no size limit.
Allow enough disk space. The watcher does not automatically delete old reports.

Slack receives job details, times, exit code, failure explanation, and applicable cancellation counts.
It does not receive local report paths or log excerpts.
Missing logs produce incomplete reports, which the watcher retries.
After `log_wait_seconds` (default 900), Slack delivery can proceed without complete logs.
The watcher does not continuously check completed reports for later log changes.

Fixed rules identify memory errors, node failures, filesystem errors, crashes, and time limits.
They also recognize Quokka output messages and watchdog messages.
A suspected output-write stall does not prove a blocked write or a filesystem fault.
Inspect backtraces and site monitoring data to find the cause.
The watcher does not collect separate backtrace directories.

### Optional GitHub uploads

To upload full reports and copied logs, set `upload_gist` to `true` in the installed configuration.
Install GitHub CLI and authenticate it as the watcher user with the `gist` scope.
Set `gh` to its absolute executable path.
The watcher creates an unlisted gist and adds its URL to Slack.
If gist creation fails, Slack delivery waits for a retry.

**Anyone with an unlisted gist URL can read its contents.**
Reports can contain usernames, account names, node names, file paths, and sensitive log data.
Enable uploads only if your site and data policies permit them.
The default `upload_gist: false` setting prevents GitHub requests and gist links in new Slack messages.
Disabling uploads does not delete existing gists or previous Slack messages.

## Status and recovery

Use these commands to check status and release held alerts:

```sh
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher --status
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher --retry-uncertain
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher --retry-blocked
```

A rejected Slack request becomes `blocked`. Correct the problem before you retry it.
A timeout, network failure, or interrupted send becomes `uncertain`.
Check the Slack channel before you retry an uncertain alert.
Each retry command releases all events with that status and runs a normal check.
Retries can produce duplicate messages after a lost response or an HTTP 5xx error.
Rate limits and HTTP 5xx errors cause automatic retries with increasing delays.

Monitor status, service logs, disk space, incomplete reports, and the last successful scan time.
Exit code 1 indicates a scan, storage, credential, cancellation, gist, or Slack failure during that run.
Previously held alerts and incomplete logs alone do not cause repeated failure exits.
Exit code 0 also covers a run skipped because another process holds the lock.

Store state on a persistent filesystem that supports POSIX locks, atomic renames, `fsync`, and SQLite locking.
Ask your site whether its shared filesystem supports these operations.
Back up the state directory while the watcher is stopped.

The watcher checks recent accounting records and performs a daily scan from initialization.
Your site controls accounting access and retention.
The watcher cannot recover records that Slurm deletes before discovery.
Use the [example configuration](examples/config.json) to adjust intervals and time limits within site policy.

## Tests

```sh
python3 -m unittest discover -s tests -v
python3 -m slurm_failure_watcher --help
```

Tests use temporary files and simulated Slurm, Slack, and GitHub responses.
They cannot confirm your site's accounting retention, log mounts, scheduler persistence, or network access.
Complete the installation test on your login node before you rely on alerts.

## License

[MIT License](LICENSE).
