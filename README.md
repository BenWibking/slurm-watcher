# Slurm failure watcher

A per-user Python 3.11+ watcher that runs on a login node, outside Slurm jobs.
It requires ordinary access to `sacct`, `squeue`, `scancel`, readable job logs, and
outbound HTTPS to Slack. It automatically cancels downstream `afterok` chains
when a watched job fails. No administrator privileges, Slurm hooks, or scheduler configuration changes
are needed. Runtime dependencies are Python's standard library.

Each invocation polls once and exits. Schedule it with **one** of cron or a user
systemd timer, according to site policy. Do not submit the watcher with `sbatch`.

## Install and configure

On the login node, from this checkout:

Check `python3 --version` first and select a Python 3.11+ executable if your
site's default `python3` is older.

```sh
python3 -m venv ~/.local/venvs/slurm-failure-watcher
~/.local/venvs/slurm-failure-watcher/bin/python -m pip install .
install -d -m 700 ~/.config/slurm-failure-watcher ~/.slurm-failures
install -m 600 examples/config.json ~/.config/slurm-failure-watcher/config.json
command -v sacct
command -v squeue
command -v scancel
command -v gh
```

Edit the installed configuration's `sacct`, `squeue`, and `scancel` values to the
absolute executable paths on your site. If Slurm requires modules or special environment variables, arrange
those in a private wrapper script and point the scheduler at that wrapper. Use an
explicit Python path; cron and systemd do not load your interactive shell setup.

Authenticate GitHub CLI as the watcher user and grant its token the `gist` scope.
Set `gh` in the configuration to the absolute executable path. Before each Slack
delivery, the watcher creates one secret gist containing `report.json` and its
referenced log snapshots, then includes the gist URL in Slack. GitHub secret gists
are unlisted, not access-controlled: anyone with the URL can read them. Use this
only when uploading the report and logs complies with site and data policy.

Create a Slack app with an incoming webhook for the intended channel using
[Slack's incoming webhook instructions](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/).
Store the URL on a single line in
`~/.config/slurm-failure-watcher/webhook` using a trusted editor, **outside this
repository**. Set that file to mode `0600` and its directory to `0700`. Do not put
the URL in shell arguments, shell history, configuration JSON, unit files, or git.
The watcher rejects symlink credentials, group/world-readable credentials, and
URLs outside Slack's HTTPS webhook hosts. Never use a real webhook in tests.
Slack URL/response bodies are not printed in service logs or persisted in SQLite.

Run once to establish the baseline, then inspect status:

```sh
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher
~/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher --status
```

Initialization durably records the current UTC second before querying accounting.
Only jobs with `End` strictly after that second are eligible. Earlier jobs are
ignored even if they appear in accounting later; jobs ending during that same
second are deliberately ignored too. Jobs already running at initialization are
included if they subsequently fail. Initialization persists even when the first
accounting query fails. Preserve the state database across upgrades: deleting it
establishes a new baseline and loses pending deliveries and deduplication state.

## Scheduling

Use one login node and one scheduler. The process lock also makes overlapping
invocations on the same supported filesystem harmless. Do not run separate state
directories for the same user/cluster unless duplicate Slack posts are intended.

### cron

If your site permits login-node cron, add this using `crontab -e`. Replace
`/home/USER` with your actual home path:

```cron
MAILTO=""
* * * * * /home/USER/.local/venvs/slurm-failure-watcher/bin/slurm-failure-watcher >> /home/USER/.slurm-failures/watcher.log 2>&1
```

Keep the log private and rotate it periodically. A cron daemon runs independently
of interactive sessions. Check that the site's cron environment can contact
Slurm accounting and that cron survives login-node maintenance.

### systemd --user

If permitted, copy [the service](examples/slurm-failure-watcher.service) and
[the timer](examples/slurm-failure-watcher.timer) into `~/.config/systemd/user/`:

```sh
systemctl --user daemon-reload
systemctl --user enable --now slurm-failure-watcher.timer
systemctl --user list-timers slurm-failure-watcher.timer
journalctl --user -u slurm-failure-watcher.service
```

The service has no dependency on any Slurm job. User services may stop at logout.
`loginctl show-user "$USER" -p Linger` reports whether lingering is enabled. Use
`loginctl enable-linger` only if site policy allows it without administrator
intervention; otherwise use the site's approved persistent user service or cron.
Do not assume a user timer will survive logout or reboot without this check.

## Accounting semantics and compatibility

Implementation was checked against the upstream
[sacct reference](https://slurm.schedmd.com/sacct.html) and
[sbatch filename patterns](https://slurm.schedmd.com/sbatch.html), rather than
assuming default time windows or that stored output paths are expanded.

The query uses `--local --allocations --array --duplicates --noheader --parsable2`,
an explicit `--user`, explicit UTC start/end times, and
`--state=BOOT_FAIL,CANCELLED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,TIMEOUT`. Allocation
records are notified once; `.batch`, `.extern`, and other steps are excluded.
A COMPLETED allocation with a failed step/DerivedExitCode is outside this state
policy. Preempted jobs are also outside it. CANCELLED records are queried because
the Quokka job-script watchdog uses `scancel`; their Slack alerts require explicit
watchdog evidence. Ordinary cancellations never produce Slack notifications.

Records use ASCII unit separator rather than `|` because job names and paths may
contain pipes. Explicit wide fields prevent ordinary truncation, including the
long OOM state name. Malformed/truncated records, unexpanded array ranges, or a
failed command reject the whole scan without advancing its cursor. Newline/unit
separator metadata is not supported by this text interface. Slurm's text output
padding is stripped; paths with leading/trailing whitespace are not supported.
There is no silent fallback that drops unsupported fields. Check `sacct --help`
and `sacct --helpformat` on older installations: this watcher requires the listed
options and the fields in `slurm_failure_watcher/core.py:FIELDS`, including
`StdOut`, `StdErr`, `WorkDir`, `DBIndex`, and `Restarts`. Job accounting visibility
and retention are controlled by the site; this tool cannot enable them.

Every successful scan advances a durable cursor. Ordinary scans start at the
previous successful cursor minus `overlap_seconds` (default one day), so downtime
does not discard the gap. Once per `deep_scan_seconds` (default one day), the
watcher rescans from initialization to catch arbitrarily late accounting updates
still retained by Slurm. It updates metadata for known entries and retries reports
when metadata changes, without posting Slack again. A missing/unknown End is
reconsidered on later scans. Delays beyond overlap can take until the next deep
scan to discover. Data purged before discovery cannot be recovered.

Identity is cluster + DBIndex, which Slurm documents as the unique job-table
index. If DBIndex is unavailable/zero, identity falls back to cluster + JobIDRaw +
Submit, covering recycled numeric IDs. Array tasks are separate records; human
array IDs supply `%A/%a` while JobIDRaw supplies `%j`. Heterogeneous components
retain their separate raw identities. Distinct database records from requeues
are separate events. If a site overwrites the same database record for multiple
runs, this watcher updates that event instead of promising one alert per attempt.
It does not require newer SLUID fields.

## Automatic afterok chain cancellation

Every newly recorded failed job triggers cancellation of its entire transitive
`afterok` chain, regardless of the classification or failure cause. This includes
CANCELLED roots (ordinary cancellations as well as watchdog cancellations),
because their `afterok` dependents cannot succeed. Ordinary cancellations still
do not produce Slack failure alerts. Historical jobs excluded by initialization
do not become cancellation roots. Upgrading an existing state database enables
the policy for failures already recorded by that installation too.

The implementation follows the snapshot and breadth-first traversal approach in
`~/example-workflows/inputs/scripts/cancel-slurm-chain.sh`, with durable plans
and revalidation. Unlike that interactive utility, this watcher automatically
follows **only `afterok` edges**, preserving jobs attached solely through
`afterany`, `afternotok`, `aftercorr`, or other dependency types. For example:

```text
42 FAILED
 ├─ afterok:42 → 43 → afterok:43 → 44    cancel 43 and 44
 └─ afterany:42 → 45                    preserve 45
```

Branching chains and multiple dependency IDs are supported. For an OR expression
such as `afterok:42?afterany:99`, the job is still structurally downstream of 42
and is cancelled under this policy, even if another branch could release it.
The policy applies to active jobs returned by squeue, including a descendant
that has already started through an alternative dependency. The already-terminal
failure root itself is not passed to scancel.

Cancellation runs before log discovery, classification, report copying, or Slack.
A missing log, invalid webhook, report-storage failure, or failed accounting poll
does not prevent retrying an already recorded cancellation plan. SQLite/state
storage itself must be available before any external cancellation is attempted.

For each cluster with work due, the watcher:

1. Takes one current-user queue snapshot using `squeue --clusters=CLUSTER --local
   --noheader --array --user=USER --format='%i|%A|%V|%u|%E'`.
2. Computes the full chain, then commits every target's display ID, raw ID,
   submission timestamp, and dependency expression to SQLite **before** cancellation.
3. Takes a fresh snapshot and checks each target's identity and dependencies.
   Missing targets are recorded as `gone`, changed identities as `replaced`, and
   edited dependencies as `detached`, without cancelling those entries.
4. Requests cancellation in batches of at most 100 explicit IDs using
   `scancel --clusters=CLUSTER --quiet --user=USER ...`. Acknowledged requests are
   `requested`, not claimed to be finished. A later snapshot must show the target
   gone. Failed batches retain their plans and retry with capped backoff.

Saved members remain available as ancestors after disappearing from the queue,
so a deeper child can still be found if its cancelled parent has vanished.
Lost scancel responses and crashes are reconciled on the next invocation;
repeating cancellation of the same still-visible job is intentional. Overlapping
roots share one request per target per snapshot. Root/target submission times and
raw IDs guard against common job-ID reuse and requeue races. Inherited SQUEUE_*
and SCANCEL_* filters/signals are removed; ownership and cluster are explicit.

Array tasks are expanded. A failed task matches dependencies on that exact task,
its raw ID, or the aggregate array ID (`afterok` on an entire array needs every
task to succeed); it does not match an unrelated sibling-task dependency. A whole
array root covers element dependencies. Heterogeneous offsets are treated as one
allocation group, as in the example utility: cancellation uses the group leader
because pending heterogeneous components cannot be cancelled independently.
Slurm also has special cancellation behavior for unsplit array elements. Consult
the [scancel array/heterogeneous semantics](https://slurm.schedmd.com/scancel.html)
when setting up such dependencies; an element request can cancel the whole
unsplit array. This does not change Slurm's dependency or cancellation configuration.

`chain_scan_seconds` (default 60) sets the interval between reconciliation attempts.
`chain_watch_seconds` (default 86400) keeps discovering newly visible/added
descendants for one day after each failure is first discovered. Every descendant
already in the initial snapshot is planned immediately, irrespective of its
scheduled start time. Increase this interval for workflows that add chain members
later. Outstanding known targets continue retrying beyond the discovery window
until absent, replaced, or detached; the root then becomes `complete`. Roots with
failed discovery keep retrying until a successful reconciliation. These are
additional queue/controller calls to budget under site policy, normally one
snapshot per due cluster plus one revalidation snapshot if targets exist.

`--status` includes chain and target counts. Reports include an `afterok_chain`
audit with per-target status, submission time, attempts, and errors; Slack
summaries include cancellation counts. Chain state changes refresh reports but
do not resend an already delivered Slack notification. A cancellation failure
makes the invocation exit nonzero while other delivery work continues.

Dependencies and IDs can change between the final check and scancel: Slurm's
CLI has no atomic "cancel this job only if submission time and dependency still
match" operation. Revalidation and explicit ownership reduce this race, but cannot
eliminate it. Nodes that have already disappeared from squeue **before the first
snapshot**, and whose IDs were never captured, can hide deeper descendants;
squeue only reports remaining dependencies. The watcher does not invent missing
graph edges from similar names. Check its audit if a site automatically removes
dependency-failed jobs immediately. Upstream references:
[squeue dependency output](https://slurm.schedmd.com/squeue.html) and
[sbatch dependency expressions](https://slurm.schedmd.com/sbatch.html).

## Deterministic failure classification

Every processed event gets a versioned `classification` in SQLite and its report.
Slack includes the category, confidence, rule ID, and a bounded evidence line.
Rules use accounting state and full available logs, with bounded memory and
evidence excerpts; they do not use an LLM or depend only on Slack's tail excerpt.
Evidence includes source paths and original line numbers. Coverage records show
bytes scanned, unreadable/changing files, and skipped lines longer than 64 KiB.
Incomplete/unsettled log discovery marks the classification `provisional`.
Delayed reports are reclassified as logs become available; an already-sent Slack
post is not sent again when the diagnosis changes.

The built-in `quokka-v1` rules were checked against the local Quokka source
(`src/simulation.hpp`, `src/io/DiagPlotfile.H`) and the job scripts in
`~/example-workflows/inputs`, including
`example/run-a.submit` and `example/run-b.submit`.
These source repositories are **not runtime dependencies**. In those scripts,
`WATCHDOG: No log writes for 900s. Collecting backtraces.` precedes backtrace
collection and `scancel`. Both stdout and stderr use `logs/%x_%j.out`. Quokka
prints `Writing plotfile <path>` and `Writing checkpoint <path>` without a general
matching completion message. A missing completion message alone therefore cannot
establish a hung write.

Rules have this explicit priority:

| Evidence | Classification |
| --- | --- |
| Slurm OOM, NODE_FAIL, BOOT_FAIL, DEADLINE | `out_of_memory`, `node_failure`, `boot_failure`, `deadline`; accounting evidence takes priority. |
| Explicit OOM signature, such as Slurm oom_kill | `out_of_memory` |
| Explicit disk-full, quota, I/O error, stale handle, read-only filesystem | `filesystem_error` |
| Explicit assertion, AMReX abort, SIGSEGV, SIGFPE | `application_crash` |
| Terminal Quokka write phase + explicit no-output watchdog | `suspected_output_write_stall`, probable; rule `watchdog-terminal-write` |
| Terminal Quokka write phase + TIMEOUT or Slurm time-limit message | `suspected_output_write_stall`, possible; rule `timeout-terminal-write` |
| Watchdog with no terminal write phase, including log never created | `watchdog_no_output` |
| TIMEOUT without the above evidence | `timeout` |
| Other FAILED allocation | `application_failure` |

A terminal write phase requires a recognized write-start line with no later
completion, coarse-step progress, or unrelated application output in that stream.
A later checkpoint supersedes an earlier plotfile phase. VisMF sub-write messages
do not finish an entire plotfile/checkpoint. Slurm termination and watchdog
messages do not count as application progress. No ordering is invented between
separate stdout/stderr files; conflicting completion evidence in another stream
suppresses the stall rule. A changed/unreadable scan or oversized skipped line
also prevents the terminal-write inference. Empty logs yield a generic category.

For example:

```text
Coarse STEP 42 at t = ... starts ...
Writing plotfile plt042
Writing checkpoint chk042
WATCHDOG: No log writes for 900s. Collecting backtraces.
WATCHDOG: Collecting backtraces into logs/watchdog_backtraces_12345_20000101_120000
```

With a CANCELLED, FAILED, or TIMEOUT record, this is a **probable suspected
checkpoint write stall**, with the checkpoint path and watchdog idle interval
recorded as evidence. If `Coarse STEP 43 ...` follows the write, it is not
classified as a write stall. If `No space left on device` appears, the explicit
filesystem error takes priority. A plain TIMEOUT is never automatically called
an I/O hang.

This identifies the last observed application phase, not a proven blocked write
syscall or a proven Lustre fault. MPI synchronization, asynchronous writes,
buffered/missing logs, or compute within output preparation can give similar
symptoms. The supplied scripts collect backtraces in a separate directory: inspect
those and site I/O telemetry to establish the exact blocking call. The watcher
does not automatically copy or parse those extra files. It does not treat the
existence of a plotfile/checkpoint directory or Header as proof of completion.

Recognized new-run boundaries (`Initializing AMReX (...)` and the scripts'
`Broadcasting container image to all compute nodes...`) discard earlier evidence
in an appended stream, avoiding stale errors/watchdogs from previous attempts.
Without such a boundary, concatenated logs cannot be reliably separated; use
per-attempt log filenames when practical. Unrecognized application formats receive
generic categories rather than speculative diagnoses.

Ordinary CANCELLED records receive local reports but wait for watchdog evidence
before Slack delivery. After the log grace period, a settled log without watchdog
evidence is marked `ignored`. Missing logs remain pending and can be reconsidered
indefinitely; a late watchdog found during report retries can release suppression.
As with other finalized snapshots, later appends to a settled, ignored log are not
watched continuously. Schema v1/v2 state is migrated automatically to v3, preserving
initialization and Slack history and adding durable chain cancellation tracking.

One accounting RPC per invocation avoids per-job controller queries. Deep scans
grow with the lifetime of the installation; choose a site-approved polling/deep
scan interval and monitor accounting costs. A finite history window would trade
that cost for a hard bound on tolerated accounting delay.

## Reports and log discovery

State defaults to `~/.slurm-failures/` (0700). Each event has
`reports/<event SHA256>/report.json`: full accounting metadata, original filename
patterns, resolved source paths, discovery notes, copy errors, UTC snapshot time,
completeness, and SHA256/byte counts of `.txt` log copies. Source bytes are
preserved without transcoding; full reports have no log-size cap. Identical stdout and
stderr paths are copied once. Files are copied in chunks; large logs require
enough disk space and can lengthen a run. Copies are content-addressed and
fsynced before the manifest is atomically replaced, so an interrupted refresh
cannot overwrite files referenced by the previous manifest. SQLite completion
is committed only after the manifest is durable. Retries can leave old snapshots
or temporary files; no automatic deletion or retention policy is imposed.
If a later refresh cannot read a previously captured source, its last copy stays
in the manifest with `current: false` and its original snapshot time.

Relative paths are resolved against accounting WorkDir. Supported substitutions:
`%%`, `%j`, `%J` (allocation ID), `%A`, `%a`, `%b`, `%u`, `%x`, `%r`, numeric padding
up to Slurm's ten-digit maximum, and the backslash suppression rule. `%N`, `%n`,
and `%t` discover matching per-node/rank files with literal path components
escaped; no shell or environment expansion occurs. NNodes/NTasks provide minimum
expected counts. A missing count makes completeness unknown and keeps retrying.
Patterns combining node and task fan-out axes have unknown coverage and keep
retrying; a minimum count cannot prove each rank's node placement. Unsupported
tokens (including `%s` and newer `%S`) are explicitly recorded and retried;
the watcher never guesses a path. Prefer `%j` or `%A_%a` for batch logs.
Using sacct `--expand-patterns` would silently choose only the first fan-out
value, so it is deliberately not used.

Only regular files visible on the login node can be captured. Node-local files,
deleted logs, inaccessible directories, and files never created after boot/launch
failure remain partial. `/dev/null` is recorded as intentionally discarded. Blank
accounting paths are treated as unknown, without guessing default filenames.
Files need the same size/mtime/inode signature on observations separated by
`settle_seconds` (default 60) to finish reporting. A file changing during copying
is retried. This is a settling heuristic, not a filesystem flush guarantee.
Logs appended after finalization are not watched continuously; metadata or chain
state changes can trigger a refresh. Preserve source logs until reports complete.

Missing or unsettled logs produce partial reports and retry indefinitely with
capped exponential backoff. Slack waits up to `log_wait_seconds` after discovery
(default 15 minutes), then sends available excerpts with an explicit partial
warning. Report attempts continue independently. Slack messages contain the job
identity, partition, Slurm start/end/elapsed timestamps, failure category and
explanation, exit code, and secret gist link. They omit local report paths and
inline log excerpts; the gist contains the full report and copied logs. Slack
mention/link control syntax is escaped before delivery.
Reports retain original bytes.
The full report and copied logs are also uploaded to an unlisted GitHub secret
gist. A deterministic event description is checked before creation, allowing a
retry to recover a gist created just before a timeout or process interruption.
The persisted gist URL prevents later Slack retries from creating duplicates.

## Delivery durability and operational checks

SQLite tracks independent report completion, report retry times/errors, Slack
status, Slack retry times/attempts, and accounting cursors. A nonblocking advisory
`flock` covers the entire invocation, including copying and network I/O. Short
SQLite transactions use FULL synchronous durability. All instances must share
the same state directory. Use storage with working POSIX locks, atomic renames,
fsync, and SQLite locking; ask your site about home filesystem support. NFS and
distributed filesystems vary. Do not run a shared database on a filesystem that
does not provide these semantics. Prefer a durable supported filesystem; node
temporary storage loses state on reboot. Back up the entire state directory
while the watcher is stopped (or use SQLite's backup API for the database).

Slack handling follows its
[documented errors](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/#handling_errors)
and [rate limits](https://docs.slack.dev/apis/web-api/rate-limits/):

| Status | Behavior |
| --- | --- |
| HTTP 200 and `ok` | Mark sent; no further automatic posts for that event. |
| HTTP 429 | Retry after at least Retry-After and backoff; persist a global webhook cooldown. |
| HTTP 5xx | Retry with capped exponential backoff. |
| Other HTTP rejection, including redirects | Mark blocked; require correction and explicit retry. Credentials are never forwarded to a redirect. |
| Timeout, network failure, unexpected success body, or crash with send in progress | Mark uncertain; hold for human reconciliation. |

At most one Slack request is made per invocation; successive requests are gated
by at least one second. With minute polling, large failure bursts drain at about
one message per minute. `max_deliveries` bounds per-run event processing (default
100); retries and unsent events remain durable. Adjust frequency only within site
accounting policy. There are no in-process retry loops that block later polls.

Incoming webhooks do not provide a transactional idempotency key or a way to
query delivery by event ID. If Slack accepts a message but its response is lost,
or the process dies before committing success, exactly-once delivery is
impossible. An event prefix appears in each message for reconciliation. Search
the channel for that prefix and inspect `--status` before deliberately retrying:

```sh
slurm-failure-watcher --status
slurm-failure-watcher --retry-uncertain
slurm-failure-watcher --retry-blocked
```

Retry flags release **all** events with that status and perform a normal poll;
uncertain retries may duplicate an already delivered message. HTTP 5xx retries
also have a residual ambiguity if a server/proxy reports failure after accepting
a message. This policy favors avoiding duplicates for transport/crash ambiguity;
it does not promise every held event reaches Slack without intervention.

For individual event inspection, open SQLite read-only (while no run is active):

```sh
sqlite3 -readonly ~/.slurm-failures/state.sqlite3 \
  'SELECT id,report_done,report_attempts,report_error,slack_status,slack_attempts FROM jobs;'
```

Monitor `--status`, service logs, storage quota, time since last successful scan,
partial reports, and held deliveries. Exit code 1 indicates scan failure, report
storage failure, credential failure, or a failed Slack attempt in that invocation.
It also indicates failed dependency-chain discovery or cancellation attempts.
Partial logs and already-held Slack events alone do not cause repeated nonzero
exits; status inspection is necessary. Exit 0 also covers skipping a concurrent
invocation. SQLite cannot make an unavailable shared filesystem or an indefinitely
blocked filesystem operation reliable. Scheduler service timeouts should leave
enough time for large log copies; interrupted work resumes on later invocations.

## Tests and deployment validation

```sh
python3 -m unittest discover -s tests -v
python3 -m slurm_failure_watcher --help
```

Tests use temporary state and mock sacct and Slack; no live scheduler or webhook
is needed. They exercise parsing/options, every requested failure state, baseline
filtering, arrays, patterns, full binary copies, delayed logs/accounting, separate
retries, crashes, concurrency, response classification, and credential checks.
Classifier tests include the supplied watchdog wording, both output types, later
progress/completion, explicit competing errors, ordinary cancellations, appended
attempt boundaries, large logs, and persisted report/Slack diagnoses.
Chain tests cover typed/annotated dependencies, AND/OR clauses, branching and
cycles, array/group matching, current-user command syntax, stale identities,
dependency edits, full-plan durability, crashes, late descendants, batching,
independent retries, and queue-disappearance confirmation.

Before relying on the installation, run it on the actual login node, confirm
status advances, then submit a small permitted test job that exits nonzero after
initialization. Check its complete local report and single Slack notification.
Repeat with your site's array output pattern. This repository's mocked tests
cannot verify site accounting retention, log mount visibility, cron availability,
systemd lingering, or Slack egress. No live cluster execution is claimed.
For a deployment test of cancellation, submit a small failing root and two
`afterok` dependents, plus an `afterany` cleanup job. Confirm the two afterok jobs
are cancelled, the cleanup job is preserved, and the report's audit matches the
queue. Do this in a permitted test allocation, not an existing production chain.
