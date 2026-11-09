# Controller operations

The controller is a deterministic Python process with a single durable SQLite database. It never imports
the simulator and uses only the documented HTTP API.

## Run

```bash
pip install -r requirements.txt
./run.sh http://127.0.0.1:8700
```

The database defaults to `state/<scenario>.sqlite3`. For an explicit path:

```bash
python -m controller run --url http://127.0.0.1:8700 --db state/test.sqlite3
python -m controller status --db state/test.sqlite3
python -m controller explain --db state/test.sqlite3 --workflow nw_renewal --at 2027-01-08T12:00:00+05:30
```

`status` reports experiments, winners, approvals, incidents, ingestion lag, throughput, credit usage and
database size. `explain` reads SQLite only and reconstructs the observations, proposed diff, governance
outcome and post-application result behind the most recent change at or before the requested time.

## Data flow

```text
HTTP history/events (NDJSON)
          |
          v
  atomic chunk transaction -----> people + population counts
          |                       hourly sufficient statistics
          |                       chunk/event cursor
          v
 experiment and incident rules
          |
          v
 queued proposal -- policy check --> auto-apply or approval API
          |                              |
          +------ audit decision <-------+
                         |
                         v
               post-change monitoring
                 retain / rollback
```

History is fully ingested before live experiments start. A chunk's aggregates and cursor commit in the
same `BEGIN IMMEDIATE` transaction. A kill before commit rolls everything back; a restart replays the whole
chunk. A committed cursor makes replay a no-op. Proposal IDs are stable for retries, and pending approvals
are reconciled from the API before any new decision is made.

## Statistical policy

- Engagement workflows use independently randomized welcome and voice allocations. Main effects are pooled
  over the other dimension; interactions are explicitly not estimated.
- Pickup workflows give 10% traffic to each untested permitted caller ID, capped at 50% total exploration.
- Winner evaluation is fixed-horizon: 300 connected calls per engagement option or 500 dials per caller ID,
  with a three-day timeout. Ordinary p-values are not used for repeated winner peeking.
- The full pre-registered option/slice family receives Holm correction. Promotion needs adjusted `p < .05`,
  at least 2 percentage points overall (3 points for a slice), and an observed guardrail inside policy.
- At most one targeted cohort is promoted per workflow. Cohorts are single-feature predicates shared by
  ingestion, experiments, configuration and audit.
- Promotion monitoring compares the first 300 eligible observations with a rolling recent 300, checking at
  each additional 100. Two supported 3-point primary drops, or one supported guardrail breach, queue a
  governance-respecting rollback.

## Incident policy

Caller IDs are checked after 300 recent dials. An ID is removed when it is at least 5 points below the best
healthy ID at one-sided `p < .01`; the action still passes normal auto-apply/approval rules. Prompt incidents
need two checks, each showing at least a 1-point QA-flag increase at one-sided `p < .005`. One LLM suggestion
call and no more than two 500-example validations are spent per incident. Passing candidates are ranked in
this order: grounded/restricted, known production-safe, strict, then free-form. The applied fix is not marked
resolved until 300 connected calls show QA recovery without more than a 2-point engagement drop.

## Failure handling

- HTTP 4xx responses are recorded. A stale base is never force-applied; non-overlapping queued work is
  rebased, while overlapping supervisor edits make the experiment stale.
- Approved changes are applied immediately during each 30-minute reconciliation cycle. An already-applied
  remote approval is recovered after restart and recorded locally.
- A workflow emits at most one proposal per simulated day. Rejected, expired or lapsed experiment proposals
  return to draft, wait for the batching boundary, and are re-proposed only after rechecking the current
  configuration.
- SQLite uses WAL, foreign keys, a 60-second busy timeout and no raw-call table. Only join features and
  sufficient statistics are retained.
- Use `scripts/crash_recovery.sh` against a fresh simulator run for the kill/restart acceptance test.

Page on: sustained non-zero event lag, repeated database write failures, a statistically supported active
guardrail breach that cannot enter governance, or a lapsed approved safety rollback. Do not page on an
underpowered experiment, a normal no-winner result, a pending request still within approver latency, or a
single prompt anomaly check.
