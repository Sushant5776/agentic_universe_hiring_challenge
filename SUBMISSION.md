# Outcome-First Agent Loop — Submission

**Name:** Sushant Garudkar  · **Date:** 2026-11-09  · **Hours:** time-boxed MVP  · **Language / storage:** Python 3, HTTPX, SQLite/WAL

## 0. TL;DR

The controller ingests simulator history and live NDJSON atomically, runs fixed-horizon factorial and caller-ID experiments, detects incidents, routes changes through policy-aware approvals, and records every decision in SQLite. Public 01 and 02 finish unattended; public 02 detects the bad caller ID and prompt, validates and applies a grounded fix, and rolls back a decayed winner. I deliberately cut per-person call-time optimization, a web UI, cross-tenant credit fairness, interaction estimation, and distributed infrastructure.

## 1. Assumptions

1. `engaged=true` is the simulator's conclusion event, so conclusion rate is computed from engaged calls and uploaded leads.
2. A 30-minute controller tick is fresh enough for the simulator and keeps approval/application behavior deterministic.
3. The simulator's event stream is the source of truth; the controller never reads `sim/`, scenario YAML, hidden truth, or run artifacts.
4. A lower-confidence-bound dry-run interval is reported, but the configured guardrail decision uses its observed point estimate; the interval is visible to a reviewer.
5. A missing feature join is a data-integrity error. The chunk is rolled back and replayed rather than dropping the call.

## 2. Scope decisions

| # | Capability | Built / Partial / Cut | Why | What it would take to finish |
|---|---|---|---|---|
| C1 | Durable ingestion | Built | Chunk cursors, replay-safe transactions, WAL | Partition/parallel ingestion for multi-host deployment |
| C2 | Shared cohorts and joins | Built | One predicate representation is used in stats and config | A richer cohort DSL |
| C3 | History bootstrap | Built | History is ingested before any experiment starts | Backfill compaction/retention |
| C4 | Call-time optimization | Cut | Explicitly outside the time-box | Contextual policy with propensities and safety constraints |
| C5 | Caller-ID health | Built | Exploration, degradation test, removal and verification | Tenant-wide DID inventory service |
| C6 | Welcome/voice experiments | Built | Independent factorial main effects, Holm correction | Interaction model and sequential design |
| C7 | Promotion/decay rollback | Built | Fixed reference/recent windows and governance rollback | More powerful drift detector |
| C8 | Hallucination response | Built | Two checks, one LLM call, two n=500 validations, post-apply verification | Calibrated QA classifier |
| C9 | Governance client | Built | Stable IDs, timing, fatigue batching, stale-base handling | Durable remote approval event subscriptions |
| C10 | Impact/dry run | Partial | Honest sufficient-statistics dry run and score | Model-based counterfactual simulation |
| C11 | Supervisor view | Partial | `status` and `explain` CLI instead of web UI | Authenticated web/API surface |
| C12 | Audit store | Built | Decisions, evidence, diffs, approvals and outcomes in SQLite | Long-term archival policy |
| C13 | Crash recovery | Built | Atomic chunks and idempotent governance recovery | Multi-process lease/leader election |
| C14 | Observability | Built | Lag, throughput, decisions/hour, credits and DB size | Metrics export and alert routing |
| C15 | Tenant-fair credits | Cut | One local controller and finite simulator budget | Global scheduler and quota policy |

The principle was to make evidence, governance and recovery correct before adding model complexity.

## 3. Stakeholders

### 01 Sales

Accepted the need for a clear client-level outcome report. Pushed back on “winner by lunch,” blended KPIs, persuasive-prompt rollout, and bypassing Northwind approvals: the simulator policy is authoritative and pickup and engagement remain separate. Reply: “We can show the lift and the affected population, but customer-facing changes still require the configured approval path. We will not roll out a persuasive prompt merely because a demo liked it; the controller validates a candidate and records the evidence. The public reports show both gains and harmful/rolled-back changes honestly.”

### 02 Client compliance

Accepted named approvals for customer-facing dimensions, prompt validation evidence, auditability and approver-hour constraints. Reply: “Every applied change has a proposal ID, base version, diff, evidence, approval and post-apply result in SQLite. Prompt changes carry simulator validation evidence. We submit in your configured window and never overwrite a supervisor edit; a stale base is rebased or stopped.”

### 03 Platform/SRE

Accepted no new stateful service, kill/restart testing and actionable health. SQLite WAL plus chunk/proposal idempotency meets the MVP. Reply: “The controller is a single Python process with a local SQLite WAL. A killed transaction rolls back and the chunk replays; duplicate proposal IDs recover from the remote config. `status` reports lag, throughput, credit burn and database size. The load run processed a simulated day well under ten minutes.”

### 04 Operations supervisor

Accepted no silent supervisor overwrite, one batched daily request, Kestrel's 11:00 floor, and a consolidated CLI view. Rejected strict/free-form fixes when a grounded candidate passes. Reply: “Your prompt edit remains the base unless evidence shows a separate incident. We send one deterministic proposal per workflow/day, within your hours, with the diff and rollback rule. In public_02 the grounded prompt passed validation and recovered QA without the engagement guardrail breach.”

### 05 Product

Accepted separate pickup/engagement metrics, shared cohort IDs, proposal fields and workflow-local promotion. Partial: the supervisor surface is CLI, and per-person call-time is cut. Reply: “The MVP chooses 300 connected calls per engagement arm, 500 dials per caller ID, three-day horizons and Holm correction. It makes no change when evidence is underpowered, and it does not transfer a winner between workflows.”

### 06 CTO

Accepted outcome-first evidence, conservative credit use, scale measurement and explicit cuts. Reply: “The loop earns autonomy through deterministic statistics and governance, not agent count. Public_02 used one suggestion call and two validations, closed the incident, and rolled back decay. The next production step is a partitioned event/approval service after measuring the SQLite envelope.”

## 4. Architecture

```text
HTTP /sim/state, /workflows, /events/chunks, /approvals
                         |
                         v
        controller.api + 30-minute controller loop
                         |
        SQLite WAL (people, population, call_stats, cursors,
        cohorts, experiments, proposals, incidents, decisions)
                         |
       fixed-horizon tests / incident rules / rollback monitor
                         |
             queued proposal -> auto-apply OR approval
                         |
              config version + audit decision + monitor
```

On restart the controller reads the scenario/time marker, reuses the same DB, replays only uncommitted chunks, reconciles remote approvals before creating decisions, and recovers an already-applied proposal by comparing its stable ID/config.

## 5. The learning loop

- **Cohorts:** one feature/value predicate, one active targeted cohort per workflow; all traffic otherwise remains `default`. History registers observed language, age, city-tier and value slices. Targeted winners need 100 connected calls in a slice and 3pp lift.
- **Experiments:** engagement workflows independently randomize welcome and voice and estimate main effects by pooling over the other dimension; interactions are intentionally not estimated. Pickup workflows reserve 10% per unseen caller ID, capped at 50% exploration.
- **Winner:** a single fixed-horizon evaluation after 300 connected calls per engagement option or 500 dials per caller ID, with three simulated days as the deadline. Holm correction spans the registered option/slice family. Promotion needs adjusted p<.05, lift >=2pp overall or >=3pp in a slice, and the guardrail.
- **Decay:** the first 300 eligible observations form the reference; a rolling recent 300 is checked every additional 100. Two supported 3pp drops or one supported guardrail breach queues a rollback through governance.
- **Caller ID:** history supplies the baseline; current IDs remain safe while unseen IDs explore. An ID at least 5pp below the best healthy ID after 300 dials with one-sided p<.01 is removed or rotated according to policy.
- **Guardrail:** pickup is the guardrail for engagement objectives and engagement is the guardrail for pickup objectives. It is computed separately and included in the proposal evidence.

## 6. Governance

Proposals persist as `queued -> pending -> approved -> applied`, with rejected/expired/lapsed outcomes and fresh re-proposal after a day boundary. Stale bases are rebased only for non-overlapping supervisor edits; overlapping edits make the recommendation stale. IDs are deterministic and reused across retries. Requests are batched one per workflow/day, sent after approver start (Kestrel no earlier than 11:00), and applied only inside the simulator's policy window. Auto-apply is used only when every changed dimension and total-variation shift are permitted by the workflow policy.

`affected_leads` is the active population count from joined lead events. The dry run carries observed primary/guardrail rates, sample counts, intervals, unsupported-cell warnings and the deterministic rationale. The impact score clamps `(lower_bound_lift / .10) * affected_share * confidence` to `[0,1]`.

## 7. Incidents

Caller-ID degradation uses 300 recent dials and one-sided p<.01. Prompt incidents require two +1pp QA-flag checks at p<.005; one LLM suggestion call and at most two n=500 validation calls are used. Candidates are ranked grounded/restricted, known-safe, strict, then free-form. In public_02 the controller removed Kestrel D2, detected P2, selected grounded P3 (pass rate 0.988), applied it through approval, and verified 338 connected calls with QA falling from 3.62% to 2.07% while engagement stayed within the guardrail. The decaying W3 promotion was subsequently rolled back.

## 8. Scale

Fresh `public_03_load` run: 5 simulated days in 87.43 wall-clock seconds, 154,768 ingested events/s, zero event/minute lag, 215,445,504-byte SQLite database, and `finished=true`. One simulated day is therefore well under the ten-minute envelope on this host. At 10x, the single SQLite writer and feature cache become the first bottlenecks; partition by workflow/tenant and move approval/audit writes to a durable service before adding contextual policy learning.

## 9. Reliability

`scripts/crash_recovery.sh <SIM_URL> [DB]` kills and restarts the controller. Chunk aggregates and cursor advance are one transaction, so an interrupted chunk is replay-safe. Remote proposal IDs are stable; a crash after remote apply is recovered from current config, and pending approvals are reconciled from `/approvals`. Transient HTTP transport/408/425/429/5xx errors retry with bounded backoff; deterministic 4xx conflicts are recorded and never force-applied.

## 10. Audit & observability

```bash
python -m controller explain --db state/public_02_new2.sqlite3 \
  --workflow ks_cancel --at 2027-01-07T18:00:00+05:30
```

The SQLite-only output includes the launch and promotion evidence, proposal/config diff, approval ID/status, experiment result, incident verification, rollback and chronological decisions. `status` reports active experiments, winners, pending proposals, incidents, changes today, ingestion lag, events/s, decisions/hour, credits and DB size. Page on sustained lag, repeated DB failures, an ungovernable supported guardrail breach, or a lapsed safety rollback; do not page on one anomaly check or an underpowered experiment.

## 11. Results

See [reports/public_01.md](reports/public_01.md), [reports/public_02.md](reports/public_02.md), and [reports/public_03.md](reports/public_03.md). Public_01 improved aggregate conclusion rate by 1.7% but had two harmful config applications, motivating stricter promotion/rollback review. Public_02 improved aggregate conclusion rate by 11.6%, fixed the prompt incident and caller-ID incident, and left no open incidents. Harmful rows are retained in the reports rather than hidden behind a blended score.

## 12. Trade-offs & rejected alternatives

| Option | Why considered | Why rejected / what instead |
|---|---|---|
| Full CLUCB/contextual bandit | Better personalization | No reliable propensities or horizon in the time-box; fixed-horizon conservative experiments first |
| ADWIN/sequential peeking | Faster drift response | More implementation and inference complexity; deterministic two-window rollback |
| Postgres/Redis/Docker | Production-like deployment | Assignment is one process; SQLite WAL is easier to audit and recover |
| Web supervisor UI | Better UX | CLI status/explain covers the graded surface; UI is cut |
| Persuasive prompt rollout | Demo lift | Compliance risk; grounded candidate wins when validation supports it |

## 13. Known weaknesses

The point-estimate guardrail is conservative only through the reported interval, not a formal sequential confidence sequence. Aggregates are hourly buckets, so exact call-level windows can overshoot by one bucket. Approval state is reconciled by polling rather than a push subscription. The controller intentionally does not optimize preferred call time, learn interactions, or provide tenant-fair credit scheduling.

## 14. If this went to production at Formi next month

First, replay hidden-like scenarios with locked thresholds and add a formal calibration/guardrail review. Second, partition ingestion and audit writes by workflow while preserving idempotent event IDs. Third, add authenticated supervisor API/UI backed by the same explain query. Fourth, add alert metrics and a remote approval subscription. Only after those controls are stable would I introduce contextual policies with logged propensities and an offline doubly robust evaluation.
