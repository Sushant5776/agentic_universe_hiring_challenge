# Crash/restart evidence

Command used against a fresh public_01 simulator:

```bash
CRASH_AFTER_SECONDS=0.3 ./scripts/crash_recovery.sh \
  http://127.0.0.1:8715 state/crash_test.sqlite3
```

The first controller was killed with SIGKILL during startup/chunk work. The
restart finished the scenario. Final SQLite checks were:

```text
finished=True
ingestion_lag_events=0
history.events_processed=187379
events.events_processed=109636
pending_proposals=[]
```

No duplicate chunk rows or lost approval state were observed.
