"""Command-line entry point for the outcome controller."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .api import SimulatorError
from .db import Store, sim_minute
from .engine import Controller


def _json(value) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _at_minute(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return sim_minute(value)


def run_command(args: argparse.Namespace) -> int:
    controller = Controller(args.url, args.db, args.advance_minutes)
    controller.run()
    return 0


def status_command(args: argparse.Namespace) -> int:
    store = Store(args.db)
    try:
        status = store.status_summary()
    finally:
        store.close()
    if args.json:
        print(_json(status))
        return 0
    print(f"Scenario: {status['scenario']}  minute={status['last_sim_minute']}  "
          f"finished={status['finished']}")
    print(f"Ingestion: {status['events_per_second']} events/s  lag={status['ingestion_lag_minutes']} min  "
          f"database={status['database_bytes']} bytes")
    print(f"Operations: {status['decisions_per_hour']} decisions/hour  "
          f"credits used={status['credits_used']} remaining={status['credits_remaining']}")
    for label, key in (("Active experiments", "active_experiments"),
                       ("Current winners", "current_winners"),
                       ("Pending proposals", "pending_proposals"),
                       ("Open incidents", "open_incidents"),
                       ("Changes today", "changes_today")):
        print(f"\n{label}:")
        rows = status[key]
        print(_json(rows) if rows else "  none")
    return 0


def explain_command(args: argparse.Namespace) -> int:
    store = Store(args.db)
    try:
        at = _at_minute(args.at)
        decisions = list(reversed(store.decisions(args.workflow, at, limit=500)))
        applied = [d for d in decisions if d["kind"] == "change_applied"]
        selected = applied[-1] if applied else None
        proposal = None
        if selected:
            proposal_id = selected["action"].get("proposal_id")
            proposal = next((p for p in store.proposals() if p["proposal_id"] == proposal_id), None)
        experiment = None
        if proposal and proposal.get("experiment_id"):
            experiment = next((e for e in store.experiments()
                               if e["experiment_id"] == proposal["experiment_id"]), None)
        related_incidents = [i for i in store.incidents() if i["workflow_id"] == args.workflow]
        output = {
            "workflow_id": args.workflow,
            "as_of_minute": at if at is not None else store.get_meta("last_sim_minute"),
            "latest_change": selected,
            "supporting_observations": (proposal or {}).get("body", {}).get("dry_run"),
            "configuration_diff": (proposal or {}).get("body", {}).get("diff"),
            "governance": None if not proposal else {
                "proposal_id": proposal["proposal_id"], "approval_id": proposal.get("approval_id"),
                "status": proposal["status"], "applied_version": proposal.get("applied_version"),
                "reasons": proposal.get("reasons", []),
            },
            "experiment_outcome": None if not experiment else {
                "experiment_id": experiment["experiment_id"], "state": experiment["state"],
                "result": experiment.get("result"), "monitoring": experiment.get("details"),
            },
            "incident_outcomes": related_incidents,
            "audit_timeline": decisions,
        }
        print(_json(output))
    finally:
        store.close()
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="python -m controller",
                                   description="Durable controller for the calling-workflow simulator")
    commands = root.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="run a scenario until completion")
    run.add_argument("--url", required=True, help="simulator base URL")
    run.add_argument("--db", type=Path, help="SQLite path (default: state/<scenario>.sqlite3)")
    run.add_argument("--advance-minutes", type=int, default=30,
                     help="simulation step; must be a multiple of the simulator tick")
    run.set_defaults(func=run_command)

    status = commands.add_parser("status", help="show controller health from SQLite")
    status.add_argument("--db", type=Path, required=True)
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=status_command)

    explain = commands.add_parser("explain", help="reconstruct why a workflow changed")
    explain.add_argument("--db", type=Path, required=True)
    explain.add_argument("--workflow", required=True)
    explain.add_argument("--at", help="ISO timestamp or simulation minute")
    explain.set_defaults(func=explain_command)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        return int(args.func(args))
    except (SimulatorError, OSError, ValueError, KeyError) as exc:
        print(f"controller: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
