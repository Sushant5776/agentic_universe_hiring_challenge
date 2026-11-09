"""Controller orchestration, experimentation, incidents, and governance."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import time
import uuid
from pathlib import Path

from .api import SimulatorClient, SimulatorError
from .config import (apply_dimensions, caller_id_exploration_config, canonical, changed_dims,
                     distribution, equal_experiment_config, max_allocation_shift, remove_caller_id,
                     set_winner, add_target_cohort)
from .db import Store
from .stats import Rate, difference_interval, holm_adjust, impact_score, two_proportion_pvalue

TERMINAL_EXPERIMENTS = ("no_winner", "aborted_harm", "rolled_back", "complete", "stale")
ACTIVE_EXPERIMENTS = ("draft", "awaiting_approval", "running", "evaluating", "promoted")


def _best_weighted(dist: dict[str, float]) -> str:
    return max(dist, key=lambda option: (dist[option], option))


def _safe_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)


class Controller:
    def __init__(self, url: str, db_path: str | Path | None = None, advance_minutes: int = 30):
        self.api = SimulatorClient(url)
        initial = self.api.state()
        path = Path(db_path) if db_path else Path("state") / f"{_safe_id(initial['scenario'])}.sqlite3"
        self.store = Store(path)
        self.store.prepare_run(initial)
        self.state = initial
        self.advance_minutes = advance_minutes
        self._refresh_workflows()

    def close(self) -> None:
        self.api.close()
        self.store.close()

    @property
    def now(self) -> int:
        return int(self.state["sim_minute"])

    @property
    def ts(self) -> str:
        return self.state["ts"]

    def run(self) -> None:
        try:
            self._ingest_available("history")
            self._ingest_available("events")
            self._refresh_workflows()
            self._reconcile_approvals()
            self._bootstrap_experiments()
            self._retry_expired_experiments()
            while True:
                self._ingest_available("events")
                self._refresh_workflows()
                self._reconcile_approvals()
                self._retry_expired_experiments()
                self._evaluate_experiments()
                self._detect_caller_id_degradation()
                self._detect_prompt_incidents()
                self._monitor_incidents()
                self._monitor_promotions()
                self._submit_queued()
                self._record_state()
                if self.state["finished"]:
                    break
                self.api.advance(self.advance_minutes)
                # ``/sim/advance`` intentionally returns only time/chunk data.
                # Refresh the full state so credit and completion metadata do
                # not become stale after validation or LLM calls.
                self.state = self.api.state()
            self._ingest_available("events")
            self._reconcile_approvals()
            self._record_state()
            print(json.dumps(self.store.status_summary(), indent=2))
        finally:
            self.close()

    def _record_state(self) -> None:
        self.store.set_meta("last_sim_minute", self.now)
        self.store.set_meta("finished", self.state["finished"])
        self.store.set_meta("credits_remaining", self.state.get("credits_remaining"))
        self.store.set_meta("simulator_last_event_id", self.state.get("last_event_id", 0))
        self.store.set_meta("simulator_chunks", self.state.get("chunks", 0))

    def _refresh_workflows(self) -> None:
        for spec in self.api.workflows():
            current = self.api.config(spec["workflow_id"])
            try:
                previous = self.store.workflow(spec["workflow_id"])
            except KeyError:
                previous = None
            self.store.upsert_workflow(spec, current, self.now)
            if previous and current["version"] != previous["version"]:
                self.store.add_decision(
                    self.now, self.ts, spec["workflow_id"], "config_reconciled",
                    f"Observed config version {previous['version']} -> {current['version']}",
                    {"previous_actor": previous.get("actor"), "new_actor": current.get("actor")},
                    {"changed_dims": changed_dims(previous["config"], current["config"])},
                    {"version": current["version"]},
                )

    def _ingest_available(self, kind: str) -> None:
        after = self.store.cursor(kind)["last_chunk"]
        for meta in self.api.chunks(kind, after):
            result = None
            for attempt in range(3):
                try:
                    result = self.store.ingest_chunk(kind, meta, self.api.iter_chunk(meta["chunk"]))
                    break
                except SimulatorError as exc:
                    if exc.code != "chunk_stream_interrupted" or attempt == 2:
                        raise
                    time.sleep(0.2 * (attempt + 1))
            assert result is not None
            if not result.get("skipped") and (kind == "history" or meta["chunk"] % 25 == 0):
                print(f"{self.ts} ingest {kind} chunk={meta['chunk']} events={result['events']} "
                      f"calls={result['calls']} seconds={result['seconds']:.2f}")

    # ------------------------------------------------------------------ experiments
    def _bootstrap_experiments(self) -> None:
        for workflow in self.store.workflows():
            if self.store.experiments(workflow_id=workflow["workflow_id"]):
                continue
            spec, config = workflow["spec"], workflow["config"]
            primary = spec["objective"]["primary"]
            exp_id = f"exp_{workflow['workflow_id']}_001"
            if primary == "engagement_rate":
                dimensions = tuple(dim for dim in ("welcome_message", "voice")
                                   if len(spec["options"][dim]) > 1)
                proposed = equal_experiment_config(config, spec["options"], dimensions)
                details = {
                    "dimensions": list(dimensions),
                    "baseline": {dim: _best_weighted(distribution(config, "default", dim)) for dim in dimensions},
                    "registered_slices": self.store.observed_slices(workflow["workflow_id"]),
                    "overall_min_connected": 300,
                    "slice_min_connected": 100,
                    "max_duration_minutes": 3 * 1440,
                    "stage": "launch",
                }
                kind = "factorial_main_effects"
            else:
                proposed = caller_id_exploration_config(config, spec["options"]["caller_id"])
                details = {
                    "dimensions": ["caller_id"],
                    "baseline": {"caller_id": _best_weighted(distribution(config, "default", "caller_id"))},
                    "min_dials": 500,
                    "max_duration_minutes": 3 * 1440,
                    "stage": "launch",
                }
                kind = "caller_id_pickup"
            self.store.create_experiment({
                "experiment_id": exp_id, "workflow_id": workflow["workflow_id"], "kind": kind,
                "state": "draft", "created_minute": self.now, "baseline_config": config,
                "experiment_config": proposed, "details": details, "updated_minute": self.now,
            })
            if canonical(config) == canonical(proposed):
                self.store.update_experiment(exp_id, self.now, state="running", started_minute=self.now,
                                             deadline_minute=self.now + 3 * 1440,
                                             applied_version=workflow["version"])
                continue
            evidence = {"purpose": "pre-registered exploration", "design": kind,
                        "dimensions": details["dimensions"], "fixed_horizon": True}
            pid = self._queue_change(workflow, proposed, "experiment_launch", evidence,
                                     experiment_id=exp_id, on_apply_state="running")
            if pid:
                self.store.update_experiment(exp_id, self.now, state="awaiting_approval")

    def _evaluate_experiments(self) -> None:
        active_proposals = {p["experiment_id"] for p in self.store.proposals(("queued", "pending", "approved"))
                            if p.get("experiment_id")}
        for experiment in self.store.experiments(states=("running", "evaluating")):
            if experiment["experiment_id"] in active_proposals:
                continue
            workflow = self.store.workflow(experiment["workflow_id"])
            if experiment["kind"] == "factorial_main_effects":
                self._evaluate_engagement_experiment(experiment, workflow)
            elif experiment["kind"] == "caller_id_pickup":
                self._evaluate_pickup_experiment(experiment, workflow)

    def _retry_expired_experiments(self) -> None:
        """Refresh evidence after an approval rejection/expiry.

        A failed governance request must not silently terminate the learning
        loop.  We wait at least one simulated day (the batching/fatigue
        boundary), reload the current config, and submit a fresh proposal only
        if the recommendation still differs from that config.
        """
        for experiment in self.store.experiments(states=("draft",)):
            details = experiment["details"]
            retry_after = details.get("retry_after")
            if retry_after is None or self.now < int(retry_after):
                continue
            workflow = self.store.workflow(experiment["workflow_id"])
            if canonical(workflow["config"]) == canonical(experiment["experiment_config"]):
                self.store.update_experiment(experiment["experiment_id"], self.now, state="running",
                                             started_minute=self.now,
                                             deadline_minute=self.now + 3 * 1440,
                                             applied_version=workflow["version"])
                continue
            old = next((p for p in self.store.proposals()
                        if p.get("experiment_id") == experiment["experiment_id"]), None)
            action = old["action_type"] if old else "experiment_launch"
            on_apply = old.get("on_apply_state", "running") if old else "running"
            pid = self._queue_change(workflow, experiment["experiment_config"], action,
                                     {"experiment_id": experiment["experiment_id"],
                                      "retry_after_governance_outcome": True},
                                     experiment_id=experiment["experiment_id"],
                                     on_apply_state=on_apply)
            if pid:
                self.store.update_experiment(experiment["experiment_id"], self.now,
                                             state="awaiting_approval")

    def _experiment_was_overwritten(self, experiment: dict, workflow: dict) -> bool:
        for dim in experiment["details"]["dimensions"]:
            expected = distribution(experiment["experiment_config"], "default", dim)
            actual = distribution(workflow["config"], "default", dim)
            if expected != actual:
                self.store.update_experiment(experiment["experiment_id"], self.now, state="stale",
                                             result={"reason": "experimental dimension changed externally",
                                                     "dimension": dim})
                self.store.add_decision(
                    self.now, self.ts, workflow["workflow_id"], "experiment_stale",
                    f"Stopped {experiment['experiment_id']} after an external {dim} edit",
                    {"expected": expected, "actual": actual}, {}, {"state": "stale"},
                )
                return True
        return False

    @staticmethod
    def _guardrail_ok(candidate: dict, baseline: dict, guardrail: dict) -> tuple[bool, dict]:
        if "pickup_rate_max_drop" in guardrail:
            a, b = Rate(candidate["connected"], candidate["dials"]), Rate(baseline["connected"], baseline["dials"])
            lo, hi = difference_interval(a, b)
            limit = float(guardrail["pickup_rate_max_drop"])
            return a.value - b.value >= -limit, {"metric": "pickup_rate", "delta": a.value - b.value,
                                  "interval": [lo, hi], "max_drop": limit,
                                  "decision": "point_estimate_with_interval_reported"}
        a, b = Rate(candidate["engaged"], candidate["connected"]), Rate(baseline["engaged"], baseline["connected"])
        lo, hi = difference_interval(a, b)
        limit = float(guardrail.get("engagement_rate_max_drop", 0.02))
        return a.value - b.value >= -limit, {"metric": "engagement_rate", "delta": a.value - b.value,
                              "interval": [lo, hi], "max_drop": limit,
                              "decision": "point_estimate_with_interval_reported"}

    def _evaluate_engagement_experiment(self, experiment: dict, workflow: dict) -> None:
        if self._experiment_was_overwritten(experiment, workflow):
            return
        since = int(experiment["started_minute"])
        deadline = int(experiment["deadline_minute"])
        details = experiment["details"]
        hypotheses: list[dict] = []
        ready = True
        slices = ["all"] + list(dict.fromkeys(
            details.get("registered_slices", []) + self.store.observed_slices(workflow["workflow_id"])
        ))
        for dim in details["dimensions"]:
            baseline_option = details["baseline"][dim]
            overall = self.store.option_stats(workflow["workflow_id"], dim, since=since, slice_key="all")
            for option in workflow["spec"]["options"][dim]:
                if overall.get(option, {}).get("connected", 0) < details["overall_min_connected"]:
                    ready = False
            for slice_key in slices:
                threshold = details["overall_min_connected"] if slice_key == "all" else details["slice_min_connected"]
                counts = self.store.option_stats(workflow["workflow_id"], dim, since=since, slice_key=slice_key)
                base = counts.get(baseline_option)
                if not base or base["connected"] < threshold:
                    continue
                for option, candidate in counts.items():
                    if option == baseline_option or candidate["connected"] < threshold:
                        continue
                    a, b = Rate(candidate["engaged"], candidate["connected"]), Rate(base["engaged"], base["connected"])
                    p = two_proportion_pvalue(a, b, "greater")
                    lo, hi = difference_interval(a, b)
                    guard_ok, guard = self._guardrail_ok(candidate, base, workflow["spec"]["objective"]["guardrail"])
                    hypotheses.append({"dimension": dim, "slice": slice_key, "option": option,
                                       "baseline": baseline_option, "p": p, "lift": a.value - b.value,
                                       "interval": [lo, hi], "candidate": candidate, "control": base,
                                       "guardrail_ok": guard_ok, "guardrail": guard})
        if not ready and self.now < deadline:
            return
        adjusted = holm_adjust([h["p"] for h in hypotheses])
        for h, p_adj in zip(hypotheses, adjusted):
            h["adjusted_p"] = p_adj
            threshold = 0.02 if h["slice"] == "all" else 0.03
            h["qualifies"] = p_adj < 0.05 and h["lift"] >= threshold and h["guardrail_ok"]
            leads, active = self.store.affected_population(workflow["workflow_id"], h["slice"])
            h["affected_leads"] = active
            h["rank"] = active * max(h["interval"][0], 0.0)
        winners = [h for h in hypotheses if h["qualifies"]]
        result_config = self._base_preserving_unrelated_changes(experiment, workflow)
        chosen: list[dict] = []
        for dim in details["dimensions"]:
            overall = sorted((h for h in winners if h["dimension"] == dim and h["slice"] == "all"),
                             key=lambda h: h["rank"], reverse=True)
            if overall:
                h = overall[0]
                result_config = set_winner(result_config, dim, h["option"], h["baseline"])
                chosen.append(h)
        targeted = sorted((h for h in winners if h["slice"] != "all"), key=lambda h: h["rank"], reverse=True)
        if targeted:
            h = targeted[0]
            feature, value = h["slice"].split("=", 1)
            cohort_id = f"seg_{feature}_{_safe_id(value)}"
            result_config = add_target_cohort(result_config, cohort_id, feature, value)
            result_config = set_winner(result_config, h["dimension"], h["option"], h["baseline"], cohort_id)
            chosen.append(h)
            self.store.conn.execute(
                "INSERT OR REPLACE INTO cohorts VALUES (?,?,?,?,?,?,?)",
                (cohort_id, workflow["workflow_id"], feature, value, h["affected_leads"],
                 experiment["experiment_id"], None),
            )
        public_hypotheses = [{k: v for k, v in h.items() if k not in ("rank",)} for h in hypotheses]
        result = {"evaluated_at": self.now, "ready": ready, "hypotheses": public_hypotheses,
                  "chosen": chosen}
        target_state = "promoted" if chosen else "no_winner"
        action_type = "experiment_promote" if chosen else "experiment_restore"
        self.store.update_experiment(experiment["experiment_id"], self.now, state="evaluating", result=result)
        evidence = {"experiment_id": experiment["experiment_id"], "fixed_horizon": True,
                    "selected": chosen, "tested_hypotheses": len(hypotheses)}
        pid = self._queue_change(workflow, result_config, action_type, evidence,
                                 experiment_id=experiment["experiment_id"], on_apply_state=target_state)
        if not pid and canonical(result_config) == canonical(workflow["config"]):
            self.store.update_experiment(experiment["experiment_id"], self.now, state=target_state)
        elif not pid:
            self.store.update_experiment(experiment["experiment_id"], self.now, state="evaluating")

    def _base_preserving_unrelated_changes(self, experiment: dict, workflow: dict) -> dict:
        out = copy.deepcopy(workflow["config"])
        for dim in experiment["details"]["dimensions"]:
            out = apply_dimensions(out, experiment["baseline_config"], [dim])
        return out

    def _evaluate_pickup_experiment(self, experiment: dict, workflow: dict) -> None:
        if self._experiment_was_overwritten(experiment, workflow):
            return
        since, deadline = int(experiment["started_minute"]), int(experiment["deadline_minute"])
        counts = self.store.option_stats(workflow["workflow_id"], "caller_id", since=since)
        baseline_option = experiment["details"]["baseline"]["caller_id"]
        baseline = counts.get(baseline_option)
        ready = bool(baseline) and all(counts.get(o, {}).get("dials", 0) >= experiment["details"]["min_dials"]
                                       for o in workflow["spec"]["options"]["caller_id"])
        if not ready and self.now < deadline:
            return
        hypotheses = []
        min_dials = int(experiment["details"]["min_dials"])
        if baseline and baseline["dials"] >= min_dials:
            for option, candidate in counts.items():
                # A fixed-horizon pickup result is not promotable until both
                # control and candidate have the registered 500 dials.  The
                # deadline can produce a documented no-winner, never a
                # low-power promotion.
                if option == baseline_option or candidate["dials"] < min_dials:
                    continue
                a, b = Rate(candidate["connected"], candidate["dials"]), Rate(baseline["connected"], baseline["dials"])
                lo, hi = difference_interval(a, b)
                guard_ok, guard = self._guardrail_ok(candidate, baseline, workflow["spec"]["objective"]["guardrail"])
                hypotheses.append({"option": option, "baseline": baseline_option,
                                   "p": two_proportion_pvalue(a, b, "greater"), "lift": a.value - b.value,
                                   "interval": [lo, hi], "candidate": candidate, "control": baseline,
                                   "guardrail_ok": guard_ok, "guardrail": guard})
        for h, p_adj in zip(hypotheses, holm_adjust([h["p"] for h in hypotheses])):
            h["adjusted_p"] = p_adj
            h["qualifies"] = p_adj < 0.05 and h["lift"] >= 0.02 and h["guardrail_ok"]
        winners = sorted((h for h in hypotheses if h["qualifies"]), key=lambda h: h["interval"][0], reverse=True)
        result_config = self._base_preserving_unrelated_changes(experiment, workflow)
        chosen = winners[:1]
        if chosen:
            h = chosen[0]
            result_config = set_winner(result_config, "caller_id", h["option"], h["baseline"])
        target_state = "promoted" if chosen else "no_winner"
        result = {"evaluated_at": self.now, "ready": ready, "hypotheses": hypotheses, "chosen": chosen}
        self.store.update_experiment(experiment["experiment_id"], self.now, state="evaluating", result=result)
        pid = self._queue_change(workflow, result_config,
                                 "experiment_promote" if chosen else "experiment_restore",
                                 {"experiment_id": experiment["experiment_id"], "selected": chosen,
                                  "fixed_horizon": True}, experiment_id=experiment["experiment_id"],
                                 on_apply_state=target_state)
        if not pid and canonical(result_config) == canonical(workflow["config"]):
            self.store.update_experiment(experiment["experiment_id"], self.now, state=target_state)
        elif not pid:
            self.store.update_experiment(experiment["experiment_id"], self.now, state="evaluating")

    # ------------------------------------------------------------------ incidents and drift
    def _detect_caller_id_degradation(self) -> None:
        since = max(0, self.now - 1440)
        for workflow in self.store.workflows():
            counts = self.store.option_stats(workflow["workflow_id"], "caller_id", since=since)
            eligible = {o: c for o, c in counts.items() if c["dials"] >= 300}
            if len(eligible) < 2:
                continue
            best_name = max(eligible, key=lambda o: eligible[o]["connected"] / eligible[o]["dials"])
            best = eligible[best_name]
            for option, candidate in eligible.items():
                if option == best_name:
                    continue
                a, b = Rate(best["connected"], best["dials"]), Rate(candidate["connected"], candidate["dials"])
                diff = a.value - b.value
                p = two_proportion_pvalue(a, b, "greater")
                key = f"caller_id:{workflow['workflow_id']}:{option}"
                existing = next((i for i in self.store.incidents() if i["incident_key"] == key), None)
                if diff < 0.05 or p >= 0.01:
                    continue
                if existing and existing["status"] in ("proposed", "monitoring", "resolved", "blocked"):
                    continue
                proposed = remove_caller_id(workflow["config"], option)
                details = (existing["details"] if existing else {}) | {
                    "degraded": option, "best": best_name, "pickup_gap": diff, "p": p,
                    "counts": {option: candidate, best_name: best},
                }
                self.store.upsert_incident(key, workflow["workflow_id"], "caller_id", "detected", self.now, details)
                pid = self._queue_change(workflow, proposed, "caller_id_incident", details)
                if pid:
                    details["proposal_id"] = pid
                    self.store.upsert_incident(key, workflow["workflow_id"], "caller_id", "proposed", self.now, details)

    def _detect_prompt_incidents(self) -> None:
        since = max(0, self.now - 1440)
        for workflow in self.store.workflows():
            prompt = workflow["config"]["prompt_version"]
            recent_total = self.store.prompt_stats(workflow["workflow_id"], prompt, kind="events", since=since)
            recent_series = self.store.prompt_series(workflow["workflow_id"], prompt, kind="events", since=since)
            recent = self._take_window(recent_series, 300, reverse=True)
            if recent["connected"] < 300:
                continue
            experiments = self.store.experiments(workflow_id=workflow["workflow_id"])
            baseline_prompt = (experiments[0]["baseline_config"]["prompt_version"] if experiments
                               else prompt)
            baseline = self.store.prompt_stats(workflow["workflow_id"], baseline_prompt, kind="history")
            if baseline["connected"] < 300:
                continue
            a, b = Rate(recent["halluc"], recent["connected"]), Rate(baseline["halluc"], baseline["connected"])
            p, diff = two_proportion_pvalue(a, b, "greater"), a.value - b.value
            key = f"prompt:{workflow['workflow_id']}:{prompt}"
            incident = next((i for i in self.store.incidents() if i["incident_key"] == key), None)
            if incident and incident["status"] in ("proposed", "monitoring", "resolved", "blocked"):
                continue
            if incident and incident["status"] == "detected" and incident["details"].get("chosen"):
                self._queue_validated_prompt(key, workflow, incident["details"])
                continue
            details = incident["details"] if incident else {"consecutive": 0, "last_checked_connected": 0}
            if recent_total["connected"] < details.get("last_checked_connected", 0) + 100:
                continue
            details.update({"current_prompt": prompt, "baseline_prompt": baseline_prompt, "recent": recent,
                            "recent_total": recent_total, "baseline": baseline,
                            "flag_rate_increase": diff, "p": p,
                            "last_checked_connected": recent_total["connected"]})
            details["consecutive"] = details.get("consecutive", 0) + 1 if diff >= 0.01 and p < 0.005 else 0
            status = "detected" if details["consecutive"] >= 2 else "observing"
            self.store.upsert_incident(key, workflow["workflow_id"], "prompt", status, self.now, details)
            if status == "detected":
                self._respond_to_prompt_incident(key, workflow, details)

    @staticmethod
    def _prompt_rank(summary: str) -> tuple[int, str]:
        text = summary.lower()
        if "grounded" in text or "restricted" in text or "tighter" in text:
            rank = 0
        elif "current" in text or "production" in text:
            rank = 1
        elif "strict" in text or "verbatim" in text:
            rank = 2
        elif "persuasive" in text or "freer" in text:
            rank = 4
        else:
            rank = 3
        return rank, text

    def _respond_to_prompt_incident(self, key: str, workflow: dict, details: dict) -> None:
        try:
            suggestions = self.api.post("/llm/suggest-fixes", {
                "workflow_id": workflow["workflow_id"],
                "problem": f"QA hallucination flags increased by {details['flag_rate_increase']:.3f}",
            })["suggestions"]
            suggestions.sort(key=lambda x: self._prompt_rank(x.get("summary", "")))
            validations = []
            chosen = None
            threshold = workflow["spec"]["governance"].get("min_validation_pass_rate", 0.97)
            for suggestion in suggestions[:2]:
                run = self.api.post("/validation/runs", {
                    "workflow_id": workflow["workflow_id"],
                    "prompt_version": suggestion["prompt_version"], "n": 500,
                })
                validations.append({**suggestion, **run})
                if chosen is None and run["pass_rate"] >= threshold:
                    chosen = {**suggestion, **run}
            details["suggestions"] = suggestions
            details["validations"] = validations
            if not chosen:
                self.store.upsert_incident(key, workflow["workflow_id"], "prompt", "blocked", self.now, details)
                self.store.add_decision(self.now, self.ts, workflow["workflow_id"], "prompt_incident_blocked",
                                        "No suggested prompt passed validation", details, {}, {})
                return
            proposed = copy.deepcopy(workflow["config"])
            proposed["prompt_version"] = chosen["prompt_version"]
            details["chosen"] = chosen
            self._queue_validated_prompt(key, workflow, details)
        except SimulatorError as exc:
            details["error"] = str(exc)
            self.store.upsert_incident(key, workflow["workflow_id"], "prompt", "blocked", self.now, details)

    def _queue_validated_prompt(self, key: str, workflow: dict, details: dict) -> None:
        chosen = details["chosen"]
        proposed = copy.deepcopy(workflow["config"])
        proposed["prompt_version"] = chosen["prompt_version"]
        pid = self._queue_change(workflow, proposed, "prompt_incident", details,
                                 validation_run_id=chosen["validation_run_id"])
        if pid:
            details["proposal_id"] = pid
            self.store.upsert_incident(key, workflow["workflow_id"], "prompt", "proposed", self.now, details)

    def _monitor_incidents(self) -> None:
        """Verify applied remediations from observed post-change calls."""

        for incident in self.store.incidents(("monitoring",)):
            details = incident["details"]
            applied = details.get("applied_minute")
            if applied is None:
                continue
            workflow = self.store.workflow(incident["workflow_id"])
            if incident["kind"] == "caller_id":
                counts = self.store.option_stats(incident["workflow_id"], "caller_id", since=int(applied))
                observed = sum(row["dials"] for row in counts.values())
                if observed < 300:
                    continue
                degraded_share = counts.get(details["degraded"], {}).get("dials", 0) / max(observed, 1)
                details["verification"] = {"dials": observed, "degraded_share": degraded_share}
                status = "resolved" if degraded_share <= 0.05 else "failed"
            else:
                chosen = details["chosen"]["prompt_version"]
                recent_series = self.store.prompt_series(incident["workflow_id"], chosen, kind="events",
                                                        since=int(applied))
                recent = self._take_window(recent_series, 300, reverse=True)
                if recent["connected"] < 300:
                    continue
                before = details["recent"]
                historical = details["baseline"]
                qa_rate = recent["halluc"] / recent["connected"]
                before_qa = before["halluc"] / max(before["connected"], 1)
                historical_qa = historical["halluc"] / max(historical["connected"], 1)
                engage = recent["engaged"] / recent["connected"]
                before_engage = before["engaged"] / max(before["connected"], 1)
                # The declared post-apply success rule is relative recovery:
                # QA flags must fall from the incident window and engagement
                # must remain within its guardrail.  Historical QA is retained
                # as context, not an extra hidden hurdle.
                qa_fell = qa_rate < before_qa
                engagement_ok = engage >= before_engage - 0.02
                details["verification"] = {
                    "calls": recent, "qa_rate": qa_rate, "incident_qa_rate": before_qa,
                    "historical_qa_rate": historical_qa, "engagement_rate": engage,
                    "incident_engagement_rate": before_engage, "qa_fell": qa_fell,
                    "engagement_guardrail_ok": engagement_ok,
                }
                status = "resolved" if qa_fell and engagement_ok else "failed"
            self.store.upsert_incident(incident["incident_key"], incident["workflow_id"],
                                       incident["kind"], status, self.now, details)
            self.store.add_decision(
                self.now, self.ts, incident["workflow_id"], "incident_verified",
                f"{incident['kind']} remediation {status}", details["verification"],
                {"proposal_id": details.get("proposal_id")}, {"status": status},
            )

    def _monitor_promotions(self) -> None:
        for experiment in self.store.experiments(states=("promoted",)):
            result = experiment.get("result") or {}
            chosen = result.get("chosen") or []
            if not chosen or experiment.get("started_minute") is None:
                continue
            details = experiment["details"]
            if details.get("rollback_queued"):
                continue
            promoted_at = int(details.get("promoted_minute", experiment["updated_minute"]))
            guard = self.store.workflow(experiment["workflow_id"])["spec"]["objective"]["guardrail"]
            streams = details.setdefault("decay_streams", {})
            rollback_reason = None
            for selection in chosen:
                dim = selection.get("dimension", "caller_id")
                option = selection["option"]
                slice_key = selection.get("slice", "all")
                stream_key = f"{dim}:{option}:{slice_key}"
                state = streams.setdefault(stream_key, {"checks": 0, "last_observations": 300})
                series = self.store.option_series(experiment["workflow_id"], dim, option, promoted_at,
                                                  slice_key=slice_key)
                metric = "engagement" if dim in ("welcome_message", "voice") else "pickup"
                trial_key = "connected" if metric == "engagement" else "dials"
                observations = sum(int(row[trial_key] or 0) for row in series)
                if observations < 400 or observations < state.get("last_observations", 300) + 100:
                    continue
                reference = self._take_window(series, 300, reverse=False, trial_key=trial_key)
                recent = self._take_window(series, 300, reverse=True, trial_key=trial_key)
                ref_rate = (Rate(reference["engaged"], reference["connected"]) if metric == "engagement"
                            else Rate(reference["connected"], reference["dials"]))
                recent_rate = (Rate(recent["engaged"], recent["connected"]) if metric == "engagement"
                               else Rate(recent["connected"], recent["dials"]))
                if min(ref_rate.trials, recent_rate.trials) < 300:
                    continue
                drop = ref_rate.value - recent_rate.value
                p = two_proportion_pvalue(ref_rate, recent_rate, "greater")
                state["checks"] = state.get("checks", 0) + 1 if drop >= 0.03 and p < 0.01 else 0
                if metric == "engagement":
                    guard_ref, guard_recent = (Rate(reference["connected"], reference["dials"]),
                                               Rate(recent["connected"], recent["dials"]))
                    guard_limit = float(guard.get("pickup_rate_max_drop", 0.02))
                else:
                    guard_ref, guard_recent = (Rate(reference["engaged"], reference["connected"]),
                                               Rate(recent["engaged"], recent["connected"]))
                    guard_limit = float(guard.get("engagement_rate_max_drop", 0.02))
                guard_drop = guard_ref.value - guard_recent.value
                guard_p = two_proportion_pvalue(guard_ref, guard_recent, "greater")
                guard_breach = guard_drop > guard_limit and guard_p < 0.01
                state.update({"last_observations": observations,
                              "last": {"drop": drop, "p": p, "reference": reference, "recent": recent,
                                       "guardrail_drop": guard_drop, "guardrail_p": guard_p}})
                if state["checks"] >= 2 or guard_breach:
                    rollback_reason = {"stream": stream_key, "drop": drop, "p": p,
                                       "guardrail_drop": guard_drop, "guardrail_p": guard_p}
                    break
            # Keep the original summary fields for the CLI/status contract,
            # while the per-stream map ensures a second promoted dimension is
            # not silently left unmonitored.
            if streams:
                first = next(iter(streams.values()))
                details["decay_checks"] = first.get("checks", 0)
                details["decay_last_observations"] = first.get("last_observations", 300)
                details["decay_last"] = first.get("last")
            if rollback_reason:
                workflow = self.store.workflow(experiment["workflow_id"])
                rollback_dims = list(experiment["details"]["dimensions"])
                if workflow["config"].get("cohorts") != experiment["baseline_config"].get("cohorts"):
                    rollback_dims.append("cohorts")
                rollback = apply_dimensions(workflow["config"], experiment["baseline_config"],
                                            rollback_dims)
                details["rollback_reason"] = rollback_reason
                pid = self._queue_change(workflow, rollback, "decay_rollback", details,
                                         experiment_id=experiment["experiment_id"], on_apply_state="rolled_back")
                details["rollback_queued"] = bool(pid)
            self.store.update_experiment(experiment["experiment_id"], self.now, details=details)

    @staticmethod
    def _take_window(series: list[dict], target: int, reverse: bool,
                     trial_key: str = "connected") -> dict[str, int]:
        total = {"dials": 0, "connected": 0, "engaged": 0, "halluc": 0}
        rows = reversed(series) if reverse else iter(series)
        for row in rows:
            for key in total:
                total[key] += int(row[key] or 0)
            if total[trial_key] >= target:
                break
        return total

    # ------------------------------------------------------------------ governance
    def _proposal_id(self, workflow_id: str, action_type: str, base_version: int, config: dict) -> str:
        existing = len(self.store.proposals()) + 1
        seed = f"{self.store.get_meta('scenario')}|{workflow_id}|{action_type}|{base_version}|{canonical(config)}|{existing}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

    def _queue_change(self, workflow: dict, proposed: dict, action_type: str, evidence: dict,
                      *, experiment_id: str | None = None, on_apply_state: str | None = None,
                      validation_run_id: str | None = None) -> str | None:
        dims = changed_dims(workflow["config"], proposed)
        if not dims:
            return None
        active = [p for p in self.store.proposals(("queued", "pending", "approved"))
                  if p["workflow_id"] == workflow["workflow_id"]]
        if active:
            self.store.add_decision(self.now, self.ts, workflow["workflow_id"], "change_deferred",
                                    f"Deferred {action_type}; another proposal is active",
                                    evidence, {"active_proposal": active[0]["proposal_id"]}, {})
            return None
        # The simulator's approver policy is intentionally fatigue-sensitive.
        # Keep one durable, batched request per workflow/day; a later cycle
        # will retry after the evidence window or approval outcome changes.
        day_start = self.now - self.now % 1440
        recent = [p for p in self.store.proposals()
                  if p["workflow_id"] == workflow["workflow_id"] and p["created_minute"] >= day_start]
        if recent:
            self.store.add_decision(
                self.now, self.ts, workflow["workflow_id"], "change_deferred",
                f"Deferred {action_type}; one proposal per workflow/day",
                evidence, {"existing_proposal": recent[-1]["proposal_id"]}, {},
            )
            return None
        slice_key = "all"
        selected = evidence.get("selected") or []
        if selected and selected[0].get("slice"):
            slice_key = selected[0]["slice"]
        leads, affected = self.store.affected_population(workflow["workflow_id"], slice_key)
        lower = max((item.get("interval", [0.0])[0] for item in selected), default=0.0)
        p_adj = min((item.get("adjusted_p", 1.0) for item in selected), default=0.5)
        score = impact_score(lower, affected / max(leads, 1), 1.0 - p_adj)
        pid = self._proposal_id(workflow["workflow_id"], action_type, workflow["version"], proposed)
        if selected:
            lead = selected[0]
            baseline_name = lead.get("baseline", "current")
            variant_name = lead.get("option", "proposed")
            sample = lead.get("candidate", {})
            control = lead.get("control", {})
            rationale = (
                f"Baseline {baseline_name} vs variant {variant_name} for {lead.get('slice', 'all')}; "
                f"n={sample.get('connected', sample.get('dials', 0))}/"
                f"{control.get('connected', control.get('dials', 0))}; "
                f"lift={lead.get('lift', 0.0):.4f}; "
                f"95% interval={lead.get('interval')}; adjusted_p={lead.get('adjusted_p', lead.get('p'))}; "
                f"guardrail={'pass' if lead.get('guardrail_ok', True) else 'fail'}; "
                "rollback after two consecutive supported 3pp primary drops or a supported guardrail breach."
            )
        else:
            rationale = (
                f"No qualifying winner for {action_type}; restore the registered baseline while preserving "
                "unrelated supervisor configuration."
            )
        body = {
            "proposal_id": pid, "workflow_id": workflow["workflow_id"], "base_version": workflow["version"],
            "proposed_config": proposed,
            "change_summary": f"{action_type.replace('_', ' ')} on {workflow['workflow_id']}",
            "diff": {"changed_dims": dims, "base_version": workflow["version"]},
            "affected_leads": affected,
            "dry_run": {"method": "randomized observed rates", "evidence": evidence,
                        "expected_primary_and_guardrail": selected,
                        "sample_counts": {"affected_leads": affected, "population_leads": leads},
                        "warning": "unsupported cells retain the current allocation"},
            "impact_score": score,
            "rationale": f"{rationale} affected_leads={affected}; impact_score={score:.3f}",
            "controller_base_config": workflow["config"],
        }
        if validation_run_id:
            body["evidence"] = {"validation_run_id": validation_run_id}
        self.store.create_proposal({
            "proposal_id": pid, "workflow_id": workflow["workflow_id"], "experiment_id": experiment_id,
            "action_type": action_type, "status": "queued", "base_version": workflow["version"],
            "proposed_config": proposed, "body": body, "on_apply_state": on_apply_state,
            "created_minute": self.now, "updated_minute": self.now,
        })
        self.store.add_decision(self.now, self.ts, workflow["workflow_id"], "change_queued",
                                body["change_summary"], evidence, {"proposal_id": pid, "diff": body["diff"]}, {})
        return pid

    def _submit_queued(self) -> None:
        pending_workflows = {p["workflow_id"] for p in self.store.proposals(("pending", "approved"))}
        for proposal in self.store.proposals(("queued",)):
            if proposal["workflow_id"] in pending_workflows:
                continue
            workflow = self.store.workflow(proposal["workflow_id"])
            if not self._rebase_if_needed(proposal, workflow):
                continue
            proposal = next(p for p in self.store.proposals(("queued",)) if p["proposal_id"] == proposal["proposal_id"])
            workflow = self.store.workflow(proposal["workflow_id"])
            dims = changed_dims(workflow["config"], proposal["proposed_config"])
            policy = workflow["spec"]["governance"]["auto_apply"]
            auto = (policy.get("enabled") and all(dim in policy.get("allowed_dims", []) for dim in dims)
                    and max_allocation_shift(workflow["config"], proposal["proposed_config"])
                    <= policy.get("max_shift", 0.0) + 1e-9)
            try:
                if auto:
                    result = self.api.put(f"/workflows/{workflow['workflow_id']}/config", {
                        "proposal_id": proposal["proposal_id"], "base_version": workflow["version"],
                        "proposed_config": proposal["proposed_config"],
                        "rationale": proposal["body"]["rationale"],
                        "evidence": proposal["body"].get("evidence"),
                    })
                    self._mark_applied(proposal, result)
                elif self._within_submit_window(workflow):
                    body = {k: v for k, v in proposal["body"].items() if k != "controller_base_config"}
                    remote = self.api.post("/approvals", body)
                    self.store.update_proposal(proposal["proposal_id"], self.now, status="pending",
                                               approval_id=remote["approval_id"])
                    self.store.add_decision(self.now, self.ts, workflow["workflow_id"], "approval_requested",
                                            body["change_summary"], body["dry_run"],
                                            {"proposal_id": proposal["proposal_id"],
                                             "approval_id": remote["approval_id"]}, remote)
                    pending_workflows.add(workflow["workflow_id"])
            except SimulatorError as exc:
                if exc.code == "duplicate_proposal_id":
                    # A crash can happen after the simulator applied an
                    # auto-change but before SQLite recorded it.  Recover the
                    # idempotent result from the current config instead of
                    # leaving the proposal queued forever.
                    try:
                        current = self.api.config(proposal["workflow_id"])
                        if canonical(current["config"]) == canonical(proposal["proposed_config"]):
                            self._mark_applied(proposal, {
                                "workflow_id": proposal["workflow_id"],
                                "version": current["version"],
                                "changed_dims": changed_dims(workflow["config"], current["config"]),
                                "recovered_after_duplicate": True,
                            })
                        else:
                            self._reconcile_approvals()
                    except SimulatorError:
                        self._reconcile_approvals()
                elif exc.code == "stale_base_version":
                    self.store.update_proposal(proposal["proposal_id"], self.now, status="stale",
                                               reasons=[str(exc)])
                else:
                    self.store.add_decision(self.now, self.ts, workflow["workflow_id"], "proposal_error",
                                            str(exc), {}, {"proposal_id": proposal["proposal_id"]}, {})

    def _within_submit_window(self, workflow: dict) -> bool:
        minute_of_day = self.now % 1440
        lo, hi = workflow["spec"]["governance"]["approver_hours"]
        start = lo * 60 + 15
        if workflow["tenant_id"] == "kestrel_finance":
            start = max(start, 11 * 60)
        return start <= minute_of_day < hi * 60

    def _rebase_if_needed(self, proposal: dict, workflow: dict) -> bool:
        if proposal["base_version"] == workflow["version"]:
            return True
        old_base = proposal["body"].get("controller_base_config")
        if not old_base:
            self.store.update_proposal(proposal["proposal_id"], self.now, status="stale",
                                       reasons=["base config unavailable"])
            return False
        intended = changed_dims(old_base, proposal["proposed_config"])
        external = changed_dims(old_base, workflow["config"])
        overlap = sorted(set(intended) & set(external))
        if overlap:
            self.store.update_proposal(proposal["proposal_id"], self.now, status="stale",
                                       reasons=[f"supervisor/external edit overlaps {overlap}"])
            if proposal.get("experiment_id"):
                self.store.update_experiment(proposal["experiment_id"], self.now, state="stale",
                                             result={"reason": "overlapping external edit", "dims": overlap})
            return False
        rebased = apply_dimensions(workflow["config"], proposal["proposed_config"], intended)
        body = proposal["body"]
        body.update({"base_version": workflow["version"], "proposed_config": rebased,
                     "controller_base_config": workflow["config"]})
        body["diff"]["base_version"] = workflow["version"]
        self.store.update_proposal(proposal["proposal_id"], self.now, base_version=workflow["version"],
                                   proposed_config=rebased, body=body)
        return True

    def _reconcile_approvals(self) -> None:
        remote_by_pid = {a["proposal_id"]: a for a in self.api.approvals()}
        for proposal in self.store.proposals(("queued", "pending", "approved")):
            remote = remote_by_pid.get(proposal["proposal_id"])
            if not remote:
                continue
            status = remote["status"]
            if status == "pending":
                self.store.update_proposal(proposal["proposal_id"], self.now, status="pending",
                                           approval_id=remote["approval_id"])
            elif status == "approved":
                self.store.update_proposal(proposal["proposal_id"], self.now, status="approved",
                                           approval_id=remote["approval_id"], reasons=remote.get("reasons", []))
                try:
                    result = self.api.post(f"/workflows/{proposal['workflow_id']}/apply",
                                           {"approval_id": remote["approval_id"]})
                    self._mark_applied(proposal, result)
                except SimulatorError as exc:
                    new_status = "lapsed" if exc.code == "approval_lapsed" else "stale" if exc.code == "stale_base_version" else "approved"
                    self.store.update_proposal(proposal["proposal_id"], self.now, status=new_status,
                                               reasons=[str(exc)])
            elif status == "applied":
                current = self.api.config(proposal["workflow_id"])
                self._mark_applied(proposal, {
                    "workflow_id": proposal["workflow_id"], "version": current["version"],
                    "changed_dims": remote.get("changed_dims", []), "recovered_after_restart": True,
                })
            elif status in ("rejected", "expired", "lapsed"):
                self.store.update_proposal(proposal["proposal_id"], self.now, status=status,
                                           approval_id=remote["approval_id"], reasons=remote.get("reasons", []))
                if proposal.get("experiment_id"):
                    experiment = next((e for e in self.store.experiments()
                                       if e["experiment_id"] == proposal["experiment_id"]), None)
                    details = (experiment or {}).get("details", {})
                    details.update({"retry_after": self.now + 1440,
                                    "last_governance_status": status,
                                    "last_governance_reasons": remote.get("reasons", [])})
                    self.store.update_experiment(proposal["experiment_id"], self.now, state="draft",
                                                 details=details,
                                                 result={"reason": status, "reasons": remote.get("reasons", [])})

    def _mark_applied(self, proposal: dict, result: dict) -> None:
        self.store.update_proposal(proposal["proposal_id"], self.now, status="applied",
                                   applied_version=result["version"])
        current = self.api.config(proposal["workflow_id"])
        self.store.update_workflow_config(proposal["workflow_id"], current, self.now)
        if proposal.get("experiment_id") and proposal.get("on_apply_state"):
            changes = {"state": proposal["on_apply_state"], "applied_version": result["version"]}
            experiment = next(e for e in self.store.experiments()
                              if e["experiment_id"] == proposal["experiment_id"])
            details = experiment["details"]
            if proposal["on_apply_state"] == "running":
                changes.update({"started_minute": self.now, "deadline_minute": self.now + 3 * 1440})
            elif proposal["on_apply_state"] == "promoted":
                details.update({"promoted_minute": self.now, "decay_last_observations": 300,
                                "decay_checks": 0})
                changes["details"] = details
            self.store.update_experiment(proposal["experiment_id"], self.now, **changes)
            # The cohort predicate is part of the same configuration change;
            # record the version at which it became active for later explain
            # and rollback queries.
            if proposal["on_apply_state"] == "promoted":
                for cohort in self.store.conn.execute(
                    "SELECT cohort_id FROM cohorts WHERE workflow_id=? AND experiment_id=?",
                    (proposal["workflow_id"], proposal["experiment_id"]),
                ).fetchall():
                    self.store.activate_cohort(proposal["workflow_id"], cohort[0], result["version"])
        for incident in self.store.incidents(("proposed",)):
            if incident["details"].get("proposal_id") == proposal["proposal_id"]:
                details = incident["details"]
                details.update({"applied_minute": self.now, "applied_version": result["version"]})
                self.store.upsert_incident(incident["incident_key"], incident["workflow_id"], incident["kind"],
                                           "monitoring", self.now, details)
        self.store.add_decision(self.now, self.ts, proposal["workflow_id"], "change_applied",
                                f"Applied {proposal['action_type']}", proposal["body"].get("dry_run", {}),
                                {"proposal_id": proposal["proposal_id"]}, result)
