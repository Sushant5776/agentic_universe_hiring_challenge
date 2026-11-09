"""Workflow configuration manipulation without importing the simulator."""

from __future__ import annotations

import copy
import json

AGENT_DIMS = ("welcome_message", "voice", "caller_id")
CHANGE_DIMS = ("cohorts", "welcome_message", "voice", "caller_id", "prompt_version", "schedule")


def distribution(config: dict, cohort: str, dim: str) -> dict[str, float]:
    return ((config.get("allocations", {}).get(cohort) or {}).get(dim)
            or config["allocations"]["default"][dim])


def changed_dims(old: dict, new: dict) -> list[str]:
    changed: list[str] = []
    if old.get("cohorts", []) != new.get("cohorts", []):
        changed.append("cohorts")
    cohorts = {"default"}
    cohorts.update(c.get("id") for c in old.get("cohorts", []))
    cohorts.update(c.get("id") for c in new.get("cohorts", []))
    for dim in AGENT_DIMS:
        if any(distribution(old, cohort, dim) != distribution(new, cohort, dim) for cohort in cohorts):
            changed.append(dim)
    if old.get("prompt_version") != new.get("prompt_version"):
        changed.append("prompt_version")
    if old.get("schedule", {}) != new.get("schedule", {}):
        changed.append("schedule")
    return [dim for dim in CHANGE_DIMS if dim in changed]


def max_allocation_shift(old: dict, new: dict) -> float:
    cohorts = {"default"}
    cohorts.update(c.get("id") for c in old.get("cohorts", []))
    cohorts.update(c.get("id") for c in new.get("cohorts", []))
    worst = 0.0
    for cohort in cohorts:
        for dim in AGENT_DIMS:
            a, b = distribution(old, cohort, dim), distribution(new, cohort, dim)
            tv = 0.5 * sum(abs(a.get(option, 0.0) - b.get(option, 0.0))
                           for option in set(a) | set(b))
            worst = max(worst, tv)
    return worst


def canonical(config: dict) -> str:
    return json.dumps(config, sort_keys=True, separators=(",", ":"))


def equal_experiment_config(config: dict, options: dict, dimensions: tuple[str, ...]) -> dict:
    out = copy.deepcopy(config)
    for dim in dimensions:
        values = options[dim]
        out["allocations"]["default"][dim] = {value: 1.0 / len(values) for value in values}
    return out


def caller_id_exploration_config(config: dict, options: list[str], exploration: float = 0.10) -> dict:
    out = copy.deepcopy(config)
    current = distribution(config, "default", "caller_id")
    unseen = [option for option in options if current.get(option, 0.0) == 0]
    if not unseen:
        return out
    reserve = min(exploration * len(unseen), 0.5)
    active_total = sum(current.values()) or 1.0
    proposed = {option: weight / active_total * (1.0 - reserve) for option, weight in current.items() if weight > 0}
    each = reserve / len(unseen)
    proposed.update({option: each for option in unseen})
    out["allocations"]["default"]["caller_id"] = proposed
    return out


def set_winner(config: dict, dim: str, winner: str, baseline: str, cohort: str = "default") -> dict:
    out = copy.deepcopy(config)
    target = out["allocations"].setdefault(cohort, {})
    target[dim] = {winner: 1.0} if winner == baseline else {winner: 0.9, baseline: 0.1}
    return out


def add_target_cohort(config: dict, cohort_id: str, feature: str, value: str) -> dict:
    out = copy.deepcopy(config)
    existing = [c for c in out.get("cohorts", []) if c.get("id") != cohort_id]
    out["cohorts"] = [{"id": cohort_id, "where": {feature: [value]}}] + existing
    return out


def remove_caller_id(config: dict, degraded: str) -> dict:
    out = copy.deepcopy(config)
    current = distribution(config, "default", "caller_id")
    healthy = {k: v for k, v in current.items() if k != degraded and v > 0}
    if not healthy:
        return out
    total = sum(healthy.values())
    out["allocations"]["default"]["caller_id"] = {k: v / total for k, v in healthy.items()}
    return out


def apply_dimensions(target: dict, source: dict, dims: list[str]) -> dict:
    """Copy selected logical dimensions from source into a target config."""

    out = copy.deepcopy(target)
    if "cohorts" in dims:
        out["cohorts"] = copy.deepcopy(source.get("cohorts", []))
        # A cohort definition and its allocation are one logical dimension.
        # Removing a cohort must not leave an orphaned allocation key that the
        # simulator rejects as an undefined cohort.
        allowed = {"default"} | {c.get("id") for c in out["cohorts"]}
        out["allocations"] = {k: v for k, v in out.get("allocations", {}).items() if k in allowed}
        schedule = out.get("schedule") or {}
        if "hour_weights" in schedule:
            schedule["hour_weights"] = {k: v for k, v in schedule["hour_weights"].items() if k in allowed}
    for dim in AGENT_DIMS:
        if dim not in dims:
            continue
        all_cohorts = set(out.get("allocations", {})) | set(source.get("allocations", {}))
        for cohort in all_cohorts:
            if cohort in source.get("allocations", {}) and dim in source["allocations"][cohort]:
                out["allocations"].setdefault(cohort, {})[dim] = copy.deepcopy(
                    source["allocations"][cohort][dim]
                )
            elif cohort != "default":
                out["allocations"].setdefault(cohort, {}).pop(dim, None)
    if "prompt_version" in dims:
        out["prompt_version"] = source["prompt_version"]
    if "schedule" in dims:
        out["schedule"] = copy.deepcopy(source.get("schedule", {}))
    return out
