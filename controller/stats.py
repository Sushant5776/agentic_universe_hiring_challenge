"""Small, dependency-free statistical helpers used by controller decisions."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Rate:
    successes: int
    trials: int

    @property
    def value(self) -> float:
        return self.successes / self.trials if self.trials else 0.0


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def two_proportion_pvalue(a: Rate, b: Rate, alternative: str = "greater") -> float:
    """P-value for a.value - b.value using a pooled two-proportion z test.

    ``greater`` tests whether ``a`` is better than ``b``; ``less`` tests the
    reverse. Degenerate samples deliberately return 1 rather than producing a
    decision from no evidence.
    """

    if a.trials <= 0 or b.trials <= 0:
        return 1.0
    pooled = (a.successes + b.successes) / (a.trials + b.trials)
    variance = pooled * (1.0 - pooled) * (1.0 / a.trials + 1.0 / b.trials)
    if variance <= 0:
        if a.value == b.value:
            return 1.0
        z = math.inf if a.value > b.value else -math.inf
    else:
        z = (a.value - b.value) / math.sqrt(variance)
    if alternative == "greater":
        return max(0.0, min(1.0, 1.0 - normal_cdf(z)))
    if alternative == "less":
        return max(0.0, min(1.0, normal_cdf(z)))
    return max(0.0, min(1.0, 2.0 * (1.0 - normal_cdf(abs(z)))))


def difference_interval(a: Rate, b: Rate, z: float = 1.96) -> tuple[float, float]:
    """Normal-approximation interval for the difference of two proportions."""

    diff = a.value - b.value
    if a.trials <= 0 or b.trials <= 0:
        return -1.0, 1.0
    var = a.value * (1.0 - a.value) / a.trials + b.value * (1.0 - b.value) / b.trials
    half = z * math.sqrt(max(var, 0.0))
    return diff - half, diff + half


def holm_adjust(pvalues: list[float]) -> list[float]:
    """Holm family-wise-error adjusted p-values in original order."""

    n = len(pvalues)
    order = sorted(range(n), key=pvalues.__getitem__)
    adjusted = [1.0] * n
    running = 0.0
    for rank, idx in enumerate(order):
        value = min(1.0, (n - rank) * pvalues[idx])
        running = max(running, value)
        adjusted[idx] = running
    return adjusted


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def impact_score(lower_bound_lift: float, affected_share: float, confidence: float) -> float:
    return clamp((max(lower_bound_lift, 0.0) / 0.10) * clamp(affected_share) * clamp(confidence))
