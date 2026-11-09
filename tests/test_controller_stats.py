from controller.stats import Rate, difference_interval, holm_adjust, impact_score, two_proportion_pvalue


def test_two_proportion_test_detects_clear_lift():
    p = two_proportion_pvalue(Rate(620, 1000), Rate(540, 1000), "greater")
    assert p < 0.001
    low, high = difference_interval(Rate(620, 1000), Rate(540, 1000))
    assert 0 < low < high


def test_two_proportion_test_refuses_empty_evidence():
    assert two_proportion_pvalue(Rate(0, 0), Rate(4, 10)) == 1.0


def test_holm_adjustment_is_monotone_in_rank_and_order_preserving():
    adjusted = holm_adjust([0.04, 0.001, 0.02])
    assert adjusted == [0.04, 0.003, 0.04]


def test_impact_score_clamps_and_uses_lower_bound():
    assert impact_score(0.05, 0.5, 0.8) == 0.2
    assert impact_score(-0.01, 1.0, 1.0) == 0.0
    assert impact_score(1.0, 1.0, 1.0) == 1.0
