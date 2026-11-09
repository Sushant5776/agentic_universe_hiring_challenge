import pytest

from controller.config import (add_target_cohort, apply_dimensions, caller_id_exploration_config,
                               changed_dims, max_allocation_shift, remove_caller_id, set_winner)


def base_config():
    return {
        "allocations": {"default": {
            "welcome_message": {"W1": 1.0},
            "voice": {"V1": 1.0},
            "caller_id": {"D1": 1.0},
        }},
        "prompt_version": "P1",
    }


def test_caller_id_exploration_reserves_ten_percent_per_unseen_id():
    proposed = caller_id_exploration_config(base_config(), ["D1", "D2", "D3"])
    assert proposed["allocations"]["default"]["caller_id"] == {"D1": 0.8, "D2": 0.1, "D3": 0.1}
    assert changed_dims(base_config(), proposed) == ["caller_id"]
    assert max_allocation_shift(base_config(), proposed) == pytest.approx(0.2)


def test_degraded_id_is_removed_and_remaining_weights_are_normalised():
    config = base_config()
    config["allocations"]["default"]["caller_id"] = {"D1": 0.2, "D2": 0.8}
    proposed = remove_caller_id(config, "D2")
    assert proposed["allocations"]["default"]["caller_id"] == {"D1": 1.0}


def test_targeted_winner_does_not_change_default_allocation():
    config = add_target_cohort(base_config(), "seg_lang_hi", "lang", "hi")
    config = set_winner(config, "voice", "V2", "V1", "seg_lang_hi")
    assert config["allocations"]["default"]["voice"] == {"V1": 1.0}
    assert config["allocations"]["seg_lang_hi"]["voice"] == {"V2": 0.9, "V1": 0.1}


def test_removing_cohort_removes_orphaned_allocations():
    target = set_winner(add_target_cohort(base_config(), "seg_lang_hi", "lang", "hi"),
                        "voice", "V2", "V1", "seg_lang_hi")
    restored = apply_dimensions(target, base_config(), ["cohorts", "voice"])
    assert restored["cohorts"] == []
    assert set(restored["allocations"]) == {"default"}
