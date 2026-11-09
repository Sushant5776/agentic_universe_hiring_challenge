import json

import pytest

from controller.db import Store


def line(value):
    return json.dumps(value)


def test_chunk_replay_is_idempotent_and_joins_person_features(tmp_path):
    store = Store(tmp_path / "controller.sqlite3")
    events = [
        line({"event_id": 1, "type": "lead", "ts": "2027-01-04T00:00:00+05:30",
              "workflow_id": "wf", "campaign_id": "c1", "person_id": "p1",
              "features": {"lang": "hi", "city_tier": "T1", "age_band": "18-30",
                           "value_band": "high"}}),
        line({"event_id": 2, "type": "call", "ts": "2027-01-04T09:00:00+05:30",
              "workflow_id": "wf", "campaign_id": "c1", "person_id": "p1", "config_version": 1,
              "cohort_id": "default", "connected": True, "engaged": True,
              "qa": {"hallucination_flag": False},
              "variant": {"welcome_message": "W1", "voice": "V1", "caller_id": "D1",
                          "prompt_version": "P1"}}),
    ]
    meta = {"chunk": 0, "events": 2, "last_event_id": 2, "until_minute": 540}
    result = store.ingest_chunk("events", meta, events)
    replay = store.ingest_chunk("events", meta, events)

    assert result["calls"] == 1
    assert replay["skipped"] is True
    assert store.option_stats("wf", "voice")["V1"] == {
        "dials": 1, "connected": 1, "engaged": 1, "halluc": 0,
    }
    assert store.affected_population("wf", "lang=hi") == (1, 0)
    store.close()


def test_failed_chunk_rolls_back_cursor_and_aggregates(tmp_path):
    store = Store(tmp_path / "controller.sqlite3")
    bad = [line({"event_id": 1, "type": "call", "ts": "2027-01-04T09:00:00+05:30",
                 "workflow_id": "wf", "campaign_id": "c1", "person_id": "missing",
                 "config_version": 1, "connected": False, "engaged": False,
                 "variant": {"welcome_message": "W1", "voice": "V1", "caller_id": "D1",
                             "prompt_version": "P1"}})]
    with pytest.raises(RuntimeError, match="missing features"):
        store.ingest_chunk("events", {"chunk": 0, "until_minute": 540}, bad)
    assert store.cursor("events")["last_chunk"] == -1
    assert store.option_stats("wf", "voice") == {}
    store.close()


def test_lead_evicted_from_cache_is_flushed_before_same_chunk_call(tmp_path):
    store = Store(tmp_path / "controller.sqlite3")
    store._cache_limit = 1
    lead = lambda event_id, person: line({
        "event_id": event_id, "type": "lead", "ts": "2027-01-04T00:00:00+05:30",
        "workflow_id": "wf", "campaign_id": "c1", "person_id": person,
        "features": {"lang": "hi", "city_tier": "T1", "age_band": "18-30", "value_band": "high"},
    })
    call = line({"event_id": 3, "type": "call", "ts": "2027-01-04T09:00:00+05:30",
                 "workflow_id": "wf", "campaign_id": "c1", "person_id": "p1", "config_version": 1,
                 "connected": False, "engaged": False,
                 "variant": {"welcome_message": "W1", "voice": "V1", "caller_id": "D1",
                             "prompt_version": "P1"}})
    store.ingest_chunk("events", {"chunk": 0, "until_minute": 540}, [lead(1, "p1"), lead(2, "p2"), call])
    assert store.option_stats("wf", "voice")["V1"]["dials"] == 1
    store.close()
