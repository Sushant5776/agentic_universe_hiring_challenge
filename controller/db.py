"""SQLite persistence and atomic event-chunk ingestion."""

from __future__ import annotations

import json
import sqlite3
import time
from collections import OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

BASE_TIME = datetime.fromisoformat("2027-01-04T00:00:00+05:30")
FEATURES = ("lang", "city_tier", "age_band", "value_band")
DIMENSION_COLUMNS = {
    "welcome_message": "welcome",
    "voice": "voice",
    "caller_id": "caller_id",
    "prompt_version": "prompt",
}


def dumps(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def loads(value: str | None, default=None):
    if value is None:
        return default
    return json.loads(value)


def sim_minute(ts: str) -> int:
    return int((datetime.fromisoformat(ts) - BASE_TIME).total_seconds() // 60)


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=60, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self._feature_cache: OrderedDict[str, tuple[str, str, str, str]] = OrderedDict()
        self._cache_limit = 200_000
        self._migrate()

    def close(self) -> None:
        self.conn.close()

    def _migrate(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                version INTEGER NOT NULL,
                config_json TEXT NOT NULL,
                actor TEXT,
                updated_minute INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS cursors (
                kind TEXT PRIMARY KEY,
                last_chunk INTEGER NOT NULL DEFAULT -1,
                last_event_id INTEGER NOT NULL DEFAULT 0,
                events_processed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS chunks (
                kind TEXT NOT NULL,
                chunk INTEGER NOT NULL,
                events INTEGER NOT NULL,
                last_event_id INTEGER NOT NULL,
                until_minute INTEGER,
                wall_seconds REAL NOT NULL,
                PRIMARY KEY(kind, chunk)
            );
            CREATE TABLE IF NOT EXISTS people (
                person_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                lang TEXT NOT NULL,
                city_tier TEXT NOT NULL,
                age_band TEXT NOT NULL,
                value_band TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS people_workflow ON people(workflow_id);
            CREATE TABLE IF NOT EXISTS population (
                workflow_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                slice_key TEXT NOT NULL,
                leads INTEGER NOT NULL DEFAULT 0,
                concluded INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(workflow_id, campaign_id, slice_key)
            );
            CREATE TABLE IF NOT EXISTS call_stats (
                kind TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                bucket_minute INTEGER NOT NULL,
                config_version INTEGER NOT NULL,
                cohort_id TEXT NOT NULL,
                slice_key TEXT NOT NULL,
                welcome TEXT NOT NULL,
                voice TEXT NOT NULL,
                caller_id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                dials INTEGER NOT NULL DEFAULT 0,
                connected INTEGER NOT NULL DEFAULT 0,
                engaged INTEGER NOT NULL DEFAULT 0,
                halluc INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(kind, workflow_id, bucket_minute, config_version, cohort_id,
                            slice_key, welcome, voice, caller_id, prompt)
            );
            CREATE INDEX IF NOT EXISTS stats_lookup
                ON call_stats(workflow_id, kind, bucket_minute, slice_key);
            CREATE TABLE IF NOT EXISTS cohorts (
                cohort_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                feature TEXT NOT NULL,
                value TEXT NOT NULL,
                estimated_population INTEGER NOT NULL,
                experiment_id TEXT,
                active_version INTEGER,
                PRIMARY KEY(workflow_id, cohort_id)
            );
            CREATE TABLE IF NOT EXISTS experiments (
                experiment_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                created_minute INTEGER NOT NULL,
                started_minute INTEGER,
                deadline_minute INTEGER,
                applied_version INTEGER,
                baseline_config_json TEXT NOT NULL,
                experiment_config_json TEXT NOT NULL,
                details_json TEXT NOT NULL,
                result_json TEXT,
                updated_minute INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS experiments_workflow ON experiments(workflow_id, state);
            CREATE TABLE IF NOT EXISTS proposals (
                proposal_id TEXT PRIMARY KEY,
                approval_id TEXT,
                workflow_id TEXT NOT NULL,
                experiment_id TEXT,
                action_type TEXT NOT NULL,
                status TEXT NOT NULL,
                base_version INTEGER NOT NULL,
                proposed_config_json TEXT NOT NULL,
                body_json TEXT NOT NULL,
                on_apply_state TEXT,
                created_minute INTEGER NOT NULL,
                updated_minute INTEGER NOT NULL,
                applied_version INTEGER,
                reasons_json TEXT
            );
            CREATE INDEX IF NOT EXISTS proposals_status ON proposals(status, workflow_id);
            CREATE TABLE IF NOT EXISTS incidents (
                incident_key TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                detected_minute INTEGER NOT NULL,
                updated_minute INTEGER NOT NULL,
                details_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS decisions (
                decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sim_minute INTEGER NOT NULL,
                ts TEXT NOT NULL,
                workflow_id TEXT,
                kind TEXT NOT NULL,
                summary TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                action_json TEXT NOT NULL,
                outcome_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS decisions_workflow ON decisions(workflow_id, sim_minute);
            """
        )
        for kind in ("history", "events"):
            self.conn.execute("INSERT OR IGNORE INTO cursors(kind) VALUES (?)", (kind,))

    def reset(self) -> None:
        tables = ("workflows", "cursors", "chunks", "people", "population", "call_stats", "cohorts",
                  "experiments", "proposals", "incidents", "decisions")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for table in tables:
                self.conn.execute(f"DELETE FROM {table}")
            self.conn.execute("DELETE FROM meta")
            for kind in ("history", "events"):
                self.conn.execute("INSERT INTO cursors(kind) VALUES (?)", (kind,))
            self.conn.execute("COMMIT")
            self._feature_cache.clear()
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def get_meta(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return loads(row["value"], default) if row else default

    def set_meta(self, key: str, value) -> None:
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, dumps(value)),
        )

    def prepare_run(self, state: dict) -> None:
        old_scenario = self.get_meta("scenario")
        old_minute = self.get_meta("last_sim_minute", -10**9)
        old_finished = self.get_meta("finished", False)
        fresh_world = (old_scenario not in (None, state["scenario"]) or old_minute > state["sim_minute"]
                       or (old_finished and not state["finished"]))
        if fresh_world:
            self.reset()
        self.set_meta("scenario", state["scenario"])
        self.set_meta("last_sim_minute", state["sim_minute"])
        self.set_meta("finished", state["finished"])
        self.set_meta("credits_remaining", state.get("credits_remaining"))
        if self.get_meta("initial_credits") is None:
            self.set_meta("initial_credits", state.get("credits_remaining"))

    def cursor(self, kind: str) -> dict:
        return dict(self.conn.execute("SELECT * FROM cursors WHERE kind=?", (kind,)).fetchone())

    def upsert_workflow(self, spec: dict, current: dict, minute: int) -> None:
        self.conn.execute(
            """INSERT INTO workflows(workflow_id,tenant_id,spec_json,version,config_json,actor,updated_minute)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(workflow_id) DO UPDATE SET tenant_id=excluded.tenant_id,
                 spec_json=excluded.spec_json, version=excluded.version, config_json=excluded.config_json,
                 actor=excluded.actor, updated_minute=excluded.updated_minute""",
            (spec["workflow_id"], spec["tenant_id"], dumps(spec), current["version"],
             dumps(current["config"]), current.get("actor"), minute),
        )

    def workflow(self, workflow_id: str) -> dict:
        row = self.conn.execute("SELECT * FROM workflows WHERE workflow_id=?", (workflow_id,)).fetchone()
        if not row:
            raise KeyError(workflow_id)
        out = dict(row)
        out["spec"] = loads(out.pop("spec_json"))
        out["config"] = loads(out.pop("config_json"))
        return out

    def workflows(self) -> list[dict]:
        ids = [r[0] for r in self.conn.execute("SELECT workflow_id FROM workflows ORDER BY workflow_id")]
        return [self.workflow(wid) for wid in ids]

    def update_workflow_config(self, workflow_id: str, current: dict, minute: int) -> None:
        self.conn.execute(
            "UPDATE workflows SET version=?,config_json=?,actor=?,updated_minute=? WHERE workflow_id=?",
            (current["version"], dumps(current["config"]), current.get("actor"), minute, workflow_id),
        )

    def _remember_features(self, person_id: str, features: tuple[str, str, str, str]) -> None:
        self._feature_cache[person_id] = features
        self._feature_cache.move_to_end(person_id)
        if len(self._feature_cache) > self._cache_limit:
            self._feature_cache.popitem(last=False)

    def _features(self, person_id: str) -> tuple[str, str, str, str] | None:
        found = self._feature_cache.get(person_id)
        if found:
            self._feature_cache.move_to_end(person_id)
            return found
        row = self.conn.execute(
            "SELECT lang,city_tier,age_band,value_band FROM people WHERE person_id=?", (person_id,)
        ).fetchone()
        if not row:
            return None
        found = tuple(row)
        self._remember_features(person_id, found)
        return found

    @staticmethod
    def _slice_keys(features: tuple[str, str, str, str]) -> tuple[str, ...]:
        return ("all",) + tuple(f"{name}={value}" for name, value in zip(FEATURES, features))

    def ingest_chunk(self, kind: str, meta: dict, lines: Iterable[str]) -> dict:
        if meta["chunk"] <= self.cursor(kind)["last_chunk"]:
            return {"skipped": True, "events": 0, "calls": 0}
        started = time.perf_counter()
        people: list[tuple] = []
        aggregates: dict[tuple, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
        populations: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
        event_count = call_count = 0
        last_event_id = 0
        event_decisions: list[tuple] = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for raw in lines:
                if not raw:
                    continue
                event = json.loads(raw)
                event_count += 1
                last_event_id = max(last_event_id, int(event.get("event_id", 0)))
                etype = event.get("type")
                if etype == "lead":
                    feat = event["features"]
                    values = tuple(str(feat[name]) for name in FEATURES)
                    people.append((event["person_id"], event["workflow_id"], *values))
                    self._remember_features(event["person_id"], values)
                    for slice_key in self._slice_keys(values):
                        populations[(event["workflow_id"], event["campaign_id"], slice_key)][0] += 1
                    if len(people) >= 5000:
                        self.conn.executemany(
                            "INSERT OR IGNORE INTO people VALUES (?,?,?,?,?,?)", people
                        )
                        people.clear()
                elif etype == "call":
                    call_count += 1
                    features = self._features(event["person_id"])
                    # A very large chunk can contain a lead, more than the LRU
                    # capacity of intervening events, and then that person's
                    # call. Flush the pending lead batch before declaring the
                    # join missing; the transaction still commits atomically.
                    if features is None and people:
                        self.conn.executemany("INSERT OR IGNORE INTO people VALUES (?,?,?,?,?,?)", people)
                        people.clear()
                        features = self._features(event["person_id"])
                    if features is None:
                        raise RuntimeError(f"missing features for {event['person_id']}")
                    minute = sim_minute(event["ts"])
                    bucket = minute - minute % 60
                    variant = event["variant"]
                    connected = int(bool(event["connected"]))
                    engaged = int(bool(event["engaged"]))
                    halluc = int(bool((event.get("qa") or {}).get("hallucination_flag")))
                    for slice_key in self._slice_keys(features):
                        key = (kind, event["workflow_id"], bucket, int(event["config_version"]),
                               event.get("cohort_id") or "default", slice_key,
                               variant["welcome_message"], variant["voice"], variant["caller_id"],
                               variant["prompt_version"])
                        counts = aggregates[key]
                        counts[0] += 1
                        counts[1] += connected
                        counts[2] += engaged
                        counts[3] += halluc
                        if engaged:
                            populations[(event["workflow_id"], event["campaign_id"], slice_key)][1] += 1
                elif etype in ("config_changed", "approval_decided") and kind == "events":
                    minute = sim_minute(event["ts"])
                    event_decisions.append((minute, event["ts"], event.get("workflow_id"), etype,
                                            etype.replace("_", " "), dumps(event), "{}", "{}"))
            if people:
                self.conn.executemany("INSERT OR IGNORE INTO people VALUES (?,?,?,?,?,?)", people)
            self.conn.executemany(
                """INSERT INTO population(workflow_id,campaign_id,slice_key,leads,concluded)
                   VALUES (?,?,?,?,?) ON CONFLICT(workflow_id,campaign_id,slice_key) DO UPDATE SET
                   leads=leads+excluded.leads, concluded=concluded+excluded.concluded""",
                ((*key, *counts) for key, counts in populations.items()),
            )
            self.conn.executemany(
                """INSERT INTO call_stats(kind,workflow_id,bucket_minute,config_version,cohort_id,slice_key,
                   welcome,voice,caller_id,prompt,dials,connected,engaged,halluc)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(kind,workflow_id,bucket_minute,config_version,cohort_id,slice_key,
                   welcome,voice,caller_id,prompt) DO UPDATE SET dials=dials+excluded.dials,
                   connected=connected+excluded.connected, engaged=engaged+excluded.engaged,
                   halluc=halluc+excluded.halluc""",
                ((*key, *counts) for key, counts in aggregates.items()),
            )
            if event_decisions:
                self.conn.executemany(
                    """INSERT INTO decisions(sim_minute,ts,workflow_id,kind,summary,evidence_json,
                       action_json,outcome_json) VALUES (?,?,?,?,?,?,?,?)""", event_decisions
                )
            elapsed = time.perf_counter() - started
            self.conn.execute(
                "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                (kind, meta["chunk"], event_count, last_event_id, meta.get("until_minute"), elapsed),
            )
            self.conn.execute(
                """UPDATE cursors SET last_chunk=?,last_event_id=?,events_processed=events_processed+?
                   WHERE kind=?""",
                (meta["chunk"], last_event_id, event_count, kind),
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            # Lead features may have been placed in the LRU before the
            # transaction failed. Clear them so a later chunk cannot observe
            # an uncommitted join that is absent from SQLite.
            self._feature_cache.clear()
            raise
        return {"skipped": False, "events": event_count, "calls": call_count,
                "seconds": time.perf_counter() - started}

    def option_stats(self, workflow_id: str, dimension: str, *, kind: str = "events",
                     since: int | None = None, until: int | None = None,
                     slice_key: str = "all", prompt: str | None = None) -> dict[str, dict[str, int]]:
        column = DIMENSION_COLUMNS[dimension]
        where = ["workflow_id=?", "kind=?", "slice_key=?"]
        args: list[object] = [workflow_id, kind, slice_key]
        if since is not None:
            where.append("bucket_minute>=?")
            args.append(since)
        if until is not None:
            where.append("bucket_minute<?")
            args.append(until)
        if prompt is not None:
            where.append("prompt=?")
            args.append(prompt)
        rows = self.conn.execute(
            f"SELECT {column} option,SUM(dials) dials,SUM(connected) connected,"
            f"SUM(engaged) engaged,SUM(halluc) halluc FROM call_stats WHERE {' AND '.join(where)} "
            f"GROUP BY {column}", args
        ).fetchall()
        return {row["option"]: {k: int(row[k] or 0) for k in ("dials", "connected", "engaged", "halluc")}
                for row in rows}

    def prompt_stats(self, workflow_id: str, prompt: str, *, kind: str, since: int | None = None,
                     until: int | None = None) -> dict[str, int]:
        where = ["workflow_id=?", "kind=?", "slice_key='all'", "prompt=?"]
        args: list[object] = [workflow_id, kind, prompt]
        if since is not None:
            where.append("bucket_minute>=?")
            args.append(since)
        if until is not None:
            where.append("bucket_minute<?")
            args.append(until)
        row = self.conn.execute(
            f"SELECT SUM(dials) dials,SUM(connected) connected,SUM(engaged) engaged,SUM(halluc) halluc "
            f"FROM call_stats WHERE {' AND '.join(where)}", args
        ).fetchone()
        return {k: int(row[k] or 0) for k in ("dials", "connected", "engaged", "halluc")}

    def option_series(self, workflow_id: str, dimension: str, option: str, since: int,
                      *, kind: str = "events", slice_key: str = "all") -> list[dict]:
        column = DIMENSION_COLUMNS[dimension]
        rows = self.conn.execute(
            f"""SELECT bucket_minute,SUM(dials) dials,SUM(connected) connected,SUM(engaged) engaged,
                SUM(halluc) halluc FROM call_stats WHERE workflow_id=? AND kind=?
                AND slice_key=? AND {column}=? AND bucket_minute>=?
                GROUP BY bucket_minute ORDER BY bucket_minute""",
            (workflow_id, kind, slice_key, option, since),
        ).fetchall()
        return [dict(row) for row in rows]

    def prompt_series(self, workflow_id: str, prompt: str, *, kind: str = "events",
                      since: int = 0, slice_key: str = "all") -> list[dict]:
        """Return time buckets for exact recent/reference-window accounting.

        Raw calls are deliberately not retained.  These sufficient statistics
        are enough to construct deterministic 300-observation windows while
        keeping the database bounded to aggregates.
        """
        rows = self.conn.execute(
            """SELECT bucket_minute,SUM(dials) dials,SUM(connected) connected,
                      SUM(engaged) engaged,SUM(halluc) halluc
               FROM call_stats
               WHERE workflow_id=? AND kind=? AND slice_key=? AND prompt=?
                 AND bucket_minute>=?
               GROUP BY bucket_minute ORDER BY bucket_minute""",
            (workflow_id, kind, slice_key, prompt, since),
        ).fetchall()
        return [dict(row) for row in rows]

    def observed_slices(self, workflow_id: str) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT DISTINCT slice_key FROM call_stats WHERE workflow_id=? AND slice_key!='all' ORDER BY slice_key",
            (workflow_id,),
        )]

    def affected_population(self, workflow_id: str, slice_key: str = "all") -> tuple[int, int]:
        row = self.conn.execute(
            "SELECT SUM(leads) leads,SUM(concluded) concluded FROM population WHERE workflow_id=? AND slice_key=?",
            (workflow_id, slice_key),
        ).fetchone()
        leads, concluded = int(row["leads"] or 0), int(row["concluded"] or 0)
        return leads, max(leads - concluded, 0)

    def activate_cohort(self, workflow_id: str, cohort_id: str, version: int) -> None:
        self.conn.execute("UPDATE cohorts SET active_version=? WHERE workflow_id=? AND cohort_id=?",
                          (version, workflow_id, cohort_id))

    def add_decision(self, minute: int, ts: str, workflow_id: str | None, kind: str, summary: str,
                     evidence=None, action=None, outcome=None) -> int:
        cur = self.conn.execute(
            """INSERT INTO decisions(sim_minute,ts,workflow_id,kind,summary,evidence_json,action_json,outcome_json)
               VALUES (?,?,?,?,?,?,?,?)""",
            (minute, ts, workflow_id, kind, summary, dumps(evidence or {}), dumps(action or {}), dumps(outcome or {})),
        )
        return int(cur.lastrowid)

    def decisions(self, workflow_id: str | None = None, at_minute: int | None = None,
                  limit: int = 50) -> list[dict]:
        where, args = [], []
        if workflow_id:
            where.append("workflow_id=?")
            args.append(workflow_id)
        if at_minute is not None:
            where.append("sim_minute<=?")
            args.append(at_minute)
        sql = "SELECT * FROM decisions" + (" WHERE " + " AND ".join(where) if where else "")
        sql += " ORDER BY sim_minute DESC,decision_id DESC LIMIT ?"
        args.append(limit)
        out = []
        for row in self.conn.execute(sql, args):
            item = dict(row)
            for key in ("evidence_json", "action_json", "outcome_json"):
                item[key.removesuffix("_json")] = loads(item.pop(key), {})
            out.append(item)
        return out

    def create_experiment(self, item: dict) -> None:
        self.conn.execute(
            """INSERT INTO experiments(experiment_id,workflow_id,kind,state,created_minute,started_minute,
               deadline_minute,applied_version,baseline_config_json,experiment_config_json,details_json,
               result_json,updated_minute) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item["experiment_id"], item["workflow_id"], item["kind"], item["state"], item["created_minute"],
             item.get("started_minute"), item.get("deadline_minute"), item.get("applied_version"),
             dumps(item["baseline_config"]), dumps(item["experiment_config"]), dumps(item.get("details", {})),
             dumps(item["result"]) if item.get("result") is not None else None, item["updated_minute"]),
        )

    def experiments(self, states: tuple[str, ...] | None = None, workflow_id: str | None = None) -> list[dict]:
        where, args = [], []
        if states:
            where.append("state IN (" + ",".join("?" for _ in states) + ")")
            args.extend(states)
        if workflow_id:
            where.append("workflow_id=?")
            args.append(workflow_id)
        rows = self.conn.execute("SELECT * FROM experiments" +
                                 (" WHERE " + " AND ".join(where) if where else "") +
                                 " ORDER BY created_minute", args).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            for key in ("baseline_config_json", "experiment_config_json", "details_json", "result_json"):
                item[key.removesuffix("_json")] = loads(item.pop(key), {} if key != "result_json" else None)
            out.append(item)
        return out

    def update_experiment(self, experiment_id: str, minute: int, **changes) -> None:
        allowed = {"state", "started_minute", "deadline_minute", "applied_version"}
        fields, args = [], []
        for key, value in changes.items():
            if key in ("details", "result"):
                fields.append(f"{key}_json=?")
                args.append(dumps(value))
            elif key in allowed:
                fields.append(f"{key}=?")
                args.append(value)
            else:
                raise KeyError(key)
        fields.append("updated_minute=?")
        args.extend([minute, experiment_id])
        self.conn.execute(f"UPDATE experiments SET {','.join(fields)} WHERE experiment_id=?", args)

    def create_proposal(self, item: dict) -> None:
        self.conn.execute(
            """INSERT INTO proposals(proposal_id,approval_id,workflow_id,experiment_id,action_type,status,
               base_version,proposed_config_json,body_json,on_apply_state,created_minute,updated_minute,
               applied_version,reasons_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item["proposal_id"], item.get("approval_id"), item["workflow_id"], item.get("experiment_id"),
             item["action_type"], item["status"], item["base_version"], dumps(item["proposed_config"]),
             dumps(item["body"]), item.get("on_apply_state"), item["created_minute"], item["updated_minute"],
             item.get("applied_version"), dumps(item.get("reasons", []))),
        )

    def proposals(self, statuses: tuple[str, ...] | None = None) -> list[dict]:
        args: list[object] = []
        sql = "SELECT * FROM proposals"
        if statuses:
            sql += " WHERE status IN (" + ",".join("?" for _ in statuses) + ")"
            args.extend(statuses)
        sql += " ORDER BY created_minute"
        out = []
        for row in self.conn.execute(sql, args):
            item = dict(row)
            for key in ("proposed_config_json", "body_json", "reasons_json"):
                item[key.removesuffix("_json")] = loads(item.pop(key), [] if key == "reasons_json" else {})
            out.append(item)
        return out

    def update_proposal(self, proposal_id: str, minute: int, **changes) -> None:
        allowed = {"approval_id", "status", "applied_version", "on_apply_state", "base_version"}
        fields, args = [], []
        for key, value in changes.items():
            if key in ("reasons", "body", "proposed_config"):
                column = "reasons_json" if key == "reasons" else f"{key}_json"
                fields.append(f"{column}=?")
                args.append(dumps(value))
            elif key in allowed:
                fields.append(f"{key}=?")
                args.append(value)
            else:
                raise KeyError(key)
        fields.append("updated_minute=?")
        args.extend([minute, proposal_id])
        self.conn.execute(f"UPDATE proposals SET {','.join(fields)} WHERE proposal_id=?", args)

    def upsert_incident(self, key: str, workflow_id: str, kind: str, status: str, minute: int, details: dict) -> None:
        self.conn.execute(
            """INSERT INTO incidents VALUES (?,?,?,?,?,?,?) ON CONFLICT(incident_key) DO UPDATE SET
               status=excluded.status,updated_minute=excluded.updated_minute,details_json=excluded.details_json""",
            (key, workflow_id, kind, status, minute, minute, dumps(details)),
        )

    def incidents(self, statuses: tuple[str, ...] | None = None) -> list[dict]:
        sql, args = "SELECT * FROM incidents", []
        if statuses:
            sql += " WHERE status IN (" + ",".join("?" for _ in statuses) + ")"
            args.extend(statuses)
        rows = self.conn.execute(sql + " ORDER BY detected_minute", args).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["details"] = loads(item.pop("details_json"), {})
            out.append(item)
        return out

    def status_summary(self) -> dict:
        cursors = {row["kind"]: dict(row) for row in self.conn.execute("SELECT * FROM cursors")}
        active = [dict(row) for row in self.conn.execute(
            "SELECT experiment_id,workflow_id,kind,state,started_minute,deadline_minute FROM experiments "
            "WHERE state NOT IN ('no_winner','aborted_harm','rolled_back','complete','stale') ORDER BY workflow_id"
        )]
        pending = [dict(row) for row in self.conn.execute(
            "SELECT proposal_id,approval_id,workflow_id,action_type,status FROM proposals "
            "WHERE status IN ('queued','pending','approved') ORDER BY workflow_id"
        )]
        incidents = [dict(row) for row in self.conn.execute(
            "SELECT incident_key,workflow_id,kind,status,detected_minute FROM incidents "
            "WHERE status IN ('detected','proposed','monitoring','blocked','failed')"
        )]
        winners = []
        for row in self.conn.execute(
            "SELECT experiment_id,workflow_id,kind,state,result_json,details_json FROM experiments "
            "WHERE state IN ('promoted','rolled_back') ORDER BY workflow_id"
        ):
            item = dict(row)
            result = loads(item.pop("result_json"), {})
            details = loads(item.pop("details_json"), {})
            item["winner"] = result.get("chosen", [])
            item["evidence"] = {"evaluated_at": result.get("evaluated_at"),
                                "fixed_horizon_ready": result.get("ready"),
                                "hypotheses_tested": len(result.get("hypotheses", []))}
            item["monitoring"] = {key: details.get(key) for key in
                                  ("promoted_minute", "decay_checks", "decay_last_observations",
                                   "decay_last", "rollback_queued") if key in details}
            winners.append(item)
        last_minute = int(self.get_meta("last_sim_minute", 0) or 0)
        day_start = last_minute - last_minute % 1440
        changes_today = [dict(row) for row in self.conn.execute(
            "SELECT proposal_id,workflow_id,action_type,status,applied_version,updated_minute "
            "FROM proposals WHERE status='applied' AND updated_minute>=? ORDER BY updated_minute", (day_start,)
        )]
        decision_count = self.conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE sim_minute>=? "
            "AND kind NOT IN ('config_changed','approval_decided')", (max(0, last_minute - 1440),)
        ).fetchone()[0]
        event_until = self.conn.execute(
            "SELECT MAX(until_minute) FROM chunks WHERE kind='events'"
        ).fetchone()[0]
        perf = self.conn.execute("SELECT SUM(events) events,SUM(wall_seconds) seconds FROM chunks").fetchone()
        events, seconds = int(perf["events"] or 0), float(perf["seconds"] or 0)
        database_bytes = sum(path.stat().st_size for path in
                             (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm")) if path.exists())
        initial_credits = self.get_meta("initial_credits")
        remaining_credits = self.get_meta("credits_remaining")
        credits_used = (None if initial_credits is None or remaining_credits is None
                        else round(float(initial_credits) - float(remaining_credits), 2))
        processed_event_id = int(cursors["events"]["last_event_id"] or 0)
        simulator_event_id = int(self.get_meta("simulator_last_event_id", processed_event_id) or 0)
        lag_events = max(0, simulator_event_id - processed_event_id)
        return {"scenario": self.get_meta("scenario"), "last_sim_minute": last_minute,
                "finished": self.get_meta("finished", False), "credits_remaining": remaining_credits,
                "credits_used": credits_used,
                "cursors": cursors, "events_per_second": round(events / seconds, 1) if seconds else None,
                "ingestion_lag_events": lag_events,
                "ingestion_lag_minutes": (max(0, last_minute - int(event_until or 0)) if lag_events else 0),
                "decisions_per_hour": round(decision_count / max(min(last_minute, 1440) / 60.0, 1.0), 2),
                "database_bytes": database_bytes, "active_experiments": active,
                "current_winners": winners, "pending_proposals": pending,
                "open_incidents": incidents, "changes_today": changes_today}
