"""Full failure identity and conservative legacy-ack compatibility (#1597)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def store(tmp_path, monkeypatch):
    from cron import executions, incidents

    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", home / "cron" / "executions.db")
    monkeypatch.setattr(incidents, "EXECUTIONS_FILE", home / "cron" / "executions.db")
    return incidents, executions, home


def _seed_legacy(incidents, job_id, error, state="closed"):
    """Real old-format row, not a signature produced by the implementation."""
    normalized = re.sub(r"\s+", " ", error).strip().lower()[:200]
    signature = hashlib.sha256((job_id + normalized).encode()).hexdigest()[:12]
    incident_id = f"{job_id[:6]}_{signature}"
    timestamp = "2026-09-27T00:00:00+00:00"
    with incidents._transaction() as conn:
        conn.execute(
            "INSERT INTO cron_incidents "
            "(id, job_id, error_sig, state, failure_type, first_seen_at, "
            "last_seen_at, acked_at, closed_at, error, output_file) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (incident_id, job_id, signature, state, "script", timestamp,
             timestamp, timestamp if state == "closed" else None,
             timestamp if state == "closed" else None, error[:500],
             "legacy-output.txt"),
        )
    return incidents.get_incident(incident_id)


@pytest.mark.parametrize("prefix_length", [199, 200, 201, 500, 2000])
def test_changed_suffix_never_inherits_acknowledgement(store, prefix_length):
    incidents, _, _ = store
    prefix = "x" * prefix_length
    old_id, _ = incidents.upsert_incident("job-1", prefix + "; severity=warning")
    assert incidents.ack_incident(old_id)
    new_id, is_new = incidents.upsert_incident("job-1", prefix + "; severity=critical")
    assert new_id != old_id
    assert is_new
    assert incidents.get_incident(new_id)["state"] == "detected"
    assert incidents.get_incident(old_id)["state"] == "closed"


def test_signature_precedes_display_bound_and_redaction(store):
    incidents, _, _ = store
    prefix = "stable context " * 50
    old_id, _ = incidents.upsert_incident("job-1", prefix + "entry held")
    assert incidents.ack_incident(old_id)
    new_id, _ = incidents.upsert_incident("job-1", prefix + "protective exit unavailable")
    old = incidents.get_incident(old_id)
    new = incidents.get_incident(new_id)
    assert old["error"] == new["error"]
    assert len(new["error"]) <= incidents.MAX_ERROR_CHARS
    assert old_id != new_id
    assert new["state"] == "detected"


def test_job_and_error_are_unambiguously_framed(store):
    incidents, _, _ = store
    first, _ = incidents.upsert_incident("shareda", "b failure")
    assert incidents.ack_incident(first)
    second, is_new = incidents.upsert_incident("shared", "ab failure")
    assert first != second
    assert is_new
    assert incidents.get_incident(first)["job_id"] == "shareda"
    assert incidents.get_incident(second)["job_id"] == "shared"
    assert incidents.get_incident(second)["state"] == "detected"


def test_case_and_whitespace_normalization_preserve_current_ack_contract(store):
    incidents, _, _ = store
    first, _ = incidents.upsert_incident("job-1", "Same failure\n  details")
    assert incidents.ack_incident(first)
    second, is_new = incidents.upsert_incident("job-1", " SAME FAILURE details  ")
    assert second == first
    assert not is_new
    assert incidents.get_incident(second)["state"] == "closed"


def test_unicode_suffix_is_not_discarded(store):
    incidents, _, _ = store
    prefix = "é🛑" * 300
    old, _ = incidents.upsert_incident("job-1", prefix + "one")
    assert incidents.ack_incident(old)
    new, is_new = incidents.upsert_incident("job-1", prefix + "two")
    assert is_new and new != old


def test_identity_retains_full_sha256_strength(store):
    incidents, _, _ = store
    incident_id, _ = incidents.upsert_incident("job-1", "failure")
    signature = incidents.get_incident(incident_id)["error_sig"]
    digest = signature.rsplit("_", 1)[-1]
    assert re.fullmatch(r"[0-9a-f]+", digest)
    assert len(digest) == hashlib.sha256().digest_size * 2


@pytest.mark.parametrize("state", ["detected", "alerted", "closed"])
@pytest.mark.parametrize("prefix_length", [0, 700], ids=["short", "long"])
def test_legacy_rows_remain_history_and_do_not_supply_new_ack(
    store, state, prefix_length
):
    incidents, _, _ = store
    error = "x" * prefix_length + "same failure"
    old = _seed_legacy(incidents, "job-1", error, state)
    new_id, is_new = incidents.upsert_incident("job-1", error)
    assert is_new and new_id != old["id"]
    assert incidents.get_incident(old["id"]) == old
    assert incidents.get_incident(new_id)["state"] == "detected"
    assert incidents.count_incidents() == 2
    assert incidents.ack_incident(new_id)
    same_id, is_new = incidents.upsert_incident("job-1", error)
    assert same_id == new_id and not is_new
    assert incidents.get_incident(new_id)["state"] == "closed"
    assert incidents.get_incident(old["id"]) == old


def test_cli_lists_and_acknowledges_new_and_legacy_ids(store, capsys):
    incidents, _, _ = store
    from hermes_cli.cron import cron_incidents

    legacy = _seed_legacy(incidents, "job-1", "old failure", "detected")
    new_id, _ = incidents.upsert_incident("job-1", "new failure")
    assert cron_incidents(argparse.Namespace(incident_action="list", state=None)) == 0
    output = capsys.readouterr().out
    assert new_id in output and legacy["id"] in output
    for incident_id in (legacy["id"], new_id):
        args = argparse.Namespace(incident_action="ack", incident_id=incident_id)
        assert cron_incidents(args) == 0
        assert incidents.get_incident(incident_id)["state"] == "closed"


def test_fresh_process_preserves_full_signature_ack_and_new_suffix_alert(store):
    incidents, _, home = store
    error = "stable context " * 50 + "entry held"
    old_id, _ = incidents.upsert_incident("job-1", error)
    assert incidents.ack_incident(old_id)
    code = (
        "import json, sys; from cron import incidents; "
        "same, fresh = incidents.upsert_incident('job-1', sys.argv[1]); "
        "changed, changed_fresh = incidents.upsert_incident('job-1', sys.argv[2]); "
        "print(json.dumps([same, fresh, incidents.get_incident(same)['state'], "
        "changed, changed_fresh, incidents.get_incident(changed)['state']]))"
    )
    # Explicit nonsecret child environment, never inherited gateway credentials.
    child_env = {
        "PATH": os.environ.get("PATH", ""), "HERMES_HOME": str(home),
        "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
        "TZ": "UTC", "LANG": "C.UTF-8", "PYTHONHASHSEED": "0",
    }
    for name in ("SYSTEMROOT", "USERPROFILE", "TEMP", "TMP"):
        if name in os.environ:
            child_env[name] = os.environ[name]
    child = subprocess.run(
        [sys.executable, "-c", code, error, "stable context " * 50 + "exit unavailable"],
        env=child_env, capture_output=True, text=True, check=True, timeout=20,
    )
    same, fresh, state, changed, changed_fresh, changed_state = json.loads(child.stdout)
    assert same == old_id and not fresh and state == "closed"
    assert changed != old_id and changed_fresh and changed_state == "detected"


def test_real_no_agent_failure_with_changed_suffix_alerts_and_stays_failed(
    store, monkeypatch
):
    incidents, executions, home = store
    from cron import jobs, scheduler

    script = home / "scripts" / "probe.py"
    script.parent.mkdir()
    deliveries = []
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: home)
    monkeypatch.setattr(
        scheduler, "_deliver_result",
        lambda job, content, **kwargs: deliveries.append(content),
    )
    monkeypatch.setattr("hermes_cli.env_loader.load_hermes_dotenv", lambda **kwargs: [])
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt=None, schedule="every 1m", script=str(script),
            no_agent=True, deliver="telegram", name="offline signature probe",
        )
        prefix = "monitor diagnostic context " * 30

        def run(suffix):
            script.write_text(
                f"import sys\nprint({(prefix + suffix)!r})\nsys.exit(1)\n",
                encoding="utf-8",
            )
            assert scheduler.run_one_job(jobs.get_job(job["id"]))
            assert jobs.get_job(job["id"])["last_status"] == "error"

        run("severity=warning; entry held")
        old = incidents.list_incidents()[0]
        assert incidents.ack_incident(old["id"])
        run("severity=critical; protective exits unavailable")
        assert len(deliveries) == 2
        new = next(row for row in incidents.list_incidents() if row["id"] != old["id"])
        assert new["state"] == "alerted"
        assert "protective exits unavailable" in Path(new["output_file"]).read_text()
        assert incidents.ack_incident(new["id"])
        run("severity=critical; protective exits unavailable")
        assert len(deliveries) == 2
        rows = executions.list_executions(job_id=job["id"])
        assert len(rows) == 3
        assert all(row["status"] == "failed" for row in rows)


def test_outer_exception_changed_suffix_uses_same_signature_guard(store, monkeypatch):
    incidents, executions, home = store
    from cron import jobs, scheduler

    deliveries = []
    message = {"text": "stable context " * 50 + "warning"}

    def crash(*args, **kwargs):
        raise RuntimeError(message["text"])

    monkeypatch.setattr(scheduler, "run_job", crash)
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: home)
    monkeypatch.setattr(
        scheduler, "_deliver_result",
        lambda job, content, **kwargs: deliveries.append(content),
    )
    with jobs.use_cron_store(home):
        job = jobs.create_job(prompt="offline exception fixture", schedule="every 1m")
        scheduler.run_one_job(jobs.get_job(job["id"]))
        assert incidents.ack_incident(incidents.list_incidents()[0]["id"])
        message["text"] = "stable context " * 50 + "critical"
        scheduler.run_one_job(jobs.get_job(job["id"]))
        assert len(deliveries) == 2
        assert incidents.count_incidents() == 2
        assert jobs.get_job(job["id"])["last_status"] == "error"
        assert all(row["status"] == "failed" for row in executions.list_executions(job_id=job["id"]))
