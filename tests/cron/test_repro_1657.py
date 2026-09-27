"""End-to-end restart reproduction for issue #1657's lost fire claims."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from unittest.mock import patch

import pytest


@pytest.mark.skipif(os.name == "nt", reason="requires POSIX SIGKILL semantics")
@pytest.mark.parametrize("identity", ["stable", "production-no-identity"])
@pytest.mark.parametrize("started", [False, True], ids=["pre-start", "running"])
def test_restart_reaps_killed_owner_and_releases_recurring_fire_claim(
    identity, started, monkeypatch, tmp_path
):
    """Every reviewed restart-table row must run again or be marked unknown."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    if identity == "stable":
        monkeypatch.setenv("HERMES_MACHINE_ID", "hermes-prod-boston-01")
    else:
        monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
        import socket

        monkeypatch.setattr(socket, "gethostname", lambda: "0c931814b3f4")

    from cron import scheduler
    from cron.jobs import claim_job_for_fire, create_job, get_job

    job = create_job(
        prompt="restart reproduction",
        schedule="every 1m",
        name=f"1657-{identity}-{started}",
    )
    child_code = r"""
import os, signal, sys
if os.environ.get('REPRO_NO_IDENTITY') == '1':
    import socket
    socket.gethostname = lambda: '0c931814b3f4'
from cron.executions import create_execution, mark_execution_running
from cron.jobs import claim_job_for_fire
job_id = sys.argv[1]
execution = create_execution(job_id, source='builtin')
claim = claim_job_for_fire(job_id, return_job=True, execution_id=execution['id'])
assert claim and claim['fire_claim']['execution_id'] == execution['id']
if os.environ.get('REPRO_STARTED') == '1':
    assert mark_execution_running(execution['id']) is not None
print(execution['id'], flush=True)
os.kill(os.getpid(), signal.SIGKILL)
"""
    child_env = os.environ.copy()
    child_env["HERMES_HOME"] = str(tmp_path / "home")
    if identity == "stable":
        child_env["HERMES_MACHINE_ID"] = "hermes-prod-boston-01"
        child_env.pop("REPRO_NO_IDENTITY", None)
    else:
        child_env.pop("HERMES_MACHINE_ID", None)
        child_env["REPRO_NO_IDENTITY"] = "1"
    if started:
        child_env["REPRO_STARTED"] = "1"
    else:
        child_env.pop("REPRO_STARTED", None)
    proc = subprocess.run(
        [sys.executable, "-c", child_code, job["id"]],
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == -signal.SIGKILL, proc.stderr
    execution_id = proc.stdout.strip()
    assert execution_id, proc.stderr

    import cron.executions as executions

    if identity == "production-no-identity":
        monkeypatch.setattr(executions.socket, "gethostname", lambda: "0c931814b3f4")
    monkeypatch.setattr(scheduler, "_last_dead_owner_reap_at", None)
    with (
        patch.object(scheduler, "get_due_jobs", return_value=[]),
        patch("tools.mcp_tool._kill_orphaned_mcp_children", lambda: None),
    ):
        scheduler.tick(verbose=False)

    recovered = next(
        row for row in executions.list_executions(job_id=job["id"])
        if row["id"] == execution_id
    )
    assert recovered["status"] == "unknown"
    assert get_job(job["id"])["fire_claim"] is None
    retry_execution = executions.create_execution(job["id"], source="builtin")
    retry = claim_job_for_fire(
        job["id"], return_job=True, execution_id=retry_execution["id"]
    )
    assert retry["fire_claim"]["execution_id"] == retry_execution["id"]
