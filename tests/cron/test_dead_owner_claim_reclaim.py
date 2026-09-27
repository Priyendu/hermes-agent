"""Dead-owner cron claim reclaim + one-shot CLI `cron run` sync gate (#86721).

A one-shot ``hermes cron run <job_id>`` used to background-dispatch the run
onto a daemon thread of the calling process when the CLI inherited a
gateway/desktop session env. The process exited immediately, the runner died
mid-LLM-call, and the job's execution row stayed ``claimed`` forever —
blocking every future run.

Two-part fix under test here:

1. ``hermes_cli.cron._job_action("run", ...)`` declares the channel stateless
   before invoking the cron API, so the background-dispatch path is gated off
   and the run executes synchronously to completion in the CLI process.
2. ``cron.scheduler.tick`` periodically reaps execution rows whose owner
   process is provably dead (``recover_interrupted_executions``), so a stale
   ``claimed`` row from a crashed/exited owner auto-clears without a gateway
   restart.
"""

from __future__ import annotations

import subprocess
import sys
import time
from unittest.mock import patch

import pytest

import cron.scheduler as scheduler_mod


@pytest.fixture()
def executions(monkeypatch, tmp_path):
    import cron.executions as executions_mod

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
    monkeypatch.setattr(
        executions_mod, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db"
    )
    return executions_mod


@pytest.fixture(autouse=True)
def _fresh_reap_window(monkeypatch):
    """Each test starts with the reap throttle open."""
    monkeypatch.setattr(scheduler_mod, "_last_dead_owner_reap_at", None)


def _dead_pid() -> int:
    """PID of a real process that has already exited."""
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(proc.stdout.strip())


def _orphan_claimed_row(executions, job_id: str) -> str:
    """Persist a claimed execution owned by a process that no longer exists.

    Mirrors what a one-shot ``hermes cron run`` leaves behind: a row stuck in
    ``claimed`` whose owner pid is dead.
    """
    record = executions.create_execution(job_id, source="direct")
    with executions._transaction() as conn:
        conn.execute(
            "UPDATE executions SET process_id='dead-cli-process', pid=?, "
            "process_started_at=NULL WHERE id=?",
            (_dead_pid(), record["id"]),
        )
    return record["id"]


def _run_tick():
    with (
        patch.object(scheduler_mod, "get_due_jobs", return_value=[]),
        patch("tools.mcp_tool._kill_orphaned_mcp_children", lambda: None),
    ):
        return scheduler_mod.tick(verbose=False)


class TestTickReapsDeadOwnerClaims:
    def test_stale_claimed_row_from_dead_owner_is_cleared_by_tick(self, executions):
        """The exact #86721 wedge: dead-owner 'claimed' row unblocks on tick."""
        execution_id = _orphan_claimed_row(executions, "orphaned-job")

        assert _run_tick() == 0

        record = executions.latest_execution("orphaned-job")
        assert record["id"] == execution_id
        assert record["status"] == "unknown"
        assert record["finished_at"]

    def test_running_row_from_dead_owner_is_also_reclaimed(self, executions):
        record = executions.create_execution("orphaned-running", source="direct")
        executions.mark_execution_running(record["id"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-cli-process', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        _run_tick()

        assert executions.latest_execution("orphaned-running")["status"] == "unknown"

    def test_recovery_clears_matching_not_started_fire_claim_and_allows_next_fire(
        self, executions
    ):
        """A dead pre-start owner must not leave the next gateway behind the TTL."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        job = create_job(prompt="x", schedule="every 1m", name="restart-fire-claim")
        record = executions.create_execution(job["id"], source="builtin")
        claimed = claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        assert claimed["fire_claim"]["execution_id"] == record["id"]

        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-gateway', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        assert executions.latest_execution(job["id"])["status"] == "unknown"
        assert get_job(job["id"])["fire_claim"] is None

        retry_execution = executions.create_execution(job["id"], source="builtin")
        retry_claim = claim_job_for_fire(
            job["id"], return_job=True, execution_id=retry_execution["id"]
        )
        assert retry_claim["fire_claim"]["execution_id"] == retry_execution["id"]

    def test_recovery_clears_execution_bound_oneshot_run_claim(self, executions):
        """A dead pre-start one-shot must not wait out its full run-claim TTL."""
        import cron.jobs as jobs

        job = jobs.create_job(prompt="x", schedule="in 30m", name="orphaned-once")
        record = executions.create_execution(job["id"], source="builtin")
        records = jobs.load_jobs()
        persisted = next(row for row in records if row["id"] == job["id"])
        persisted["run_claim"] = {
            "at": "2026-09-25T12:00:00+00:00",
            "by": "one-shot-runner",
        }
        jobs.save_jobs(records)
        assert jobs.link_run_claim_to_execution(
            job["id"], execution_id=record["id"],
            expected_owner="one-shot-runner",
            expected_at="2026-09-25T12:00:00+00:00",
        )
        claimed = jobs.claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        assert claimed["fire_claim"]["execution_id"] == record["id"]

        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-gateway', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        recovered = jobs.get_job(job["id"])
        assert recovered["fire_claim"] is None
        assert recovered["run_claim"] is None

    def test_recovery_clears_legacy_oneshot_claim_inside_bounded_time_window(
        self, executions
    ):
        """M6: exercise the no-execution-ID legacy run-claim time window."""
        from datetime import datetime, timedelta
        import cron.jobs as jobs

        job = jobs.create_job(
            prompt="x", schedule="in 30m", name="legacy-once-window"
        )
        record = executions.create_execution(job["id"], source="builtin")
        with executions._transaction() as conn:
            claimed_at = conn.execute(
                "SELECT claimed_at FROM executions WHERE id=?",
                (record["id"],),
            ).fetchone()["claimed_at"]
        legacy_at = (
            datetime.fromisoformat(claimed_at) - timedelta(seconds=5)
        ).isoformat()
        all_jobs = jobs.load_jobs()
        persisted = next(row for row in all_jobs if row["id"] == job["id"])
        persisted["run_claim"] = {"at": legacy_at, "by": "legacy-runner"}
        jobs.save_jobs(all_jobs)
        jobs.claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )

        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-once', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        recovered = jobs.get_job(job["id"])
        assert recovered["fire_claim"] is None
        assert recovered["run_claim"] is None

    def test_recovery_preserves_replacement_oneshot_claim_execution_id(
        self, executions
    ):
        """M9: recovery must not clear a replacement one-shot execution."""
        import cron.jobs as jobs

        job = jobs.create_job(
            prompt="x", schedule="in 30m", name="replacement-once-run-claim"
        )
        dead = executions.create_execution(job["id"], source="builtin")
        replacement = executions.create_execution(job["id"], source="builtin")
        executions.mark_execution_running(replacement["id"])
        replacement_fire_claim = {
            "at": "2026-09-27T00:00:00+00:00",
            "by": "replacement-dispatcher",
            "execution_id": replacement["id"],
        }
        replacement_run_claim = dict(replacement_fire_claim)
        all_jobs = jobs.load_jobs()
        persisted = next(row for row in all_jobs if row["id"] == job["id"])
        persisted["fire_claim"] = replacement_fire_claim
        persisted["run_claim"] = replacement_run_claim
        jobs.save_jobs(all_jobs)
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-old-once', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), dead["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        recovered = jobs.get_job(job["id"])
        assert recovered["fire_claim"] == replacement_fire_claim
        assert recovered["run_claim"] == replacement_run_claim
        replacement_row = next(
            row for row in executions.list_executions(job_id=job["id"])
            if row["id"] == replacement["id"]
        )
        assert replacement_row["status"] == "running"

    def test_recovery_clears_matching_legacy_claim_only_without_live_owner(
        self, executions
    ):
        """Pre-upgrade claims can be correlated by time only absent any live row."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        job = create_job(prompt="x", schedule="every 1m", name="legacy-fire-claim")
        record = executions.create_execution(job["id"], source="builtin")
        assert claim_job_for_fire(job["id"], return_job=True)
        assert "execution_id" not in get_job(job["id"])["fire_claim"]
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-gateway', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        assert get_job(job["id"])["fire_claim"] is None

    def test_recovery_reconciles_claim_before_retention_prunes_execution(
        self, executions, monkeypatch
    ):
        """A newly recovered row must be captured before terminal-row pruning."""
        import cron.executions as ledger
        import cron.jobs as jobs

        job = jobs.create_job(
            prompt="x", schedule="every 1m", name="prune-after-recovery"
        )
        record = executions.create_execution(job["id"], source="builtin")
        jobs.claim_job_for_fire(job["id"], return_job=True, execution_id=record["id"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-prune-owner', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )
            for index in range(2):
                conn.execute(
                    """INSERT INTO executions
                       (id, job_id, source, process_id, owner_host_id, pid,
                        process_started_at, status, claimed_at, finished_at, error)
                       VALUES (?, ?, 'test', 'terminal', NULL, 1, NULL,
                               'completed', '2099-01-01T00:00:00+00:00',
                               '2099-01-01T00:00:00+00:00', NULL)""",
                    (f"newer-terminal-{index}", job["id"]),
                )
        monkeypatch.setattr(ledger, "MAX_TERMINAL_EXECUTIONS", 1)

        assert executions.recover_interrupted_executions() == 1
        assert jobs.get_job(job["id"])["fire_claim"] is None
        with executions._transaction() as conn:
            assert conn.execute(
                "SELECT 1 FROM executions WHERE id=?", (record["id"],)
            ).fetchone() is None

    def test_recovery_does_not_rescan_expired_unknown_history(self, executions):
        """Old unknown rows must not force a jobs.json read on every reaper."""
        record = executions.create_execution("old-unknown", source="builtin")
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET status='unknown', finished_at=?, error=? "
                "WHERE id=?",
                (
                    "2000-01-01T00:00:00+00:00",
                    "Scheduler restarted after this execution's owner exited.",
                    record["id"],
                ),
            )

        with patch("cron.jobs.clear_recovered_fire_claim") as clear_claim:
            assert executions.recover_interrupted_executions() == 0
        clear_claim.assert_not_called()

    def test_recovery_clears_fire_claim_for_started_dead_execution(
        self, executions
    ):
        """A dead recurring owner releases its lease even after side effects began."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        job = create_job(prompt="x", schedule="every 1m", name="started-fire-claim")
        record = executions.create_execution(job["id"], source="builtin")
        executions.mark_execution_running(record["id"])
        claimed = claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        claim_before = dict(claimed["fire_claim"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-gateway', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        assert executions.latest_execution(job["id"])["status"] == "unknown"
        assert get_job(job["id"])["fire_claim"] is None
        retry_execution = executions.create_execution(job["id"], source="builtin")
        retry = claim_job_for_fire(
            job["id"], return_job=True, execution_id=retry_execution["id"]
        )
        assert retry["fire_claim"]["execution_id"] != claim_before["execution_id"]

    def test_recovery_never_treats_a_remote_execution_pid_as_dead(self, executions):
        """A live remote owner is not disproved by a PID lookup on this host."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        job = create_job(prompt="x", schedule="every 1m", name="remote-fire-claim")
        record = executions.create_execution(job["id"], source="external")
        claimed = claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        claim_before = dict(claimed["fire_claim"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET owner_host_id='remote-host', "
                "process_id='remote-process', pid=?, process_started_at=NULL "
                "WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 0
        assert executions.latest_execution(job["id"])["status"] == "claimed"
        assert get_job(job["id"])["fire_claim"] == claim_before

    def test_missing_owner_host_id_keeps_legacy_pid_recovery_when_both_unidentified(
        self, executions, monkeypatch
    ):
        """The historical single-namespace, no-identity case still recovers."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        monkeypatch.setattr(executions, "_owner_host_id", lambda: None)
        job = create_job(prompt="x", schedule="every 1m", name="unknown-host-claim")
        record = executions.create_execution(job["id"], source="builtin")
        claimed = claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        claim_before = dict(claimed["fire_claim"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET owner_host_id=NULL, process_id=?, pid=?, "
                "process_started_at=NULL WHERE id=?",
                ("legacy-owner", _dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        assert executions.latest_execution(job["id"])["status"] == "unknown"
        assert get_job(job["id"])["fire_claim"] is None

    def test_missing_owner_host_id_is_preserved_by_identified_dashboard(
        self, executions, monkeypatch
    ):
        """A local PID miss cannot prove a migrated gateway owner is dead."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        monkeypatch.setattr(
            executions, "_owner_host_id", lambda: "hermes-dashboard"
        )
        job = create_job(prompt="x", schedule="every 1m", name="legacy-gateway")
        record = executions.create_execution(job["id"], source="builtin")
        claimed = claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        claim_before = dict(claimed["fire_claim"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET owner_host_id=NULL, process_id=?, pid=?, "
                "process_started_at=NULL WHERE id=?",
                ("legacy-gateway", _dead_pid(), record["id"]),
            )

        with patch("gateway.status._pid_exists", return_value=False):
            assert executions.recover_interrupted_executions() == 0

        assert executions.latest_execution(job["id"])["status"] == "claimed"
        assert get_job(job["id"])["fire_claim"] == claim_before

    def test_production_no_identity_ephemeral_hostname_reclaims_dead_owner(
        self, executions, monkeypatch
    ):
        """M4: production's Docker hostname/no machine ID still recovers by PID."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
        monkeypatch.setattr(executions.socket, "gethostname", lambda: "0c931814b3f4")
        with patch(
            "hermes_cli.config.load_config_readonly",
            return_value={"cron": {}},
        ):
            job = create_job(
                prompt="x", schedule="every 1m", name="production-no-identity"
            )
            record = executions.create_execution(job["id"], source="builtin")
            claimed = claim_job_for_fire(
                job["id"], return_job=True, execution_id=record["id"]
            )
            assert claimed["fire_claim"]["execution_id"] == record["id"]
            with executions._transaction() as conn:
                owner = conn.execute(
                    "SELECT owner_host_id FROM executions WHERE id=?",
                    (record["id"],),
                ).fetchone()["owner_host_id"]
                conn.execute(
                    "UPDATE executions SET process_id='dead-production-owner', pid=?, "
                    "process_started_at=NULL WHERE id=?",
                    (_dead_pid(), record["id"]),
                )

            assert owner is None
            assert executions.recover_interrupted_executions() == 1
            assert executions.latest_execution(job["id"])["status"] == "unknown"
            assert get_job(job["id"])["fire_claim"] is None
            retry_execution = executions.create_execution(
                job["id"], source="builtin"
            )
            retry = claim_job_for_fire(
                job["id"], return_job=True, execution_id=retry_execution["id"]
            )
            assert retry["fire_claim"]["execution_id"] != record["id"]

    def test_owner_host_id_requires_stable_configured_identity(
        self, executions, monkeypatch
    ):
        monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
        monkeypatch.setattr(executions.socket, "gethostname", lambda: "0123456789ab")
        assert executions._owner_host_id() is None

        monkeypatch.setattr(executions.socket, "gethostname", lambda: "stable-host")
        assert executions._owner_host_id() == "stable-host"

        monkeypatch.setenv("HERMES_MACHINE_ID", "stable-host-across-restarts")
        assert executions._owner_host_id() == "stable-host-across-restarts"

    def test_owner_host_id_uses_config_only_for_ephemeral_hostname(
        self, executions, monkeypatch
    ):
        monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
        monkeypatch.setattr(executions.socket, "gethostname", lambda: "0123456789ab")
        with patch(
            "hermes_cli.config.load_config_readonly",
            return_value={"cron": {"machine_id": "stable-config-host"}},
        ):
            assert executions._owner_host_id() == "stable-config-host"

    def test_owner_host_id_prefers_service_hostname_over_shared_profile_config(
        self, executions, monkeypatch
    ):
        monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
        monkeypatch.setattr(executions.socket, "gethostname", lambda: "hermes-dashboard")
        with patch(
            "hermes_cli.config.load_config_readonly",
            return_value={"cron": {"machine_id": "shared-profile-id"}},
        ):
            assert executions._owner_host_id() == "hermes-dashboard"

    def test_recovery_does_not_clear_a_replacement_execution_claim(self, executions):
        """A stale recovery candidate cannot revoke a newer claim by the same job."""
        import cron.jobs as jobs

        job = jobs.create_job(
            prompt="x", schedule="every 1m", name="replacement-fire-claim"
        )
        record = executions.create_execution(job["id"], source="builtin")
        claimed = jobs.claim_job_for_fire(
            job["id"], return_job=True, execution_id=record["id"]
        )
        replacement = dict(claimed["fire_claim"])
        replacement["execution_id"] = "newer-execution"
        all_jobs = jobs.load_jobs()
        all_jobs[0]["fire_claim"] = replacement
        jobs.save_jobs(all_jobs)
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-gateway', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), record["id"]),
            )

        assert executions.recover_interrupted_executions() == 1
        assert jobs.get_job(job["id"])["fire_claim"] == replacement

    def test_legacy_recovery_preserves_claim_when_another_execution_is_live(
        self, executions
    ):
        """A legacy claim is ambiguous if any live attempt now owns the job."""
        from cron.jobs import claim_job_for_fire, create_job, get_job

        job = create_job(prompt="x", schedule="every 1m", name="live-fire-claim")
        interrupted = executions.create_execution(job["id"], source="builtin")
        assert claim_job_for_fire(job["id"], return_job=True)
        live = executions.create_execution(job["id"], source="builtin")
        executions.mark_execution_running(live["id"])
        with executions._transaction() as conn:
            conn.execute(
                "UPDATE executions SET process_id='dead-gateway', pid=?, "
                "process_started_at=NULL WHERE id=?",
                (_dead_pid(), interrupted["id"]),
            )
        claim_before = dict(get_job(job["id"])["fire_claim"])

        assert executions.recover_interrupted_executions() == 1
        assert executions.latest_execution(job["id"])["status"] == "running"
        assert get_job(job["id"])["fire_claim"] == claim_before

    def test_live_owner_claim_is_never_rewritten(self, executions):
        """A claim owned by a live process (this one) must survive the reap."""
        record = executions.create_execution("live-job", source="builtin")
        executions.mark_execution_running(record["id"])

        _run_tick()

        assert executions.latest_execution("live-job")["status"] == "running"

    def test_reap_is_throttled_between_ticks(self, monkeypatch, executions):
        calls = []
        monkeypatch.setattr(
            "cron.executions.recover_interrupted_executions",
            lambda: calls.append(1) or 0,
        )

        _run_tick()
        _run_tick()
        assert len(calls) == 1, "back-to-back ticks must not reap twice"

        monkeypatch.setattr(
            scheduler_mod,
            "_last_dead_owner_reap_at",
            time.monotonic() - scheduler_mod._DEAD_OWNER_REAP_INTERVAL_SECONDS - 1,
        )
        _run_tick()
        assert len(calls) == 2, "an expired throttle window must reap again"

    def test_reap_failure_does_not_break_the_tick(self, monkeypatch):
        def _boom():
            raise RuntimeError("ledger unavailable")

        monkeypatch.setattr(
            "cron.executions.recover_interrupted_executions", _boom
        )

        assert _run_tick() == 0

    def test_lost_fire_claim_clears_only_its_linked_one_shot_claim(
        self, executions, monkeypatch,
    ):
        """A scheduler loser must not strand its run_claim on the failed row."""
        import cron.jobs as jobs

        job = jobs.create_job(
            prompt="x", schedule="in 30m", name="lost-fire-claim-oneshot",
        )
        snapshot = jobs.load_jobs()
        record = next(row for row in snapshot if row["id"] == job["id"])
        record["run_claim"] = {
            "at": "2026-09-25T12:00:00+00:00",
            "by": "scheduler-a",
        }
        jobs.save_jobs(snapshot)
        due_job = jobs.get_job(job["id"])

        monkeypatch.setattr(scheduler_mod, "get_due_jobs", lambda: [due_job])
        monkeypatch.setattr(scheduler_mod, "advance_next_runs", lambda _ids: 0)
        monkeypatch.setattr(scheduler_mod, "claim_job_for_fire", lambda *_a, **_k: False)

        with (
            patch.object(scheduler_mod, "run_one_job") as run,
            patch("tools.mcp_tool._kill_orphaned_mcp_children", lambda: None),
        ):
            result = scheduler_mod.tick(verbose=False)

        assert result == 1
        execution = executions.latest_execution(job["id"])
        assert execution["status"] == "failed"
        assert execution["error"] == "Fire claim lost; execution was not started."
        assert jobs.get_job(job["id"])["run_claim"] is None
        run.assert_not_called()

    def test_one_shot_binding_error_terminalizes_created_execution(
        self, executions, monkeypatch,
    ):
        """A link I/O failure must not leave a live-owner claimed ledger row."""
        import cron.jobs as jobs

        job = jobs.create_job(
            prompt="x", schedule="in 30m", name="binding-error-oneshot",
        )
        snapshot = jobs.load_jobs()
        record = next(row for row in snapshot if row["id"] == job["id"])
        record["run_claim"] = {
            "at": "2026-09-25T12:00:00+00:00",
            "by": "scheduler-a",
        }
        jobs.save_jobs(snapshot)
        due_job = jobs.get_job(job["id"])

        monkeypatch.setattr(scheduler_mod, "get_due_jobs", lambda: [due_job])
        monkeypatch.setattr(scheduler_mod, "advance_next_runs", lambda _ids: 0)
        def fail_link(*_args, **_kwargs):
            raise OSError("jobs store unavailable")

        monkeypatch.setattr(scheduler_mod, "link_run_claim_to_execution", fail_link)
        with patch("tools.mcp_tool._kill_orphaned_mcp_children", lambda: None):
            result = scheduler_mod.tick(verbose=False)

        assert result == 0
        execution = executions.latest_execution(job["id"])
        assert execution["status"] == "failed"
        assert "Scheduler execution setup failed before dispatch" in execution["error"]
        assert jobs.get_job(job["id"])["run_claim"] is None


class TestOneShotCliRunIsSynchronous:
    @pytest.fixture(autouse=True)
    def _restore_async_delivery_flag(self):
        from gateway.session_context import _SESSION_ASYNC_DELIVERY, _UNSET

        token = _SESSION_ASYNC_DELIVERY.set(_UNSET)
        yield
        _SESSION_ASYNC_DELIVERY.reset(token)

    def test_cli_run_declares_stateless_channel_before_dispatch(self, monkeypatch):
        """`hermes cron run` must gate off async delivery so the run executes
        synchronously in the CLI process instead of on a doomed daemon thread."""
        from gateway.session_context import async_delivery_supported
        from hermes_cli import cron as cron_cli

        observed = {}

        def _fake_cron_api(**kwargs):
            observed["async_delivery"] = async_delivery_supported()
            return {"success": True, "job": {"executed": True, "execution_success": True}}

        monkeypatch.setattr(cron_cli, "_cron_api", _fake_cron_api)

        assert cron_cli._job_action("run", "job-123", "Triggered") == 0
        assert observed["async_delivery"] is False
        # Scoped declaration: the capability must be restored after the call
        # so in-process callers (tests, embedding apps) are not tainted.
        assert async_delivery_supported() is True

    def test_non_run_actions_leave_channel_capability_alone(self, monkeypatch):
        from gateway.session_context import async_delivery_supported
        from hermes_cli import cron as cron_cli

        observed = {}

        def _fake_cron_api(**kwargs):
            observed["async_delivery"] = async_delivery_supported()
            return {"success": True, "job": {"name": "j"}}

        monkeypatch.setattr(cron_cli, "_cron_api", _fake_cron_api)

        cron_cli._job_action("pause", "job-123", "Paused")
        assert observed["async_delivery"] is True

    def test_background_dispatch_refused_when_channel_stateless(self, monkeypatch):
        """End-to-end gate: with the stateless declaration active, the cron
        tool's background dispatcher must fall back to synchronous execution
        (return None) even when a session key is inherited from a gateway env."""
        from gateway.session_context import declare_stateless_channel
        from tools.cronjob_tools import _try_dispatch_background_run

        declare_stateless_channel()
        monkeypatch.setenv("HERMES_SESSION_KEY", "inherited-gateway-session")

        result = _try_dispatch_background_run(
            {"id": "job-x", "name": "job-x"}, session_id="sess-1"
        )
        assert result is None
