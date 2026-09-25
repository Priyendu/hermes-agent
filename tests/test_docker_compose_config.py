"""Deployment config contracts for Docker Compose (#1612)."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "compose_file",
    ["docker-compose.yml", "docker-compose.windows.yml"],
)
def test_dashboard_compose_service_uses_docker_init(compose_file: str) -> None:
    """The dashboard must run under Docker's init/reaper in shipped Compose."""
    config = yaml.safe_load((REPO_ROOT / compose_file).read_text(encoding="utf-8"))

    assert config["services"]["dashboard"]["init"] is True
