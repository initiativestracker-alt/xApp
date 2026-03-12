"""
platform_core/utils.py
Shared helpers used by master_dashboard, generate_xapp, builders, workflow_manager.
"""
from __future__ import annotations
import json
import re
from pathlib import Path

PLATFORM_CORE_DIR = Path(__file__).resolve().parent
REGISTRY_PATH     = PLATFORM_CORE_DIR / "workflow_registry.json"
APPLICATIONS_PATH = PLATFORM_CORE_DIR / "applications.json"
MEMBERS_PATH      = PLATFORM_CORE_DIR / "users_members.json"

BASE_PORT = 5001  # master_dashboard is 5000; workflows start at 5001


# ── JSON helpers ─────────────────────────────────────────────────────────────

def read_json(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


# ── Registry helpers ─────────────────────────────────────────────────────────

def load_registry() -> list[dict]:
    return read_json(REGISTRY_PATH).get("workflows", [])


def save_registry(workflows: list[dict]) -> None:
    write_json(REGISTRY_PATH, {"workflows": workflows})


def get_workflow(workflow_id: str) -> dict | None:
    return next((w for w in load_registry() if w["workflow_id"] == workflow_id), None)


def next_available_port() -> int:
    """Return the next port after the highest currently registered."""
    registry = load_registry()
    if not registry:
        return BASE_PORT
    return max(w.get("port", BASE_PORT) for w in registry) + 1


# ── Slug / ID helpers ────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    """Turn a workflow name into a safe folder/id slug."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug[:40] if slug else "workflow"


# ── Members ──────────────────────────────────────────────────────────────────

def load_members() -> list[dict]:
    return read_json(MEMBERS_PATH).get("members", [])
