"""
platform_core/workflow_manager.py
Launches and stops workflow mini-app subprocesses.
Updates workflow_registry.json with pid and status after each action.
"""
from __future__ import annotations
import subprocess
import sys
from pathlib import Path

PLATFORM_CORE_DIR = Path(__file__).resolve().parent
PLATFORM_ROOT     = PLATFORM_CORE_DIR.parent
WORKFLOWS_DIR     = PLATFORM_ROOT / "workflows"

from utils import load_registry, save_registry


def launch(workflow_id: str) -> dict:
    """
    Start the workflow app as a subprocess.
    Returns the updated registry entry.
    """
    registry = load_registry()
    entry    = next((w for w in registry if w["workflow_id"] == workflow_id), None)

    if not entry:
        raise ValueError(f"Workflow '{workflow_id}' not found in registry.")

    app_path = WORKFLOWS_DIR / workflow_id / "app.py"
    if not app_path.exists():
        raise FileNotFoundError(f"app.py not found for workflow '{workflow_id}'.")

    # Launch as independent subprocess — survives if master_dashboard restarts
    proc = subprocess.Popen(
        [sys.executable, str(app_path)],
        cwd=str(WORKFLOWS_DIR / workflow_id),
    )

    entry["pid"]    = proc.pid
    entry["status"] = "running"

    save_registry(registry)
    return entry


def stop(workflow_id: str) -> dict:
    """
    Stop the workflow subprocess by PID.
    Returns the updated registry entry.
    """
    import os, signal

    registry = load_registry()
    entry    = next((w for w in registry if w["workflow_id"] == workflow_id), None)

    if not entry:
        raise ValueError(f"Workflow '{workflow_id}' not found in registry.")

    pid = entry.get("pid")
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass  # Already stopped

    entry["pid"]    = None
    entry["status"] = "stopped"

    save_registry(registry)
    return entry


def status(workflow_id: str) -> str:
    """
    Return the current status string for a workflow.
    Also checks if the PID is still alive and corrects stale 'running' entries.
    """
    import os

    registry = load_registry()
    entry    = next((w for w in registry if w["workflow_id"] == workflow_id), None)

    if not entry:
        return "unknown"

    pid = entry.get("pid")
    if entry.get("status") == "running" and pid:
        try:
            os.kill(pid, 0)  # 0 = check existence, no signal sent
        except (ProcessLookupError, PermissionError):
            # Process is gone — correct the registry
            entry["status"] = "stopped"
            entry["pid"]    = None
            save_registry(registry)
            return "stopped"

    return entry.get("status", "stopped")
