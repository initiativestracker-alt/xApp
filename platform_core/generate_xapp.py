"""
platform_core/generate_xapp.py
════════════════════════════════════════════════════════════════════════════
Orchestrator: scaffolds a new workflow folder, then deploys it to Lambda.

Called by master_dashboard/app.py  →  POST /api/generate

What it does:
  1.  Derive workflow_id and port from name
  2.  Create workflows/<id>/ folder structure
  3.  Generate app.py via builders.generate_app_code()
  4.  Copy templates/ and static/ from base_workflow/
  5.  Write config/workflow.json  (uploaded pipeline nodes)
  6.  Write data/dashboard_data.json  (steps from nodes, empty jobs/batches)
  7.  Copy requirements.txt from base_workflow/
  8.  Call builders.build_and_deploy()  →  zip + upload → Lambda URL
  9.  Save URL + status to workflow_registry.json
  10. Save full blueprint to applications.json
════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

PLATFORM_CORE_DIR = Path(__file__).resolve().parent
PLATFORM_ROOT     = PLATFORM_CORE_DIR.parent
BASE_WORKFLOW_DIR = PLATFORM_ROOT / "workflow_templates" / "base_workflow"
WORKFLOWS_DIR     = PLATFORM_ROOT / "workflows"

from utils import (
    load_registry, save_registry,
    read_json, write_json,
    APPLICATIONS_PATH, next_available_port, slugify,
)
from builders import generate_app_code, build_and_deploy


# ── Helpers ───────────────────────────────────────────────────────────────────

def _steps_from_nodes(nodes: list[dict]) -> list[dict]:
    """Convert pipeline nodes into dashboard_data step entries."""
    steps = []
    for node in nodes:
        node_id = int(node.get("id", 0))
        name    = node.get("name", "")
        desc    = (node.get("user sescription")
                   or node.get("user description")
                   or node.get("designer description", ""))
        if isinstance(desc, str):
            desc = desc.strip()[:200]

        name_lower = name.lower()
        if name_lower in ("input", "output"):
            expected = 60
        elif any(k in name_lower for k in ("hitl", "annotation", "human")):
            expected = 900
        elif any(k in name_lower for k in ("llm", "extraction", "gpt")):
            expected = 300
        elif any(k in name_lower for k in ("router", "hypo")):
            expected = 60
        else:
            expected = 180

        inputs  = node.get("inputs",  [])
        outputs = node.get("outputs", [])
        steps.append({
            "step_order":       node_id,
            "step_id":          str(node_id),
            "step_name":        name,
            "description":      desc,
            "input_list_json":  json.dumps(inputs  if isinstance(inputs,  list) else [inputs]),
            "output_list_json": json.dumps(outputs if isinstance(outputs, list) else [outputs]),
            "expected_seconds": expected,
        })
    return steps


def _empty_dashboard(workflow_id: str, steps: list[dict]) -> dict:
    return {
        "steps":             steps,
        "steps_by_workflow": {workflow_id: steps},
        "jobs":              [],
        "job_members":       [],
        "batches":           [],
        "batch_steps":       [],
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def scaffold_workflow(
    name:           str,
    scope:          str,
    workflow_nodes: list[dict],
    created_by:     str = "",
) -> dict:
    """
    Scaffold a workflow folder and deploy it to AWS Lambda.
    Returns the completed registry entry dict (includes the Lambda URL).
    """
    workflow_id = "wf_" + slugify(name)
    port        = next_available_port()   # kept for local-run compat
    created_at  = datetime.now(timezone.utc).isoformat()

    blueprint = {
        "workflow_id": workflow_id,
        "name":        name,
        "scope":       scope,
        "port":        port,
        "nodes":       workflow_nodes,
        "created_by":  created_by,
        "created_at":  created_at,
    }

    target_dir = WORKFLOWS_DIR / workflow_id

    # ── Create folder structure ───────────────────────────────────────────────
    for sub in ["config", "data", "templates", "static", "input_files"]:
        (target_dir / sub).mkdir(parents=True, exist_ok=True)

    # ── Generate app.py ───────────────────────────────────────────────────────
    app_code = generate_app_code(blueprint)
    (target_dir / "app.py").write_text(app_code, encoding="utf-8")

    # ── Copy templates and static from base_workflow ──────────────────────────
    for tmpl in (BASE_WORKFLOW_DIR / "templates").glob("*"):
        shutil.copy2(tmpl, target_dir / "templates" / tmpl.name)
    for asset in (BASE_WORKFLOW_DIR / "static").glob("*"):
        shutil.copy2(asset, target_dir / "static" / asset.name)

    # ── Copy requirements.txt ─────────────────────────────────────────────────
    base_req = BASE_WORKFLOW_DIR / "requirements.txt"
    if base_req.exists():
        shutil.copy2(base_req, target_dir / "requirements.txt")

    # ── Write config/workflow.json ────────────────────────────────────────────
    write_json(target_dir / "config" / "workflow.json", {
        "nodes": workflow_nodes,
        "edges": [],
    })

    # ── Write data/dashboard_data.json ────────────────────────────────────────
    steps = _steps_from_nodes(workflow_nodes)
    write_json(
        target_dir / "data" / "dashboard_data.json",
        _empty_dashboard(workflow_id, steps),
    )

    # ── Build + Deploy to Lambda ──────────────────────────────────────────────
    lambda_url = build_and_deploy(workflow_id, blueprint)

    # ── Register in workflow_registry.json ───────────────────────────────────
    registry = [w for w in load_registry() if w["workflow_id"] != workflow_id]
    entry = {
        "workflow_id": workflow_id,
        "name":        name,
        "scope":       scope,
        "port":        port,           # kept for reference / local dev
        "status":      "deployed",
        "pid":         None,           # no subprocess — Lambda manages itself
        "url":         lambda_url,     # ← public Lambda URL
        "created_at":  created_at,
        "created_by":  created_by,
    }
    registry.append(entry)
    save_registry(registry)

    # ── Persist full blueprint to applications.json ───────────────────────────
    apps = [a for a in read_json(APPLICATIONS_PATH).get("applications", [])
            if a.get("app_id") != workflow_id]
    apps.append({
        "app_id":     workflow_id,
        "name":       name,
        "scope":      scope,
        "created_by": created_by,
        "created_at": created_at,
        "status":     "deployed",
        "port":       port,
        "url":        lambda_url,
        "nodes":      workflow_nodes,
    })
    write_json(APPLICATIONS_PATH, {"applications": apps})

    print(f"[generate_xapp] ✓ '{name}' deployed → {lambda_url}")
    return entry
