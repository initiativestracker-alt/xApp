"""
workflow app.py  —  generated from base_workflow template
Substitution tokens (replaced by builders.py at scaffold time):
    {{WORKFLOW_ID}}    → e.g. wf_lease_abstraction
    {{WORKFLOW_NAME}}  → e.g. Lease Abstraction
    {{PORT}}           → e.g. 5001
"""
from __future__ import annotations
from typing import Optional, Any
import json
import os
import ssl
import copy
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote
from urllib import request as urlrequest, error as urlerror

import re
from flask import Flask, abort, flash, redirect, render_template, request, send_from_directory, session, url_for
from flask import session as flask_session
from functools import wraps

BASE_DIR = Path(__file__).resolve().parent

# ── Workflow identity (injected by builders.py) ───────────────────────────────
WORKFLOW_ID   = "{{WORKFLOW_ID}}"
WORKFLOW_NAME = "{{WORKFLOW_NAME}}"
PORT          = {{PORT}}

# ── Data paths (all local to this workflow folder) ────────────────────────────
DASHBOARD_DATA_PATH   = BASE_DIR / "data"   / "dashboard_data.json"
WORKFLOW_CONFIG_PATH  = BASE_DIR / "config" / "workflow.json"
INPUT_FILES_DIR       = BASE_DIR / "input_files"

# ── Shared platform data (two levels up in platform_core/) ────────────────────
_PLATFORM_CORE   = BASE_DIR.parent.parent / "platform_core"
MEMBERS_PATH     = _PLATFORM_CORE / "users_members.json"
REGISTRY_PATH    = _PLATFORM_CORE / "workflow_registry.json"

# ── Legacy compatibility aliases (used by helpers below) ─────────────────────
LEASE_WORKFLOW_PATH    = WORKFLOW_CONFIG_PATH
UPLOADED_WORKFLOW_PATH = WORKFLOW_CONFIG_PATH
SYNTHETIC_MASTER_PATH  = BASE_DIR / "synthetic" / "master_synthetic_data.json"

# ── Load .env ─────────────────────────────────────────────────────────────────
def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().lstrip("\ufeff")
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v

_load_env_file(BASE_DIR.parent.parent / ".env")


# ════════════════════════════════════════════════════════════════════════════
# JSON PERSISTENCE
# ════════════════════════════════════════════════════════════════════════════

def _read_json(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def load_members() -> list[dict]:
    return _read_json(MEMBERS_PATH).get("members", [])

def load_workflows_data() -> dict:
    """Return workflow metadata from platform registry for this workflow."""
    reg = _read_json(REGISTRY_PATH).get("workflows", [])
    wf  = next((w for w in reg if w["workflow_id"] == WORKFLOW_ID), {})
    return {
        "workflows": [wf] if wf else [],
        "workflow_members": [],   # not used in workflow app
    }

def load_dashboard_data() -> dict:
    return _read_json(DASHBOARD_DATA_PATH)

def save_members(members: list[dict]) -> None:
    _write_json(MEMBERS_PATH, {"members": members})

def save_workflows_data(data: dict) -> None:
    pass  # workflow app does not write registry

def save_dashboard_data(data: dict) -> None:
    _write_json(DASHBOARD_DATA_PATH, data)

# ════════════════════════════════════════════════════════════════════════════
# PURE PYTHON QUERY HELPERS
# All analytics that were formerly SQL are implemented here as Python
# comprehensions and aggregations over plain list[dict] collections.
# ════════════════════════════════════════════════════════════════════════════

def _member_by_id(members: list[dict], member_id: str) -> Optional[dict]:
    return next((m for m in members if m["member_id"] == member_id), None)


def _member_by_email(members: list[dict], email: str) -> Optional[dict]:
    return next((m for m in members if m.get("email", "").lower() == email.lower()), None)


def _workflow_by_id(workflows: list[dict], workflow_id: str) -> Optional[dict]:
    return next((w for w in workflows if w["workflow_id"] == workflow_id), None)


def _job_by_id(jobs_list: list[dict], job_id: str) -> Optional[dict]:
    return next((j for j in jobs_list if j.get("job_id") == job_id), None)


def _batch_by_id(batches_list: list[dict], batch_id: str) -> Optional[dict]:
    return next((r for r in batches_list if r.get("batch_id") == batch_id), None)


def _step_by_order(steps: list[dict], step_order: int) -> Optional[dict]:
    return next((s for s in steps if int(s["step_order"]) == step_order), None)


def _batch_step(batch_steps: list[dict], batch_id: str, step_order: int) -> Optional[dict]:
    return next(
        (js for js in batch_steps
         if js["batch_id"] == batch_id and int(js["step_order"]) == step_order),
        None,
    )


def _workflow_ids_for_member(workflow_members: list[dict], member_id: str) -> list[str]:
    return [wm["workflow_id"] for wm in workflow_members if wm["member_id"] == member_id]


def _job_ids_for_member(job_members: list[dict], member_id: str) -> list[str]:
    return [bm["job_id"] for bm in job_members if bm["member_id"] == member_id]


def _member_ids_for_workflow(workflow_members: list[dict], workflow_id: str) -> list[str]:
    return [wm["member_id"] for wm in workflow_members if wm["workflow_id"] == workflow_id]


def _member_ids_for_job(job_members: list[dict], job_id: str) -> list[str]:
    return [bm["member_id"] for bm in job_members if bm.get("job_id") == job_id or bm.get("batch_id") == job_id]


# ── Dashboard aggregate helpers ──────────────────────────────────────────────

def _filter_batches(batches_list: list[dict],
                  workflow_id: Optional[str] = None,
                  job_id: Optional[str] = None,
                  batch_id: Optional[str] = None,
                  status: Optional[str] = None) -> list[dict]:
    result = batches_list
    if workflow_id:
        result = [r for r in result if r.get("workflow_id") == workflow_id]
    if job_id:
        result = [r for r in result if r.get("job_id") == job_id]
    if batch_id:
        result = [r for r in result if r.get("batch_id") == batch_id]
    if status:
        result = [r for r in result if r.get("status") == status]
    return result


def _filter_batch_steps(batch_steps: list[dict],
                      job_ids: Optional[set] = None,
                      step_order: Optional[int] = None) -> list[dict]:
    result = batch_steps
    if job_ids is not None:
        result = [js for js in result if js["batch_id"] in job_ids]
    if step_order is not None:
        result = [js for js in result if int(js["step_order"]) == step_order]
    return result


def _avg_cycle_seconds(jobs: list[dict]) -> Optional[float]:
    """Average (completed_at - started_at) in seconds over completed jobs."""
    durations = []
    for j in jobs:
        if j.get("status") != "completed":
            continue
        s = parse_dt(j.get("started_at"))
        c = parse_dt(j.get("completed_at"))
        if s and c:
            durations.append((c - s).total_seconds())
    return sum(durations) / len(durations) if durations else None


def _member_stats_for_workflow(batch_steps: list[dict],
                                batches_list: list[dict],
                                workflow_id: Optional[str],
                                steps: list[dict]) -> list[dict]:
    """
    Replicate the GROUP BY claimed_by query used for stalled-members insight.
    Returns list of dicts: claimed_by, total_claimed, total_completed,
                           overdue_count, max_age_seconds.
    """
    now = datetime.now(timezone.utc)
    scope_batch_ids = {r.get("batch_id") for r in (
        _filter_batches(batches_list, workflow_id=workflow_id) if workflow_id else batches_list
    ) if r.get("batch_id")}
    step_expected = {int(s["step_order"]): float(s.get("expected_seconds") or 300) for s in steps}

    by_member: dict[str, dict] = {}
    for js in batch_steps:
        if js["batch_id"] not in scope_batch_ids:
            continue
        claimer = js.get("claimed_by")
        if not claimer:
            continue
        m = by_member.setdefault(claimer, {
            "claimed_by": claimer,
            "total_claimed": 0,
            "total_completed": 0,
            "overdue_count": 0,
            "max_age_seconds": 0.0,
        })
        m["total_claimed"] += 1
        if js.get("completed_at"):
            m["total_completed"] += 1
        elif js.get("claimed_at"):
            exp = step_expected.get(int(js.get("step_order", 0)), 300)
            ca = parse_dt(js["claimed_at"])
            if ca:
                age = (now - ca).total_seconds()
                m["max_age_seconds"] = max(m["max_age_seconds"], age)
                if age > exp * 2:
                    m["overdue_count"] += 1

    return list(by_member.values())


def _member_open_counts(batch_steps: list[dict],
                        batches_list: list[dict],
                        workflow_id: Optional[str]) -> list[dict]:
    """GROUP BY claimed_by WHERE claimed_by IS NOT NULL AND completed_at IS NULL."""
    scope_batch_ids = {r.get("batch_id") for r in (
        _filter_batches(batches_list, workflow_id=workflow_id) if workflow_id else batches_list
    ) if r.get("batch_id")}
    counts: dict[str, int] = {}
    for js in batch_steps:
        if js["batch_id"] not in scope_batch_ids:
            continue
        if js.get("claimed_by") and not js.get("completed_at"):
            counts[js["claimed_by"]] = counts.get(js["claimed_by"], 0) + 1
    return [{"claimed_by": k, "cnt": v} for k, v in counts.items()]


def _blocked_batches_rows(batch_steps: list[dict],
                       batches_list: list[dict],
                       steps: list[dict],
                       workflow_id: Optional[str]) -> list[dict]:
    """Replicate the blocked jobs table query."""
    now = datetime.now(timezone.utc)
    active_batch_ids = {r.get("batch_id") for r in _filter_batches(batches_list, workflow_id=workflow_id, status="active") if r.get("batch_id")}
    step_name_map = {int(s["step_order"]): s.get("step_name", "") for s in steps}
    step_exp_map  = {int(s["step_order"]): float(s.get("expected_seconds") or 300) for s in steps}
    rows = []
    for js in batch_steps:
        if js["batch_id"] not in active_batch_ids:
            continue
        if js.get("completed_at") or not js.get("claimed_by") or not js.get("claimed_at"):
            continue
        ca = parse_dt(js["claimed_at"])
        age_seconds = (now - ca).total_seconds() if ca else 0.0
        so = int(js.get("step_order", 0))
        rows.append({
            "batch_id":          js["batch_id"],
            "step_name":       step_name_map.get(so, ""),
            "claimed_by":      js["claimed_by"],
            "age_seconds":     age_seconds,
            "expected_seconds": step_exp_map.get(so, 300),
        })
    rows.sort(key=lambda r: r["age_seconds"], reverse=True)
    return rows


# ════════════════════════════════════════════════════════════════════════════
# SEED — runs once if dashboard_data.json has no jobs
# ════════════════════════════════════════════════════════════════════════════

def seed_if_empty() -> None:
    """
    Idempotent seed — runs on every request but only writes when something is
    actually missing.  Safe to call repeatedly.

    Per-workflow logic:
    • workflows   — added if absent from workflows.json
    • jobs        — added per workflow_id if none exist yet for that workflow
    • batches     — added per batch_id if not already present
    • batch_steps — added per (batch_id, step_order) if not already present
    • steps       — stored per workflow_id in dashboard_data["steps_by_workflow"]
                    (also kept in legacy "steps" key for backward-compat)
    """
    seed_data = _seed_data()
    master    = _load_master_synthetic()
    dd        = load_dashboard_data()
    wf_data   = load_workflows_data()

    changed_wf = False
    changed_dd = False

    # ── 1. Workflows & workflow_members ─────────────────────────────────────
    workflows        = wf_data.get("workflows", [])
    workflow_members = wf_data.get("workflow_members", [])
    for w in seed_data.get("workflows", []):
        wid = w["workflow_id"]
        if not _workflow_by_id(workflows, wid):
            workflows.append({
                "workflow_id":      wid,
                "name":             w["name"],
                "description":      w.get("description", ""),
                "priority":         w.get("priority", 99),
                "status":           w.get("status", "active"),
                "supervisor_id":    w.get("supervisor_id"),
                "est_time_seconds": w.get("est_time_seconds", 0),
            })
            changed_wf = True
        for mid in w.get("assigned_members", []):
            if not any(wm["workflow_id"] == wid and wm["member_id"] == mid
                       for wm in workflow_members):
                workflow_members.append({"workflow_id": wid, "member_id": mid})
                changed_wf = True
    if changed_wf:
        save_workflows_data({"workflows": workflows, "workflow_members": workflow_members})

    # ── 2. Jobs (work-package containers e.g. jb_lease_001) ─────────────────
    jobs        = dd.get("jobs", [])
    job_members = dd.get("job_members", [])
    existing_job_ids = {j["job_id"] for j in jobs}

    for b in seed_data.get("jobs", []):
        jid = b.get("job_id") or b.get("batch_id")
        if not jid or jid in existing_job_ids:
            continue
        jobs.append({
            "job_id":               jid,
            "workflow_id":          b["workflow_id"],
            "name":                 b.get("name", jid),
            "description":          b.get("description", ""),
            "priority":             b.get("priority", 99),
            "status":               b.get("status", "active"),
            "supervisor_id":        b.get("supervisor_id"),
            "special_instructions": b.get("special_instructions"),
            "est_time_seconds":     b.get("est_time_seconds", 0),
            "total_batches":        b.get("total_batches", b.get("total_jobs", 0)),
        })
        existing_job_ids.add(jid)
        for mid in b.get("assigned_members", []):
            if not any(bm["job_id"] == jid and bm["member_id"] == mid for bm in job_members):
                job_members.append({"job_id": jid, "member_id": mid})
        changed_dd = True

    # ── 3. Build steps per workflow from workflow JSON / synthetic payload ───
    # steps_by_workflow: { workflow_id: [ {step_order, step_id, step_name, ...} ] }
    steps_by_workflow: dict[str, list] = dd.get("steps_by_workflow") or {}

    # Workflow → canonical workflow JSON path
    wf_json_paths: dict[str, Optional[Path]] = {
        "wf_lease_abstraction": LEASE_WORKFLOW_PATH,
    }
    if UPLOADED_WORKFLOW_PATH.exists():
        # Determine which workflow the uploaded JSON belongs to by checking workflows.json
        registered = [w["workflow_id"] for w in workflows if w["workflow_id"] != "wf_lease_abstraction"]
        for wid in registered:
            if wid not in wf_json_paths:
                wf_json_paths[wid] = UPLOADED_WORKFLOW_PATH

    all_wf_ids = {w["workflow_id"] for w in workflows}
    for wid in all_wf_ids:
        if wid in steps_by_workflow:
            continue  # already built
        wf_path = wf_json_paths.get(wid)
        if wf_path and wf_path.exists():
            steps_by_workflow[wid] = load_steps_from_workflow_json(wf_path)
            changed_dd = True
        else:
            # Fall back to synthetic payload steps
            synth_steps = master.get("workflow_payloads", {}).get(wid, {}).get("steps", [])
            if synth_steps:
                steps_by_workflow[wid] = [
                    {
                        "step_order":       idx,
                        "step_id":          str(s.get("step_id", idx)),
                        "step_name":        s.get("step_name", f"Step {idx}"),
                        "description":      "",
                        "input_list_json":  "[]",
                        "output_list_json": "[]",
                        "expected_seconds": _expected_seconds_for_step_name(
                            s.get("step_name", ""), idx),
                    }
                    for idx, s in enumerate(synth_steps)
                ]
                changed_dd = True

    # Keep legacy "steps" key pointing to the first available workflow's steps
    # so old code paths that read dd["steps"] still get something useful.
    legacy_steps = dd.get("steps") or []
    if not legacy_steps and steps_by_workflow:
        first_wid = next(iter(steps_by_workflow))
        legacy_steps = steps_by_workflow[first_wid]
        changed_dd = True

    # ── 4. Batches (individual documents) & batch_steps ─────────────────────
    batches     = dd.get("batches", [])
    batch_steps = dd.get("batch_steps", [])
    existing_batch_ids = {r["batch_id"] for r in batches}
    existing_bs_keys   = {(rs["batch_id"], int(rs["step_order"])) for rs in batch_steps}

    # Collect all ext-batch records from every key in batches_extended
    je = seed_data.get("batches_extended", {})
    record_ext_map: dict[str, dict] = {}
    for key, entries in je.items():
        if key == "note" or not isinstance(entries, list):
            continue
        for entry in entries:
            rid = entry.get("batch_id")
            if rid:
                record_ext_map[rid] = entry

    # Also auto-inject payloads for any registered workflow missing one
    for wid in all_wf_ids:
        if wid not in master.get("workflow_payloads", {}):
            master = _inject_workflow_payload(wid, master=master, save=True)

    # Build per-workflow payload lookups
    _all_payloads = master.get("workflow_payloads", {})
    records_by_workflow:     dict[str, dict] = {}
    synth_steps_by_workflow: dict[str, dict] = {}
    for wid, synth in _all_payloads.items():
        if not synth:
            continue
        dataset_records = synth.get("dataset", {}).get("batches", [])
        records_by_workflow[wid] = {
            r.get("batch_id"): r for r in dataset_records if r.get("batch_id")
        }
        synth_steps_by_workflow[wid] = {
            str(s.get("step_id")): s for s in synth.get("steps", [])
        }

    created_at = utc_now()

    for rid, ext in record_ext_map.items():
        if rid in existing_batch_ids:
            continue
        wid    = ext.get("workflow_id")
        status = ext.get("status", "active")
        cso    = ext.get("current_step_order", 0)
        job_id = ext.get("job_id") or ext.get("parent_batch_id")
        batches.append({
            "batch_id":           rid,
            "job_id":             job_id,
            "workflow_id":        wid,
            "created_at":         created_at,
            "started_at":         created_at,
            "completed_at":       created_at if status == "completed" else None,
            "current_step_order": cso,
            "status":             status,
        })
        existing_batch_ids.add(rid)
        changed_dd = True

        wf_steps = steps_by_workflow.get(wid or "", legacy_steps)
        for st in wf_steps:
            so = int(st["step_order"])
            if (rid, so) in existing_bs_keys:
                continue
            payload: dict = {}
            synth_step = synth_steps_by_workflow.get(wid or "", {}).get(str(st.get("step_id", so)))
            if synth_step:
                for rec in synth_step.get("batches", []):
                    if rec.get("batch_id") == rid:
                        payload = rec.get("input") or {}
                        break
            if not payload:
                payload = records_by_workflow.get(wid or "", {}).get(rid, {})
            batch_steps.append({
                "batch_id":     rid,
                "step_order":   so,
                "claimed_by":   None,
                "claimed_at":   None,
                "completed_at": None,
                "payload_json": json.dumps(payload, indent=2, ensure_ascii=False),
                "member_notes": "",
            })
            existing_bs_keys.add((rid, so))

    # ── 5. Persist only if something actually changed ────────────────────────
    if changed_dd:
        save_dashboard_data({
            "steps":             legacy_steps,
            "steps_by_workflow": steps_by_workflow,
            "jobs":              jobs,
            "job_members":       job_members,
            "batches":           batches,
            "batch_steps":       batch_steps,
        })


def _get_steps_for_workflow(workflow_id: str, dd: Optional[dict] = None) -> list[dict]:
    """
    Return the steps list for a given workflow_id.
    Priority: steps_by_workflow[wid] → workflow JSON file → synthetic payload steps.
    This is the single authoritative lookup for steps_meta in every route.
    """
    if dd is None:
        dd = load_dashboard_data()

    # 1. steps_by_workflow (populated by seed_if_empty)
    sbw = dd.get("steps_by_workflow") or {}
    if workflow_id in sbw and sbw[workflow_id]:
        return sbw[workflow_id]

    # 2. Workflow JSON file
    if workflow_id == "wf_lease_abstraction" and LEASE_WORKFLOW_PATH.exists():
        return load_steps_from_workflow_json(LEASE_WORKFLOW_PATH)
    if UPLOADED_WORKFLOW_PATH.exists():
        return load_steps_from_workflow_json(UPLOADED_WORKFLOW_PATH)

    # 3. Synthetic payload steps as last resort
    synth_steps = _workflow_payload(workflow_id).get("steps", [])
    if synth_steps:
        return [
            {
                "step_order":       idx,
                "step_id":          str(s.get("step_id", idx)),
                "step_name":        s.get("step_name", f"Step {idx}"),
                "description":      "",
                "input_list_json":  "[]",
                "output_list_json": "[]",
                "expected_seconds": _expected_seconds_for_step_name(s.get("step_name", ""), idx),
            }
            for idx, s in enumerate(synth_steps)
        ]
    return []


# ════════════════════════════════════════════════════════════════════════════
# UTILITY / HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════

def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().lstrip("\ufeff")
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        if k.startswith("GEMINI_"):
            os.environ[k] = v
            continue
        if k not in os.environ:
            os.environ[k] = v


_load_env_file(BASE_DIR / ".env")


def _gemini_ssl_context() -> ssl.SSLContext:
    try:
        import certifi  # type: ignore
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def _load_master_synthetic() -> dict[str, Any]:
    if SYNTHETIC_MASTER_PATH.exists():
        return json.loads(SYNTHETIC_MASTER_PATH.read_text(encoding="utf-8"))
    return {}


def _seed_data() -> dict[str, Any]:
    master = _load_master_synthetic()
    return master.get("seed") or {}


def _steps_from_workflow_json(wf_path: Optional[Path] = None) -> list[dict]:
    """Build a synthetic steps list from a workflow node JSON file."""
    path = wf_path or (UPLOADED_WORKFLOW_PATH if UPLOADED_WORKFLOW_PATH.exists() else LEASE_WORKFLOW_PATH)
    if not path or not path.exists():
        return []
    wf = json.loads(path.read_text(encoding="utf-8"))
    nodes_sorted = sorted(
        wf.get("nodes", []),
        key=lambda n: (int(n.get("x", 0) or 0), int(n.get("y", 0) or 0), int(n.get("id", 0) or 0)),
    )
    return [
        {"step_id": str(n.get("id")), "step_name": str(n.get("name")), "batches": []}
        for n in nodes_sorted
    ]


def _inject_workflow_payload(
    workflow_id: str,
    custom_payload: Optional[dict] = None,
    master: Optional[dict] = None,
    save: bool = True,
) -> dict:
    """
    Register a synthetic payload for workflow_id in master_synthetic_data.json.

    Two modes:
      • custom_payload supplied → use it as-is (stamps in workflow_id).
      • no custom_payload       → clone the first available existing payload,
                                  retag it for workflow_id, rebuild steps from
                                  the uploaded/lease workflow JSON.

    Called automatically by _workflow_payload() on first access (fallback),
    explicitly from save_config() after a new workflow is uploaded,
    and from the CLI: python app.py inject --workflow-id X [--input FILE].
    """
    import copy as _copy

    if master is None:
        master = _load_master_synthetic()

    if custom_payload is not None:
        payload = _copy.deepcopy(custom_payload)
        payload["workflow_id"] = workflow_id
    else:
        payloads  = master.get("workflow_payloads", {})
        source_id, source = next(
            ((wid, p) for wid, p in payloads.items() if p and wid != workflow_id),
            (None, None),
        )
        if not source:
            print(f"[synthetic] No source payload found to clone for '{workflow_id}' — skipping.")
            return master

        payload = _copy.deepcopy(source)
        payload["workflow_id"]          = workflow_id
        payload["source_workflow_file"] = f"cloned_from_{source_id}"
        for b in payload.get("dataset", {}).get("batches", []):
            b["workflow_id"] = workflow_id
        payload["steps"] = _steps_from_workflow_json()
        if "_dashboard_state" in payload:
            payload["_dashboard_state"]["workflow_id"] = workflow_id
        print(
            f"[synthetic] Cloned '{source_id}' → '{workflow_id}' "
            f"({len(payload['steps'])} steps, "
            f"{len(payload.get('dataset', {}).get('batches', []))} batches)"
        )

    master.setdefault("workflow_payloads", {})[workflow_id] = payload
    if save and SYNTHETIC_MASTER_PATH.exists():
        SYNTHETIC_MASTER_PATH.write_text(
            json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    return master


def _workflow_payload(workflow_id: str) -> dict[str, Any]:
    master   = _load_master_synthetic()
    payloads = master.get("workflow_payloads", {})
    if workflow_id in payloads:
        return payloads.get(workflow_id) or {}

    # First access for this workflow — auto-inject a fallback so the dashboard
    # is never empty. Explicit synthetic data can be provided later.
    print(f"[synthetic] '{workflow_id}' missing — auto-cloning from existing payload.")
    _inject_workflow_payload(workflow_id, master=master, save=True)
    return _load_master_synthetic().get("workflow_payloads", {}).get(workflow_id) or {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def jinja_search(s, pattern):
    return bool(re.search(pattern, str(s or ""), re.IGNORECASE))


def jinja_match(s, pattern):
    return bool(re.match(pattern, str(s or ""), re.IGNORECASE))


def extract_complexity(description: str) -> Optional[str]:
    if not description:
        return None
    m = re.search(r'Complexity:\s*(Advanced|High|Medium|Low)', description, re.IGNORECASE)
    return m.group(1).capitalize() if m else None


def compute_pipeline_health(step_rows: list, stale_jobs: int, bottleneck_step_order) -> dict:
    has_critical = any(r.get("time_ratio") is not None and r["time_ratio"] > 1.5 for r in step_rows)
    has_slow     = any(r.get("time_ratio") is not None and r["time_ratio"] > 1.2 for r in step_rows)
    if stale_jobs > 0 or has_critical:
        return {"level": "danger", "label": "At Risk",
                "reason": f"{stale_jobs} overdue job(s) or critical step delay detected."}
    if bottleneck_step_order is not None or has_slow:
        return {"level": "warn", "label": "Needs Attention",
                "reason": "One or more steps running above expected time."}
    return {"level": "ok", "label": "Healthy",
            "reason": "All steps within expected timing. No overdue jobs."}


def step_display_name(step_name: str) -> str:
    lowered = (step_name or "").lower().strip()
    if lowered.startswith("hypo"):
        if step_name.startswith("hypo - "):
            return f"hypo (*) - {step_name[len('hypo - '):].strip()}"
        if step_name.startswith("hypo (*)"):
            return step_name
        return f"hypo (*) {step_name[4:].lstrip()}"
    return step_name


def label_from_path(path: str) -> str:
    parts = [p.replace("_", " ").strip() for p in path.split(".") if p.strip()]
    return " / ".join(p[:1].upper() + p[1:] if p else "" for p in parts)


def _is_scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def flatten_scalar_fields(obj: object, prefix: str = "") -> dict[str, object]:
    out: dict[str, object] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k)
            next_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(v, dict):
                out.update(flatten_scalar_fields(v, next_prefix))
            elif _is_scalar(v):
                out[next_prefix] = v
    return out


def _normalize_io_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if "\n" in text:
            return [s.strip(" -\t") for s in text.splitlines() if s.strip()]
        if "," in text:
            return [s.strip() for s in text.split(",") if s.strip()]
        return [text]
    return []


def _expected_seconds_for_step_name(step_name: str, step_order: int = 0) -> int:
    n = (step_name or "").strip().lower()
    if not n:
        return 300
    if n in ("input", "output"):
        return 120
    if "finder" in n or "router" in n:
        return 180
    if "chunker" in n or "converter" in n or "enhancer" in n:
        return 240
    if "textract" in n or "pdf_to_json" in n:
        return 300
    if "llm" in n or "gpt" in n:
        return 420
    if "validation" in n or "compliance" in n:
        return 360
    if n.startswith("hypo"):
        return 480
    return 240 + ((int(step_order) % 4) * 60)


def load_steps_from_workflow_json(workflow_path: Path) -> list[dict[str, Any]]:
    if not workflow_path.exists():
        return []
    data = json.loads(workflow_path.read_text(encoding="utf-8"))
    nodes = data.get("nodes", [])
    nodes_sorted = sorted(
        nodes,
        key=lambda n: (int(n.get("x", 0) or 0), int(n.get("y", 0) or 0), int(n.get("id", 0) or 0)),
    )
    out: list[dict[str, Any]] = []
    for idx, n in enumerate(nodes_sorted):
        name  = str(n.get("name") or "")
        desc  = str(n.get("user sescription") or n.get("designer description") or "").strip()
        inputs  = _normalize_io_list(n.get("inputs"))
        outputs = _normalize_io_list(n.get("outputs"))
        out.append({
            "step_order":      idx,
            "step_id":         str(n.get("id") or idx),
            "step_name":       name,
            "description":     desc,
            "input_list_json": json.dumps(inputs, ensure_ascii=False),
            "output_list_json": json.dumps(outputs, ensure_ascii=False),
            "expected_seconds": _expected_seconds_for_step_name(name, idx),
        })
    return out


def _candidate_pdf_basenames_from_payload(payload_obj: dict[str, Any]) -> list[str]:
    cands: list[str] = []
    flat = flatten_scalar_fields(payload_obj)
    for v in flat.values():
        if not isinstance(v, str):
            continue
        s = v.strip()
        if ".pdf" not in s.lower():
            continue
        parsed = urlparse(s)
        path_part = parsed.path if parsed.scheme else s
        base = Path(unquote(path_part)).name.strip()
        if base.lower().endswith(".pdf"):
            cands.append(base)
    uniq: list[str] = []
    for x in cands:
        if x not in uniq:
            uniq.append(x)
    return uniq


def set_path_value(root: dict, path: str, value: object) -> None:
    cur: dict = root
    parts = [p for p in path.split(".") if p]
    if not parts:
        return
    for p in parts[:-1]:
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[parts[-1]] = value


def coerce_value(raw: str, value_type: str) -> object:
    if value_type == "none":
        return None
    if value_type == "bool":
        return raw.lower() == "true"
    if value_type == "int":
        return None if raw.strip() == "" else int(raw)
    if value_type == "float":
        return None if raw.strip() == "" else float(raw)
    return raw


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    seconds_int = max(0, int(seconds))
    h = seconds_int // 3600
    m = (seconds_int % 3600) // 60
    s = seconds_int % 60
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _slugify_workflow_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower())
    return slug.strip("_") or "workflow"


def build_assigned_job(workflow_id: str, workflow_name: str, priority: Optional[int]) -> dict[str, Any]:
    return {
        "assigned_job_id": f"assigned_{workflow_id}",
        "workflow_id":     workflow_id,
        "job_name":        f"297384_{_slugify_workflow_name(workflow_name)}",
        "priority":        int(priority or 99),
    }


# ════════════════════════════════════════════════════════════════════════════
# FLASK APPLICATION
# ════════════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════════════════════
# APPLICATION
# ════════════════════════════════════════════════════════════════════════════

def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

    app.jinja_env.tests['search'] = jinja_search
    app.jinja_env.tests['match']  = jinja_match

    from flask import session
    from functools import wraps

    @app.context_processor
    def _inject_helpers():
        member = None
        if session.get("member_id"):
            members = load_members()
            member  = _member_by_id(members, session["member_id"])
        return {
            "format_duration":   format_duration,
            "step_display_name": step_display_name,
            "member": member,
        }

    def login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not session.get("member_id"):
                return redirect(url_for("login"))
            return f(*args, **kwargs)
        return decorated

    # ── Auth routes (duplicated per workflow for isolation) ───────────────────

    @app.get("/login")
    def login():
        if session.get("member_id"):
            return redirect(url_for("manager_dashboard"))
        return render_template("login.html")

    @app.post("/login")
    def login_post():
        email    = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()
        if not email or not password:
            flash("Please enter both email and password.")
            return redirect(url_for("login"))
        members = load_members()
        member  = next((m for m in members if m.get("email","").lower() == email.lower()), None)
        if not member:
            flash("Member not found with that email.")
            return redirect(url_for("login"))
        session["member_id"] = member["member_id"]
        return redirect(url_for("manager_dashboard"))

    @app.get("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.get("/")
    def index():
        if session.get("member_id"):
            return redirect(url_for("manager_dashboard"))
        return redirect(url_for("login"))

    # back-compat stubs so base.html url_for() calls don't 404
    @app.get("/setup")
    def home():
        return redirect(url_for("manager_dashboard"))

    @app.get("/workbench")
    def member_home():
        return redirect(url_for("manager_dashboard"))

    @app.get("/manager")
    @login_required
    def manager_dashboard():  # noqa: C901
        from flask import session as flask_session
        member_id = flask_session.get("member_id")

        members   = load_members()
        member    = _member_by_id(members, member_id) if member_id else None
        member_map = {m["member_id"]: m for m in members}

        wf_data          = load_workflows_data()
        workflows_all    = wf_data.get("workflows", [])
        workflow_members = wf_data.get("workflow_members", [])

        dd         = load_dashboard_data()
        steps      = dd.get("steps", [])
        jobs       = dd.get("jobs", [])
        batches    = dd.get("batches", [])
        batch_steps  = dd.get("batch_steps", [])

        selected_workflow_id      = request.args.get("workflow_id") or flask_session.get("selected_workflow_id")
        selected_assigned_job_id  = request.args.get("assigned_job_id")

        # Build workflow list for this member (fall back to all)
        member_wf_ids = _workflow_ids_for_member(workflow_members, member_id) if member_id else []
        workflow_rows = sorted(
            [w for w in workflows_all if w["workflow_id"] in member_wf_ids],
            key=lambda w: (w.get("priority") or 99, w.get("name") or ""),
        )
        if not workflow_rows:
            workflow_rows = sorted(workflows_all,
                                   key=lambda w: (w.get("priority") or 99, w.get("name") or ""))
        if not workflow_rows:
            flash("No workflows available.", "info")
            return redirect(url_for("member_home"))

        if not selected_workflow_id or not any(w["workflow_id"] == selected_workflow_id for w in workflow_rows):
            flash("Please select a workflow first.", "info")
            return redirect(url_for("member_home"))
        flask_session["selected_workflow_id"] = selected_workflow_id

        selected_workflow = next((w for w in workflow_rows if w["workflow_id"] == selected_workflow_id), None)
        selected_workflow_name = selected_workflow["workflow_id"] if selected_workflow else "Workflow"
        assigned_jobs = [build_assigned_job(
            selected_workflow_id, selected_workflow_name,
            selected_workflow.get("priority") if selected_workflow else None,
        )]
        if not selected_assigned_job_id or not any(j["assigned_job_id"] == selected_assigned_job_id for j in assigned_jobs):
            selected_assigned_job_id = assigned_jobs[0]["assigned_job_id"]

        now = datetime.now(timezone.utc)

        # ── Ensure this workflow is seeded in dashboard_data ─────────────────
        # seed_if_empty runs globally on every request but only writes what is
        # missing.  Calling it again here is a no-op when already seeded.
        seed_if_empty()
        # Reload dd after potential seed
        dd         = load_dashboard_data()
        steps      = dd.get("steps", [])
        batches    = dd.get("batches", [])
        batch_steps  = dd.get("batch_steps", [])

        synthetic_data = _workflow_payload(selected_workflow_id)
        _ds = synthetic_data.get("_dashboard_state", {})

        # ── Core counts ──────────────────────────────────────────────────────
        scoped_batches = _filter_batches(batches, workflow_id=selected_workflow_id)
        total_batches  = len(scoped_batches)
        completed_batches = sum(1 for r in scoped_batches if r.get("status") == "completed")
        failed_batches    = sum(1 for r in scoped_batches if r.get("status") == "failed")
        active_batches    = sum(1 for r in scoped_batches if r.get("status") == "active")

        # ── Timing ──────────────────────────────────────────────────────────
        started_ats   = [parse_dt(r["started_at"]) for r in scoped_batches if r.get("started_at")]
        started_dt    = min(started_ats) if started_ats else None
        elapsed_seconds = (now - started_dt).total_seconds() if started_dt else None

        avg_cycle_seconds = _avg_cycle_seconds(scoped_batches)

        # ── Steps meta — from steps_by_workflow (single source of truth) ─────
        steps_meta = _get_steps_for_workflow(selected_workflow_id, dd)

        expected_cycle_seconds = sum((s.get("expected_seconds") or 300) for s in steps_meta)
        per_record_expected = float(avg_cycle_seconds) if avg_cycle_seconds else float(expected_cycle_seconds)
        expected_total_seconds    = per_record_expected * float(total_batches or 0)
        expected_remaining_seconds = max(0.0, expected_total_seconds - float(elapsed_seconds or 0.0))

        throughput_per_hour = None
        if elapsed_seconds and elapsed_seconds > 0 and completed_batches:
            throughput_per_hour = float(completed_batches) / (float(elapsed_seconds) / 3600.0)
        if not throughput_per_hour and _ds.get("tph"):
            throughput_per_hour = float(_ds["tph"])

        # ── Per-step metrics ─────────────────────────────────────────────────
        scoped_batch_ids  = {r["batch_id"] for r in scoped_batches}
        scoped_active_ids = {r["batch_id"] for r in scoped_batches if r.get("status") == "active"}
        scoped_map = {r["batch_id"]: r for r in scoped_batches}

        step_rows = []
        for st in steps_meta:
            step_order    = int(st["step_order"])
            expected_secs = float(st.get("expected_seconds") or 300)

            records_in_step = sum(
                1 for r in scoped_batches
                if r.get("status") == "active" and int(r.get("current_step_order", -1)) == step_order
            )
            records_completed_step = sum(
                1 for r in scoped_batches
                if int(r.get("current_step_order", -1)) > step_order or r.get("status") == "completed"
            )
            records_unclaimed = sum(
                1 for js in batch_steps
                if js["batch_id"] in scoped_active_ids
                and int(js["step_order"]) == step_order
                and not js.get("completed_at")
                and not js.get("claimed_by")
                and int((scoped_map.get(js["batch_id"]) or {}).get("current_step_order", -1)) == step_order
            )
            step_durations = []
            for js in batch_steps:
                if js["batch_id"] not in scoped_batch_ids:
                    continue
                if int(js["step_order"]) != step_order:
                    continue
                if js.get("completed_at") and js.get("claimed_at"):
                    ca = parse_dt(js["claimed_at"])
                    co = parse_dt(js["completed_at"])
                    if ca and co:
                        step_durations.append((co - ca).total_seconds())
            avg_step_seconds = (sum(step_durations) / len(step_durations)) if step_durations else None
            stale_count = sum(
                1 for js in batch_steps
                if js["batch_id"] in scoped_active_ids
                and int(js["step_order"]) == step_order
                and not js.get("completed_at")
                and js.get("claimed_by")
                and js.get("claimed_at")
                and (now - parse_dt(js["claimed_at"])).total_seconds() > expected_secs * 2
            )

            time_ratio = (
                float(avg_step_seconds) / expected_secs
                if avg_step_seconds is not None and expected_secs else None
            )
            completion_pct = float(records_completed_step) * 100.0 / float(total_batches) if total_batches else 0.0
            active_pct     = float(records_in_step) * 100.0 / float(active_batches) if active_batches else 0.0

            step_rows.append({
                "o":                   step_order,
                "step_id":             st.get("step_id", str(step_order)),
                "step_name":           st.get("step_name", ""),
                "step_name_display":   step_display_name(st.get("step_name", "")),
                "exp":                 expected_secs,
                "active":              records_in_step,
                "unassigned":          records_unclaimed,
                "done":                records_completed_step,
                "avg":                 avg_step_seconds,
                "time_ratio":          time_ratio,
                "completion_pct":      completion_pct,
                "active_pct":          active_pct,
                "stale":               stale_count,
            })

        max_in_step        = max((r["active"] for r in step_rows), default=0)
        max_completed_step = max((r["done"] for r in step_rows), default=0)
        bottleneck_step_order = None
        if max_in_step > 0:
            bottleneck_step_order = next(
                (r["o"] for r in step_rows if r["active"] == max_in_step), None
            )
        total_stuck = sum(r["stale"] for r in step_rows)

        # ── Insights ─────────────────────────────────────────────────────────
        insights = []

        if active_batches > 0:
            scored_steps = [
                (r, (r["active"] / max(active_batches, 1)) * (r.get("time_ratio") or 1.0))
                for r in step_rows if r["active"] > 0
            ]
            if scored_steps:
                worst = max(scored_steps, key=lambda x: x[1])
                ws    = worst[0]
                pct_of_active = round(ws["active"] / active_batches * 100)
                tr    = ws.get("time_ratio")
                detail = f"{ws['active']} of {active_batches} active batches ({pct_of_active}%) are waiting here"
                if tr is not None:
                    detail += f", taking {round(tr,1)}× longer than expected"
                detail += "."
                insights.append({
                    "id":      "bottleneck",
                    "sev":     "warn" if pct_of_active < 70 else "danger",
                    "title":   f"Work is piling up at: {ws['step_name_display']}",
                    "finding": detail,
                    "action":  "Check if this step needs more people assigned or if there is a technical issue slowing it down.",
                    "action_link": None,
                })

        unclaimed_steps = [r for r in step_rows if r["unassigned"] > 0]
        if unclaimed_steps:
            total_unclaimed = sum(r["unassigned"] for r in unclaimed_steps)
            names = ", ".join(r["step_name_display"] for r in unclaimed_steps[:3])
            if len(unclaimed_steps) > 3:
                names += f" and {len(unclaimed_steps)-3} more"
            insights.append({
                "id":      "unclaimed",
                "sev":     "warn",
                "title":   f"{total_unclaimed} job{'s' if total_unclaimed != 1 else ''} waiting — nobody assigned",
                "finding": f"Jobs are sitting at {names} with no team member picked them up yet.",
                "action":  "Assign team members to these steps so work can move forward.",
            })

        member_stats = _member_stats_for_workflow(batch_steps, batches, selected_workflow_id, steps_meta)
        stalled_members = []
        for ms in member_stats:
            open_count = (ms["total_claimed"] or 0) - (ms["total_completed"] or 0)
            if open_count > 0 and (ms["total_completed"] or 0) == 0:
                stalled_members.append({
                    "member":  ms["claimed_by"],
                    "open":    open_count,
                    "max_age": format_duration(ms["max_age_seconds"]),
                    "overdue": ms["overdue_count"] or 0,
                })
        if stalled_members:
            names = ", ".join(f"{m['member']} ({m['open']} open)" for m in stalled_members)
            insights.append({
                "id":      "stalled_members",
                "sev":     "danger" if any(m["overdue"] > 0 for m in stalled_members) else "warn",
                "title":   "Team member(s) have open work but no completions",
                "finding": f"{names} — they have claimed jobs but completed none so far.",
                "action":  "Check in with them directly. They may be blocked or unaware of what to do.",
            })

        moc = _member_open_counts(batch_steps, batches, selected_workflow_id)
        if moc and active_batches > 0:
            total_claimed_open = sum(r["cnt"] for r in moc)
            for m in moc:
                share = m["cnt"] / max(total_claimed_open, 1) * 100
                if share > 70 and m["cnt"] > 2:
                    insights.append({
                        "id":      "spof",
                        "sev":     "warn",
                        "title":   f"{m['claimed_by']} is handling {round(share)}% of all active work",
                        "finding": f"{m['claimed_by']} has {m['cnt']} open jobs — {round(share)}% of everything in progress.",
                        "action":  "Redistribute some of their jobs to other team members to reduce risk.",
                        "action_link": None,
                    })

        if avg_cycle_seconds is not None and expected_cycle_seconds:
            variance_pct = (float(avg_cycle_seconds) - float(expected_cycle_seconds)) / float(expected_cycle_seconds) * 100
            if abs(variance_pct) > 10:
                direction = "slower" if variance_pct > 0 else "faster"
                sev = "warn" if variance_pct > 30 else ("ok" if variance_pct < 0 else "info")
                insights.append({
                    "id":      "pace",
                    "sev":     sev,
                    "title":   f"Jobs completing {abs(round(variance_pct))}% {direction} than planned",
                    "finding": f"Average completion time is {format_duration(int(avg_cycle_seconds))}, expected was {format_duration(int(expected_cycle_seconds))}.",
                    "action":  "Review which steps are taking the most time." if variance_pct > 0 else "Pipeline is ahead of schedule.",
                })

        eta_narrative = None
        if throughput_per_hour and throughput_per_hour > 0 and completed_batches > 0:
            remaining = total_batches - completed_batches
            eta_secs = remaining / throughput_per_hour * 3600
            eta_narrative = f"At current pace ({throughput_per_hour:.1f} jobs/hr), remaining {remaining} job{'s' if remaining != 1 else ''} will finish in ~{format_duration(int(eta_secs))}."
        elif completed_batches == 0 and active_batches > 0:
            eta_narrative = "No jobs have completed yet — ETA will appear once the first jobs finish."

        has_danger = any(i["sev"] == "danger" for i in insights)
        has_warn   = any(i["sev"] == "warn"   for i in insights)
        pipeline_health = (
            {"level": "danger", "label": "Needs immediate attention"} if has_danger else
            {"level": "warn",   "label": "Needs attention"} if has_warn else
            {"level": "ok",     "label": "Running smoothly"}
        )

        blocked_jobs_raw = _blocked_batches_rows(batch_steps, batches, steps_meta, selected_workflow_id)
        blocked_jobs = [
            {
                "batch_id":      r["batch_id"],
                "step_name":   step_display_name(r["step_name"]),
                "claimed_by":  r["claimed_by"],
                "age_seconds": float(r["age_seconds"]),
                "age_display": format_duration(float(r["age_seconds"])),
                "overdue":     float(r["age_seconds"]) > float(r["expected_seconds"]) * 2,
            }
            for r in blocked_jobs_raw
        ]

        view = request.args.get("view", "table")
        if view not in ("table", "cards", "funnel"):
            view = "table"

        # synthetic_data and _ds already loaded at top of this route
        tp_hist   = _ds.get("tpHist")   or [round(throughput_per_hour or 0, 1)] * 4
        tp_labels = _ds.get("tpLabels") or ["-45m", "-30m", "-15m", "now"]

        dashboard_data = {
            "total":       total_batches,
            "completed":   completed_batches,
            "active":      active_batches,
            "failed":      failed_batches,
            "elapsed":     elapsed_seconds or 0.0,
            "elapsed_s":   int(elapsed_seconds or 0),
            "avg_s":       avg_cycle_seconds or per_record_expected,
            "exp_s":       expected_total_seconds,
            "tph":         throughput_per_hour or 0.0,
            "remaining_s": int(expected_remaining_seconds or 0),
            "steps":       step_rows,
            "bn_order":    bottleneck_step_order or -1,
            "insights":    insights,
            "tpHist":      tp_hist,
            "tpLabels":    tp_labels,
        }

        step_names_map = {
            r["o"]: {"n": r["step_name"], "h": r["step_name"].lower().startswith("hypo")}
            for r in step_rows
        }

        project_scope = ""
        wf_obj = _workflow_by_id(load_workflows_data().get("workflows", []), selected_workflow_id)
        if wf_obj:
            project_scope = wf_obj.get("scope") or wf_obj.get("description", "")

        return render_template(
            "manager_dashboard.html",
            workflows=workflow_rows,
            selected_workflow_id=selected_workflow_id,
            selected_workflow_name=selected_workflow_name,
            assigned_jobs=assigned_jobs,
            selected_assigned_job_id=selected_assigned_job_id,
            dashboard_data=json.dumps(dashboard_data),
            step_names_map=json.dumps(step_names_map),
            synthetic_data=json.dumps(synthetic_data),
            project_scope=project_scope,
            view=view,
            total_jobs=total_batches,
            completed_jobs=completed_batches,
            failed_jobs=failed_batches,
            active_jobs=active_batches,
            elapsed_seconds=elapsed_seconds,
            avg_cycle_seconds=avg_cycle_seconds,
            expected_remaining_seconds=expected_remaining_seconds,
            throughput_per_hour=throughput_per_hour,
            steps=step_rows,
            max_in_step=max_in_step or 1,
            max_completed_step=max_completed_step or 1,
            bottleneck_step_order=bottleneck_step_order,
            total_stuck=total_stuck,
            insights=insights,
            blocked_jobs=blocked_jobs,
            eta_narrative=eta_narrative,
            pipeline_health=pipeline_health,
            member=member,
        )

    # ── Member dashboard ─────────────────────────────────────────────────────

    @app.get("/member")
    @login_required
    def member_dashboard():  # noqa: C901
        from flask import session as flask_session
        member_id = flask_session.get("member_id")
        user      = member_id

        selected_batch_id        = request.args.get("batch_id")
        selected_job_id          = request.args.get("job_id")
        selected_assigned_job_id = request.args.get("assigned_job_id")
        selected_workflow_id     = request.args.get("workflow_id") or flask_session.get("selected_workflow_id")

        members   = load_members()
        member    = _member_by_id(members, member_id) if member_id else None

        wf_data          = load_workflows_data()
        workflows_all    = wf_data.get("workflows", [])
        workflow_members = wf_data.get("workflow_members", [])

        dd         = load_dashboard_data()
        steps      = dd.get("steps", [])
        jobs       = dd.get("jobs", [])
        job_members = dd.get("job_members", [])
        batches    = dd.get("batches", [])
        batch_steps  = dd.get("batch_steps", [])

        member_wf_ids = _workflow_ids_for_member(workflow_members, user) if user else []
        workflow_rows = sorted(
            [w for w in workflows_all if w["workflow_id"] in member_wf_ids],
            key=lambda w: (w.get("priority") or 99, w.get("name") or ""),
        )
        if not workflow_rows:
            workflow_rows = sorted(workflows_all,
                                   key=lambda w: (w.get("priority") or 99, w.get("name") or ""))

        if selected_workflow_id and not any(w["workflow_id"] == selected_workflow_id for w in workflow_rows):
            selected_workflow_id = None

        if not selected_workflow_id and selected_batch_id:
            j = _batch_by_id(batches, selected_batch_id)
            if j:
                selected_workflow_id = j["workflow_id"]

        if not selected_workflow_id:
            flash("Please select a workflow first.", "info")
            return redirect(url_for("member_home"))
        flask_session["selected_workflow_id"] = selected_workflow_id

        selected_workflow = _workflow_by_id(workflow_rows, selected_workflow_id)
        assigned_jobs = [build_assigned_job(
            selected_workflow_id,
            selected_workflow["workflow_id"] if selected_workflow else selected_workflow_id,
            selected_workflow.get("priority") if selected_workflow else None,
        )]
        if not selected_assigned_job_id or not any(j["assigned_job_id"] == selected_assigned_job_id for j in assigned_jobs):
            selected_assigned_job_id = assigned_jobs[0]["assigned_job_id"]

        workflow_jobs = sorted(
            [b for b in jobs if b["workflow_id"] == selected_workflow_id],
            key=lambda b: (b.get("priority") or 99, b.get("job_id") or ""),
        )
        valid_job_ids = {b["job_id"] for b in workflow_jobs}
        if selected_job_id and selected_job_id not in valid_job_ids:
            selected_job_id = None

        if selected_workflow_id == "wf_lease_abstraction":
            steps_list = load_steps_from_workflow_json(LEASE_WORKFLOW_PATH)
        else:
            steps_list = sorted(steps, key=lambda s: int(s["step_order"]))

        # Build scope filter
        def _scoped_jobs(jlist):
            r = [j for j in jlist if j.get("workflow_id") == selected_workflow_id]
            if selected_job_id:
                r = [j for j in r if j.get("job_id") == selected_job_id]
            if selected_batch_id:
                r = [j for j in r if j.get("batch_id") == selected_batch_id]
            return r

        scoped_batches = _scoped_jobs(batches)
        scoped_batch_ids = {r["batch_id"] for r in scoped_batches}
        scoped_active_ids = {r["batch_id"] for r in scoped_batches if r.get("status") == "active"}
        batches_map = {r["batch_id"]: r for r in batches}

        steps_out = []
        for st in steps_list:
            step_order = int(st["step_order"])

            available = sum(
                1 for js in batch_steps
                if js["batch_id"] in scoped_active_ids
                and int(js["step_order"]) == step_order
                and int((batches_map.get(js["batch_id"]) or {}).get("current_step_order", -1)) == step_order
                and not js.get("completed_at")
                and not js.get("claimed_by")
            )
            in_progress = sum(
                1 for js in batch_steps
                if js["batch_id"] in scoped_active_ids
                and int(js["step_order"]) == step_order
                and int((batches_map.get(js["batch_id"]) or {}).get("current_step_order", -1)) == step_order
                and not js.get("completed_at")
                and js.get("claimed_by")
            )
            mine = sum(
                1 for js in batch_steps
                if js["batch_id"] in scoped_active_ids
                and int(js["step_order"]) == step_order
                and int((batches_map.get(js["batch_id"]) or {}).get("current_step_order", -1)) == step_order
                and not js.get("completed_at")
                and js.get("claimed_by") == user
            )
            completed = sum(
                1 for r in scoped_batches
                if int(r.get("current_step_order", -1)) > step_order or r.get("status") == "completed"
            )
            completed_by_me = len({
                js["batch_id"] for js in batch_steps
                if js["batch_id"] in scoped_batch_ids
                and int(js["step_order"]) == step_order
                and js.get("claimed_by") == user
                and js.get("completed_at")
            })

            steps_out.append({
                "step_order":        step_order,
                "step_id":           st.get("step_id", str(step_order)),
                "step_name":         st.get("step_name", ""),
                "step_name_display": step_display_name(st.get("step_name", "")),
                "is_ui_step":        str(st.get("step_name") or "").lower().startswith("hypo"),
                "description":       st.get("description") or "",
                "input_list":        json.loads(st.get("input_list_json") or "[]"),
                "output_list":       json.loads(st.get("output_list_json") or "[]"),
                "expected_seconds":  st.get("expected_seconds"),
                "available":         available,
                "in_progress":       in_progress,
                "mine":              mine,
                "completed_by_me":   completed_by_me,
                "completed":         completed,
                "eta_seconds":       0.0 if available else None,
            })

        workflow_job_ids = sorted(
            [j["job_id"] for j in jobs if j.get("workflow_id") == selected_workflow_id]
        )

        step_by_order = {int(s["step_order"]): s for s in steps_list}

        my_jobs_raw = sorted(
            [
                js for js in batch_steps
                if js["batch_id"] in scoped_active_ids
                and not js.get("completed_at")
                and js.get("claimed_by") == user
            ],
            key=lambda js: js.get("claimed_at") or "",
        )
        my_jobs = []
        for js in my_jobs_raw:
            r = batches_map.get(js["batch_id"])
            if not r:
                continue
            sobj = _step_by_order(steps_list, int(js["step_order"]))
            my_jobs.append({
                "batch_id":             js["batch_id"],
                "workflow_id":         r.get("workflow_id"),
                "step_order":          js["step_order"],
                "step_name":           sobj.get("step_name", "") if sobj else "Step",
                "step_display_name":   step_display_name(sobj.get("step_name", "") if sobj else "Step"),
                "claimed_at_display": format_duration((now - parse_dt(js["claimed_at"])).total_seconds()) if js.get("claimed_at") else "",
            })

        completed_doc_ids = {r["batch_id"] for r in scoped_batches if r.get("status") == "completed"}

        selected_job          = None
        selected_step         = None
        selected_job_step     = None
        selected_step_name_display = None
        selected_input_list   = []
        selected_output_list  = []
        selected_is_ui_step   = False
        selected_payload_json = "{}"
        selected_member_notes = ""
        selected_business_fields = []

        if selected_job_id:
            j = _batch_by_id(jobs, selected_job_id)
            if (j and j.get("status") == "active"
                    and (not selected_workflow_id or j.get("workflow_id") == selected_workflow_id)
                    and (not selected_job_id or j.get("batch_id") == selected_job_id)):
                so   = int(j["current_step_order"])
                js_r = _batch_step(batch_steps, selected_job_id, so)
                if js_r and not js_r.get("completed_at") and js_r.get("claimed_by") == user:
                    selected_job = {
                        "batch_id":             j["batch_id"],
                        "status":             j["status"],
                        "current_step_order": so,
                        "claimed_by":         js_r.get("claimed_by"),
                        "claimed_at":         js_r.get("claimed_at"),
                    }
                    st_meta = step_by_order.get(so) or {}
                    selected_step = {
                        "step_id":         st_meta.get("step_id", str(so)),
                        "step_name":       st_meta.get("step_name", "Unknown Step"),
                        "description":     st_meta.get("description") or "",
                        "expected_seconds": st_meta.get("expected_seconds", 300),
                    }
                    selected_job_step = {
                        "payload_json": js_r.get("payload_json") or "{}",
                        "member_notes": js_r.get("member_notes") or "",
                    }
                    selected_step_name_display = step_display_name(selected_step["step_name"])
                    selected_input_list  = json.loads(st_meta.get("input_list_json")  or "[]")
                    selected_output_list = json.loads(st_meta.get("output_list_json") or "[]")
                    selected_is_ui_step  = str(selected_step["step_name"]).lower().startswith("hypo")
                    selected_payload_json = selected_job_step["payload_json"]
                    selected_member_notes = selected_job_step["member_notes"]
                    try:
                        payload_obj = json.loads(selected_payload_json)
                    except json.JSONDecodeError:
                        payload_obj = {}
                    flat = flatten_scalar_fields(payload_obj)
                    for path, value in sorted(flat.items()):
                        vt = _value_type(value)
                        selected_business_fields.append({
                            "path":       path,
                            "label":      label_from_path(path),
                            "value_type": vt,
                            "value":      "—" if value is None else str(value),
                        })

        return render_template(
            "member.html",
            user=user,
            selected_job_id=selected_job_id,
            selected_workflow_id=selected_workflow_id,
            assigned_jobs=assigned_jobs,
            selected_assigned_job_id=selected_assigned_job_id,
            workflow_jobs=workflow_jobs,
            workflow_job_ids=workflow_job_ids,
            steps=steps_out,
            my_jobs=my_jobs,
            selected_job=selected_job,
            selected_step=selected_step,
            selected_job_step=selected_job_step,
            selected_step_name_display=selected_step_name_display,
            selected_input_list=selected_input_list,
            selected_output_list=selected_output_list,
            selected_is_ui_step=selected_is_ui_step,
            selected_payload_json=selected_payload_json,
            selected_member_notes=selected_member_notes,
            selected_business_fields=selected_business_fields,
            claimed_completed_count=len(completed_doc_ids),
            workflows=workflow_rows,
            member=member,
        )

    # ── Member Claim ─────────────────────────────────────────────────────────

    @app.post("/member/claim")
    def member_claim():
        from flask import session as flask_session
        user         = request.form.get("user") or "member1"
        batch_id     = request.form.get("batch_id")
        workflow_id  = request.form.get("workflow_id")
        assigned_job_id = request.form.get("assigned_job_id")
        try:
            step_order = int(request.form.get("step_order", ""))
        except ValueError:
            flash("Invalid step.", "error")
            return redirect(url_for("member_dashboard", user=user, batch_id=batch_id,
                                    workflow_id=workflow_id, assigned_job_id=assigned_job_id))

        dd        = load_dashboard_data()
        steps     = dd.get("steps", [])
        jobs      = dd.get("jobs", [])
        batches   = dd.get("batches", [])
        batch_steps = dd.get("batch_steps", [])

        if workflow_id == "wf_lease_abstraction":
            lease_steps = load_steps_from_workflow_json(LEASE_WORKFLOW_PATH)
            row = next((s for s in lease_steps if int(s["step_order"]) == step_order), None)
            step_name = row["step_name"] if row else None
        else:
            st = _step_by_order(steps, step_order)
            step_name = st["step_name"] if st else None

        if not step_name or not str(step_name).lower().startswith("hypo"):
            flash("This step is automated and cannot be claimed.", "info")
            return redirect(url_for("member_dashboard", user=user, batch_id=batch_id,
                                    workflow_id=workflow_id, assigned_job_id=assigned_job_id))

        # Find the first unclaimed job at this step
        candidates = [
            js for js in batch_steps
            if int(js["step_order"]) == step_order
            and not js.get("completed_at")
            and not js.get("claimed_by")
        ]
        if workflow_id:
            candidates = [js for js in candidates
                          if (_batch_by_id(batches, js["batch_id"]) or {}).get("workflow_id") == workflow_id]
        if batch_id:
            candidates = [js for js in candidates
                          if (_batch_by_id(batches, js["batch_id"]) or {}).get("batch_id") == batch_id]
        candidates.sort(key=lambda js: (
            (_batch_by_id(batches, js["batch_id"]) or {}).get("created_at") or "",
            js["batch_id"],
        ))

        if not candidates:
            flash("No available job to claim for this step.", "info")
            return redirect(url_for("member_dashboard", user=user, batch_id=batch_id,
                                    workflow_id=workflow_id, assigned_job_id=assigned_job_id))

        target_batch_id = candidates[0]["batch_id"]
        now = utc_now()
        for js in batch_steps:
            if js["batch_id"] == target_batch_id and int(js["step_order"]) == step_order:
                if not js.get("claimed_by") and not js.get("completed_at"):
                    js["claimed_by"] = user
                    js["claimed_at"] = now
        for j in batches:
            if j["batch_id"] == target_batch_id:
                j["current_step_order"] = step_order
        save_dashboard_data(dd)
        return redirect(url_for("member_batch", batch_id=target_batch_id, user=user))

    # ── Claim Next ───────────────────────────────────────────────────────────

    @app.get("/member/claim-next")
    def member_claim_next():
        user         = request.args.get("user", "member1")
        batch_id     = request.args.get("batch_id")
        workflow_id  = request.args.get("workflow_id")
        assigned_job_id = request.args.get("assigned_job_id")
        try:
            step_order = int(request.args.get("step_order", ""))
        except ValueError:
            return redirect(url_for("member_dashboard", user=user, batch_id=batch_id,
                                    workflow_id=workflow_id, assigned_job_id=assigned_job_id))

        dd        = load_dashboard_data()
        steps     = dd.get("steps", [])
        jobs      = dd.get("jobs", [])
        batches   = dd.get("batches", [])
        batch_steps = dd.get("batch_steps", [])

        if workflow_id == "wf_lease_abstraction":
            lease_steps = load_steps_from_workflow_json(LEASE_WORKFLOW_PATH)
            row = next((s for s in lease_steps if int(s["step_order"]) == step_order), None)
            step_name = row["step_name"] if row else None
        else:
            st = _step_by_order(steps, step_order)
            step_name = st["step_name"] if st else None

        if not step_name or not str(step_name).lower().startswith("hypo"):
            flash("This step is automated and cannot be claimed.", "info")
            return redirect(url_for("member_dashboard", user=user, batch_id=batch_id,
                                    workflow_id=workflow_id, assigned_job_id=assigned_job_id))

        candidates = [
            js for js in batch_steps
            if int(js["step_order"]) == step_order
            and not js.get("completed_at")
            and not js.get("claimed_by")
        ]
        active_batch_ids = {r["batch_id"] for r in batches if r.get("status") == "active"}
        candidates = [js for js in candidates if js["batch_id"] in active_batch_ids]
        if workflow_id:
            candidates = [js for js in candidates
                          if (_batch_by_id(batches, js["batch_id"]) or {}).get("workflow_id") == workflow_id]
        if batch_id:
            candidates = [js for js in candidates
                          if (_batch_by_id(batches, js["batch_id"]) or {}).get("batch_id") == batch_id]
        candidates.sort(key=lambda js: (
            (_batch_by_id(batches, js["batch_id"]) or {}).get("created_at") or "",
            js["batch_id"],
        ))
        if not candidates:
            flash("No more available jobs for this step.", "info")
            return redirect(url_for("member_dashboard", user=user, batch_id=batch_id,
                                    workflow_id=workflow_id, assigned_job_id=assigned_job_id))

        target_batch_id = candidates[0]["batch_id"]
        now = utc_now()
        for js in batch_steps:
            if js["batch_id"] == target_batch_id and int(js["step_order"]) == step_order:
                if not js.get("claimed_by") and not js.get("completed_at"):
                    js["claimed_by"] = user
                    js["claimed_at"] = now
        for r in batches:
            if r["batch_id"] == target_batch_id:
                r["current_step_order"] = step_order
        save_dashboard_data(dd)
        return redirect(url_for("member_batch", batch_id=target_batch_id, user=user))

    # ── HITL Job Dispatcher (/job?embed=1&step=<name>) ───────────────────────
    # Called by the HITL iframe modal in manager_dashboard.
    # Resolves step name → finds best available batch at that step →
    # auto-claims if unclaimed → redirects to member_batch?embed=1.

    @app.get("/job")
    @login_required
    def job():
        step_name   = request.args.get("step", "").strip()
        embed       = request.args.get("embed", "0")
        workflow_id = session.get("selected_workflow_id")
        user        = session.get("member_id", "member1")

        if not step_name or not workflow_id:
            flash("Missing step or workflow context.")
            return redirect(url_for("manager_dashboard"))

        dd          = load_dashboard_data()
        batches     = dd.get("batches", [])
        batch_steps = dd.get("batch_steps", [])
        steps_meta  = _get_steps_for_workflow(workflow_id, dd)

        # Resolve step name → step_order (case-insensitive to be safe)
        step_obj = next(
            (s for s in steps_meta
             if s.get("step_name", "").lower() == step_name.lower()),
            None,
        )
        if not step_obj:
            flash(f"Step '{step_name}' not found.")
            return redirect(url_for("manager_dashboard"))
        step_order = int(step_obj["step_order"])

        # Active batches currently at this step for this workflow
        active_batch_ids = {
            b["batch_id"] for b in batches
            if b.get("workflow_id") == workflow_id
            and b.get("status") == "active"
            and int(b.get("current_step_order", -1)) == step_order
        }

        # Priority: unclaimed first → already claimed by this user → anyone else
        def _priority(js):
            if not js.get("claimed_by"):     return 0
            if js.get("claimed_by") == user: return 1
            return 2

        candidates = sorted(
            [js for js in batch_steps
             if js["batch_id"] in active_batch_ids
             and int(js["step_order"]) == step_order
             and not js.get("completed_at")],
            key=_priority,
        )

        if not candidates:
            flash(f"No active jobs at step '{step_name}'.")
            return redirect(url_for("manager_dashboard"))

        target = candidates[0]
        # Auto-claim if unclaimed
        if not target.get("claimed_by"):
            target["claimed_by"] = user
            target["claimed_at"] = utc_now()
            for b in batches:
                if b["batch_id"] == target["batch_id"]:
                    b["current_step_order"] = step_order
            save_dashboard_data(dd)

        return redirect(url_for("member_batch",
                                batch_id=target["batch_id"],
                                user=user, embed=embed))

    # ── Member Job view ──────────────────────────────────────────────────────

    @app.get("/member/batch/<batch_id>")
    def member_batch(batch_id: str):
        user     = request.args.get("user", "member1")
        embedded = request.args.get("embed") == "1"

        members   = load_members()
        member_map = {m["member_id"]: m for m in members}

        wf_data    = load_workflows_data()
        workflows  = wf_data.get("workflows", [])

        dd         = load_dashboard_data()
        steps      = dd.get("steps", [])
        jobs       = dd.get("jobs", [])
        batches    = dd.get("batches", [])
        batch_steps  = dd.get("batch_steps", [])

        job = _batch_by_id(batches, batch_id)
        if not job:
            flash("Job not found.", "error")
            return redirect(url_for("member_dashboard", user=user))

        workflow_id = job["workflow_id"]
        if workflow_id == "wf_lease_abstraction":
            lease_steps = load_steps_from_workflow_json(LEASE_WORKFLOW_PATH)
            step = next((s for s in lease_steps if int(s["step_order"]) == int(job["current_step_order"])), None)
        else:
            step = _step_by_order(steps, int(job["current_step_order"]))
        if not step:
            flash("Step not found.", "error")
            return redirect(url_for("member_dashboard", user=user,
                                    batch_id=job["batch_id"], workflow_id=workflow_id))

        js = _batch_step(batch_steps, batch_id, int(job["current_step_order"]))
        if not js:
            flash("Job step not found.", "error")
            return redirect(url_for("member_dashboard", user=user,
                                    batch_id=job["batch_id"], workflow_id=workflow_id))

        can_edit   = js.get("claimed_by") in (None, user)
        input_list  = json.loads(step.get("input_list_json") or "[]")
        output_list = json.loads(step.get("output_list_json") or "[]")
        is_ui_step  = str(step.get("step_name") or "").lower().startswith("hypo")

        try:
            payload_obj = json.loads(js.get("payload_json") or "{}")
        except json.JSONDecodeError:
            payload_obj = {}

        flat = flatten_scalar_fields(payload_obj)
        business_fields = []
        for path, value in sorted(flat.items()):
            encoded = path.replace(".", "__DOT__")
            vt = _value_type(value)
            business_fields.append({
                "path":       path,
                "label":      label_from_path(path),
                "name":       f"f__{encoded}",
                "type_name":  f"t__{encoded}",
                "value_type": vt,
                "value":      "" if value is None else str(value),
            })

        wf = _workflow_by_id(workflows, workflow_id)
        workflow_name = wf["name"] if wf else "Unknown Workflow"
        sup = member_map.get((wf or {}).get("supervisor_id") or "", {})
        workflow_supervisor_name = sup.get("name") or (wf or {}).get("supervisor_id") or ""

        batch = _batch_by_id(jobs, job.get("job_id") or job.get("batch_id") or "")
        batch_display_name   = batch["name"] if batch else str(job.get("job_id") or job.get("batch_id") or "")
        batch_sup = member_map.get((batch or {}).get("supervisor_id") or "", {})
        batch_supervisor_name = batch_sup.get("name") or (batch or {}).get("supervisor_id") or workflow_supervisor_name

        logical_job_name = f"297384_{_slugify_workflow_name(workflow_name)}"

        # HITL KPI
        hitl_kpi = {"actions": 0, "completed": 0, "pending": 0, "estimated_minutes": 0}
        step_order_kpi  = int(job["current_step_order"])
        expected_secs_kpi = int(step.get("expected_seconds") or 300)
        if is_ui_step:
            wf_job_ids = {j.get("batch_id") for j in batches if j.get("workflow_id") == workflow_id}
            active_wf_ids = {j.get("batch_id") for j in batches if j.get("workflow_id") == workflow_id and j.get("status") == "active"}
            in_progress_step = sum(
                1 for js2 in batch_steps
                if js2["batch_id"] in active_wf_ids
                and int(js2["step_order"]) == step_order_kpi
                and int((_batch_by_id(batches, js2["batch_id"]) or {}).get("current_step_order", -1)) == step_order_kpi
                and not js2.get("completed_at")
                and js2.get("claimed_by")
            )
            mine_pending_step = sum(
                1 for js2 in batch_steps
                if js2["batch_id"] in active_wf_ids
                and int(js2["step_order"]) == step_order_kpi
                and int((_batch_by_id(batches, js2["batch_id"]) or {}).get("current_step_order", -1)) == step_order_kpi
                and not js2.get("completed_at")
                and js2.get("claimed_by") == user
            )
            completed_by_me_step = sum(
                1 for js2 in batch_steps
                if js2["batch_id"] in wf_job_ids
                and int(js2["step_order"]) == step_order_kpi
                and js2.get("claimed_by") == user
                and js2.get("completed_at")
            )
            est_minutes = int(round((mine_pending_step * expected_secs_kpi) / 60.0))
            if mine_pending_step > 0 and est_minutes == 0:
                est_minutes = 1
            hitl_kpi = {
                "actions":          in_progress_step,
                "completed":        completed_by_me_step,
                "pending":          mine_pending_step,
                "estimated_minutes": est_minutes,
            }

        total_steps = max(len(steps) if steps else 1, 1)
        current_step_index = min(max(int(job["current_step_order"]) + 1, 1), total_steps)
        is_lease_workflow  = workflow_id == "wf_lease_abstraction"

        pdf_documents: list[dict[str, str]] = []
        selected_document_url = ""
        if is_lease_workflow and INPUT_FILES_DIR.exists():
            lease_pdfs = sorted(INPUT_FILES_DIR.glob("*.pdf"), key=lambda p: p.name.lower())
            name_to_url: dict[str, str] = {}
            for idx, p in enumerate(lease_pdfs):
                stat    = p.stat()
                doc_url = url_for("input_file", filename=p.name)
                pdf_documents.append({
                    "idx":       str(idx),
                    "name":      p.name,
                    "url":       doc_url,
                    "modified":  datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d"),
                    "size_kb":   str(max(1, int(round(stat.st_size / 1024.0)))),
                })
                name_to_url[p.name.lower()] = doc_url
            if pdf_documents:
                payload_names = _candidate_pdf_basenames_from_payload(payload_obj)
                for nm in payload_names:
                    if nm.lower() in name_to_url:
                        selected_document_url = name_to_url[nm.lower()]
                        break
                if not selected_document_url:
                    try:
                        lease_num = int(re.sub(r"[^0-9]", "", batch_id) or "1")
                    except ValueError:
                        lease_num = 1
                    selected_document_url = pdf_documents[(lease_num - 1) % len(pdf_documents)]["url"]

        return render_template(
            "job.html",
            user=user,
            job=job,
            step=step,
            workflow_name=workflow_name,
            current_step_index=current_step_index,
            total_steps=total_steps,
            is_lease_workflow=is_lease_workflow,
            pdf_documents=pdf_documents,
            selected_document_url=selected_document_url,
            step_name_display=step_display_name(step["step_name"]),
            job_step=js,
            can_edit=can_edit,
            input_list=input_list,
            output_list=output_list,
            business_fields=business_fields,
            is_ui_step=is_ui_step,
            hitl_kpi=hitl_kpi,
            logical_job_name=logical_job_name,
            batch_display_name=batch_display_name,
            batch_supervisor_name=batch_supervisor_name,
            embedded=embedded,
        )

    # ── Job Save ─────────────────────────────────────────────────────────────

    @app.post("/member/batch/<batch_id>/save")
    def member_batch_save(batch_id: str):
        user = request.form.get("user", "member1")

        dd        = load_dashboard_data()
        batches   = dd.get("batches", [])
        batch_steps = dd.get("batch_steps", [])

        job = _batch_by_id(batches, batch_id)
        if not job:
            flash("Job not found.", "error")
            return redirect(url_for("member_dashboard", user=user))

        step_order = int(job["current_step_order"])
        js = _batch_step(batch_steps, batch_id, step_order)
        if not js:
            flash("Job step not found.", "error")
            return redirect(url_for("member_dashboard", user=user,
                                    batch_id=job["batch_id"], workflow_id=job["workflow_id"]))
        if js.get("claimed_by") not in (None, user):
            flash("Job is claimed by another member.", "error")
            return redirect(url_for("member_batch", batch_id=batch_id, user=user))

        notes     = request.form.get("member_notes", "")
        edit_mode = request.form.get("edit_mode", "simple")
        parsed: dict = {}
        if edit_mode == "json":
            payload_text = request.form.get("payload_json", "").strip() or "{}"
            try:
                parsed = json.loads(payload_text)
            except json.JSONDecodeError as e:
                flash(f"Payload JSON is invalid: {e}", "error")
                return redirect(url_for("member_batch", batch_id=batch_id, user=user))
        else:
            try:
                parsed = json.loads(js.get("payload_json") or "{}")
            except json.JSONDecodeError:
                parsed = {}
            for key in request.form.keys():
                if not key.startswith("f__"):
                    continue
                encoded    = key[len("f__"):]
                path       = encoded.replace("__DOT__", ".")
                value_type = request.form.get(f"t__{encoded}", "str")
                raw_val    = request.form.get(key, "")
                try:
                    value = coerce_value(raw_val, value_type)
                except ValueError as e:
                    flash(f"Invalid value for {label_from_path(path)}: {e}", "error")
                    return redirect(url_for("member_batch", batch_id=batch_id, user=user))
                set_path_value(parsed, path, value)

        normalized = json.dumps(parsed, indent=2, ensure_ascii=False)
        js["payload_json"]  = normalized
        js["member_notes"]  = notes
        save_dashboard_data(dd)
        flash("Saved.", "success")
        return redirect(url_for("member_batch", batch_id=batch_id, user=user))

    # ── Job Complete ─────────────────────────────────────────────────────────

    @app.post("/member/batch/<batch_id>/complete")
    def member_batch_complete(batch_id: str):
        user = request.form.get("user", "member1")
        claim_next      = request.form.get("claim_next") == "1"
        assigned_job_id = request.form.get("assigned_job_id")

        dd        = load_dashboard_data()
        steps     = dd.get("steps", [])
        batches   = dd.get("batches", [])
        batch_steps = dd.get("batch_steps", [])

        job = _batch_by_id(batches, batch_id)
        if not job:
            flash("Job not found.", "error")
            return redirect(url_for("member_dashboard", user=user))

        step_order = int(job["current_step_order"])
        js = _batch_step(batch_steps, batch_id, step_order)
        if not js:
            flash("Job step not found.", "error")
            return redirect(url_for("member_dashboard", user=user,
                                    batch_id=job["batch_id"], workflow_id=job["workflow_id"]))
        if js.get("claimed_by") != user:
            flash("You must claim this job before completing it.", "error")
            return redirect(url_for("member_batch", batch_id=batch_id, user=user))

        now = utc_now()
        if not js.get("completed_at"):
            js["completed_at"] = now

        if job["workflow_id"] == "wf_lease_abstraction":
            lease_steps  = load_steps_from_workflow_json(LEASE_WORKFLOW_PATH)
            last_step_order = (len(lease_steps) - 1) if lease_steps else 0
        else:
            step_orders = [int(s["step_order"]) for s in steps]
            last_step_order = max(step_orders) if step_orders else 0

        next_step_order = step_order + 1
        for j in jobs:
            if j["batch_id"] == batch_id:
                if next_step_order > last_step_order:
                    j["status"]       = "completed"
                    j["completed_at"] = now
                else:
                    j["current_step_order"] = next_step_order
                    if next_step_order == last_step_order:
                        j["status"]       = "completed"
                        j["completed_at"] = now

        save_dashboard_data(dd)
        flash("Marked complete.", "success")

        if claim_next:
            return redirect(url_for("member_claim_next", user=user, step_order=step_order,
                                    workflow_id=job["workflow_id"], assigned_job_id=assigned_job_id))
        return redirect(url_for("member_dashboard", user=user,
                                workflow_id=job["workflow_id"], assigned_job_id=assigned_job_id))

    # ── Static / API routes ──────────────────────────────────────────────────

    @app.get("/input-files/<path:filename>")
    def input_file(filename: str):
        if not filename.lower().endswith(".pdf"):
            abort(404)
        if not INPUT_FILES_DIR.exists():
            abort(404)
        return send_from_directory(str(INPUT_FILES_DIR), filename)

    @app.get("/api/sample/workflow")
    def get_sample_workflow():
        if LEASE_WORKFLOW_PATH.exists():
            return json.loads(LEASE_WORKFLOW_PATH.read_text(encoding="utf-8"))
        return {"error": "Sample workflow not found"}, 404

    @app.get("/api/sample/bots")
    def get_sample_bots():
        # BOT_DESC_PATH was removed; return empty list or specific error
        return {"error": "Sample bots data no longer available"}, 404

    @app.post("/api/save_config")
    def save_config():
        data = request.json
        if not data:
            return {"error": "No data provided"}, 400

        wf_json = data.get("workflow")
        scope   = data.get("scope", "").strip()

        if wf_json:
            UPLOADED_WORKFLOW_PATH.write_text(json.dumps(wf_json), encoding="utf-8")
        # scope is stored per-workflow in workflows.json — no separate file needed

        # ── Derive a workflow name from the uploaded JSON or fall back to scope ──
        # If derived from scope, only take the first 40 chars for the name
        wf_name = (
            wf_json.get("name")
            or wf_json.get("workflow_name")
            or (scope[:40] if scope else None)
            or "Unnamed Workflow"
        ) if wf_json else (scope[:40] if scope else "Unnamed Workflow")

        # ── Build a stable workflow_id from the name ──────────────────────────
        import re as _re
        # Shortened ID generation: slugify + max 30 chars
        wf_id_slug = _re.sub(r"[^a-z0-9]+", "_", wf_name.lower()).strip("_")
        wf_id = "wf_" + wf_id_slug[:25]
        if not wf_id_slug:
            import time
            wf_id = f"wf_{int(time.time()) % 100000}"

        # ── Register workflow + assign to the logged-in user ──────────────────
        creator_id = session.get("member_id")
        wf_data   = load_workflows_data()
        workflows  = wf_data.get("workflows", [])
        wf_members = wf_data.get("workflow_members", [])

        if not _workflow_by_id(workflows, wf_id):
            workflows.append({
                "workflow_id":      wf_id,
                "name":             wf_name,
                "description":      wf_name,
                "scope":            scope,
                "priority":         len(workflows) + 1,
                "status":           "active",
                "supervisor_id":    creator_id,
                "est_time_seconds": 0,
            })
        else:
            # Update scope on existing workflow if re-uploaded
            for w in workflows:
                if w["workflow_id"] == wf_id:
                    w["scope"] = scope
                    break

        if creator_id and not any(
            wm["workflow_id"] == wf_id and wm["member_id"] == creator_id
            for wm in wf_members
        ):
            wf_members.append({"workflow_id": wf_id, "member_id": creator_id})

        save_workflows_data({"workflows": workflows, "workflow_members": wf_members})

        # ── Clear only the steps/jobs for this workflow so a fresh seed runs ──
        # Batches, jobs, batch_steps from other workflows are preserved.
        dd = load_dashboard_data()
        existing_wf_ids = {w["workflow_id"] for w in workflows if w["workflow_id"] != wf_id}
        dd["steps"]     = []   # steps are global to uploaded workflow config, always reset
        dd["batches"]      = [j  for j in dd.get("batches",      []) if j.get("workflow_id") in existing_wf_ids]
        dd["batch_steps"] = [js for js in dd.get("batch_steps", []) if js.get("batch_id") in {j.get("batch_id") for j in dd["batches"]}]
        save_dashboard_data(dd)

        # ── Ensure synthetic payload exists for this workflow ─────────────────
        # If no custom synthetic data has been provided for wf_id, clone from an
        # existing payload so the dashboard is immediately populated.
        # When the user later provides real synthetic data they can call:
        #   _inject_workflow_payload(wf_id, custom_payload=their_data)
        master = _load_master_synthetic()
        if wf_id not in master.get("workflow_payloads", {}):
            _inject_workflow_payload(wf_id, master=master, save=True)

        return {"status": "success", "workflow_id": wf_id}

    @app.post("/api/chat-ui-command")
    def chat_ui_command():
        data = request.get_json(silent=True) or {}
        text = str(data.get("text") or "").strip()
        if not text:
            return {"ok": False, "message": "Empty command."}, 400

        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        model   = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash").strip()
        if not api_key:
            return {"ok": False, "message": "GEMINI_API_KEY missing in .env"}, 400

        prompt = (
            "Convert the user message into one UI action JSON only. "
            "Allowed types: set_left_pct, delta_left_pct, fit_pdf_width, collapse_center, expand_center, reset_layout. "
            "Rules: set_left_pct value must be integer 45..90. delta_left_pct value must be integer -20..20. "
            "If unsupported/unsafe, return {\"type\":\"unsupported\"}. "
            "Return strict JSON object with keys: type, value(optional). "
            f"User message: {text}"
        )
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json"},
        }
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        req = urlrequest.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=20, context=_gemini_ssl_context()) as resp:
                raw = resp.read().decode("utf-8")
        except urlerror.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            return {"ok": False, "message": f"Gemini HTTP error: {e.code}", "detail": body[:400]}, 502
        except ssl.SSLCertVerificationError as e:
            return {"ok": False,
                    "message": "Gemini SSL verification failed.",
                    "detail": str(e)}, 502
        except Exception as e:
            return {"ok": False, "message": f"Gemini request failed: {str(e).strip() or repr(e)}"}, 502

        try:
            out    = json.loads(raw)
            txt    = out["candidates"][0]["content"]["parts"][0]["text"]
            action = json.loads(txt)
        except Exception:
            return {"ok": False, "message": "Invalid Gemini response format."}, 502

        allowed = {"set_left_pct", "delta_left_pct", "fit_pdf_width",
                   "collapse_center", "expand_center", "reset_layout"}
        action_type = str(action.get("type") or "").strip()
        if action_type not in allowed:
            return {"ok": False, "message": "Unsupported command."}, 200
        value = action.get("value")
        if action_type == "set_left_pct":
            try:
                value = max(45, min(90, int(value)))
            except Exception:
                return {"ok": False, "message": "Invalid width value."}, 200
            action["value"] = value
        if action_type == "delta_left_pct":
            try:
                value = max(-20, min(20, int(value)))
            except Exception:
                return {"ok": False, "message": "Invalid delta value."}, 200
            action["value"] = value
        return {"ok": True, "action": action}

    @app.post("/api/layout-edit")
    def api_layout_edit():
        data        = request.get_json(silent=True) or {}
        instruction = str(data.get("instruction") or "").strip()
        manifest    = data.get("manifest") or {}

        if not instruction:
            return {"ok": False, "message": "Instruction is required."}, 400
        if not manifest:
            return {"ok": False, "message": "Manifest is required."}, 400

        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        model   = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash").strip()
        if not api_key:
            return {"ok": False, "message": "GEMINI_API_KEY missing in .env"}, 400

        system_prompt = """\
You are a UI developer assistant for XAPP, a HITL document review tool.
You receive a page manifest and a user instruction. Return a JSON array of action objects.

Action types:
  {"action":"set_css_var",   "target":"--var",         "value":"..."}
  {"action":"set_attribute", "target":"css-selector",  "attribute":"...", "value":"..."}
  {"action":"set_style",     "target":"css-selector",  "property":"...",  "value":"..."}
  {"action":"insert_html",   "target":"css-selector",  "position":"beforeend|afterbegin", "html":"..."}
  {"action":"replace_html",  "target":"css-selector",  "html":"..."}
  {"action":"remove_element","target":"css-selector"}
  {"action":"add_class",     "target":"css-selector",  "class":"..."}
  {"action":"remove_class",  "target":"css-selector",  "class":"..."}

Rules:
- Only use IDs/vars from the manifest. Prefer #id selectors.
- Theme toggle: set_attribute on "html" {attribute:"data-theme", value:"dark"|"light"}. The CSS vars are already defined for both themes — do NOT set individual colour vars for theme changes.
- Accent colour: set_css_var for --blue, --blue-bg, --blue-bd only.
- New KPI card: the manifest includes kpi_card_html — clone it EXACTLY (same classes, same structure), only change the label text, value id, and sub text. Insert into ".ctx-strip" with position "beforeend". Also set_style on ".ctx-strip" {property:"gridTemplateColumns", value:"repeat(N,minmax(0,1fr))"} where N = new total count.
- Panel width: set_style on "#panel-left" and "#panel-center" (property:"width").
- New widget: insert_html with self-contained HTML using CSS classes/vars from the manifest design tokens.
- Return ONLY the JSON array. No explanation, no markdown.\
"""

        user_prompt = (
            f"Page manifest:\n{json.dumps(manifest, indent=2)}\n\n"
            f"Instruction: {instruction}"
        )

        payload = {
            "contents": [{"role": "user", "parts": [{"text": system_prompt + "\n\n" + user_prompt}]}],
            "generationConfig": {"responseMimeType": "application/json"},
        }
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        req = urlrequest.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=45, context=_gemini_ssl_context()) as resp:
                raw = resp.read().decode("utf-8")
        except urlerror.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            return {"ok": False, "message": f"Gemini HTTP error: {e.code}", "detail": body[:400]}, 502
        except ssl.SSLCertVerificationError as e:
            return {"ok": False,
                    "message": "Gemini SSL verification failed. Install/upgrade certifi.",
                    "detail": str(e)}, 502
        except Exception as e:
            return {"ok": False, "message": f"Gemini request failed: {str(e).strip() or repr(e)}"}, 502

        try:
            out     = json.loads(raw)
            txt     = out["candidates"][0]["content"]["parts"][0]["text"].strip()
            # Strip markdown fences if model wrapped the JSON
            if txt.startswith("```"):
                txt = txt.replace("```json", "").replace("```", "").strip()
            actions = json.loads(txt)
            if isinstance(actions, dict):
                actions = [actions]
        except Exception:
            return {"ok": False, "message": "Invalid JSON from model."}, 502

        if not isinstance(actions, list):
            return {"ok": False, "message": "Model did not return an actions array."}, 502

        return {"ok": True, "actions": actions}

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=PORT, debug=True)
