"""
master_dashboard/app.py
════════════════════════════════════════════════════════════════════════════
Port 5000 — always running on your server.

Responsibilities:
  - Login / Logout
  - /workbench   — list all workflows (cards link to their Lambda URLs)
  - /setup       — upload workflow JSON + Generate XAPP form
  - POST /api/generate  — scaffold folder + deploy to Lambda via generate_xapp.py
  - POST /api/redeploy  — redeploy an existing workflow (rebuild + re-upload)

Phase 2: workflow cards link directly to their Lambda URL.
No subprocess launch/stop — Lambda manages its own lifecycle.
════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import os
import sys
from functools import wraps
from pathlib import Path

from flask import (
    Flask, flash, jsonify, redirect,
    render_template, request, session, url_for,
)

# ── Paths ─────────────────────────────────────────────────────────────────────
MASTER_DIR    = Path(__file__).resolve().parent
PLATFORM_ROOT = MASTER_DIR.parent
PLATFORM_CORE = PLATFORM_ROOT / "platform_core"

sys.path.insert(0, str(PLATFORM_CORE))

from utils import load_registry, load_members, read_json

# config.py loads .env automatically on import
import config


# ── Helpers ───────────────────────────────────────────────────────────────────

def _member_by_id(members, member_id):
    return next((m for m in members if m["member_id"] == member_id), None)

def _member_by_email(members, email):
    return next((m for m in members if m.get("email","").lower() == email.lower()), None)

def format_duration(seconds) -> str:
    if seconds is None:
        return "—"
    try:
        s = int(float(seconds))
    except (TypeError, ValueError):
        return "—"
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    h, m = s // 3600, (s % 3600) // 60
    return f"{h}h {m:02d}m"


# ── App factory ───────────────────────────────────────────────────────────────

def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

    @app.context_processor
    def _inject():
        member = None
        if session.get("member_id"):
            member = _member_by_id(load_members(), session["member_id"])
        return {"member": member, "format_duration": format_duration}

    def login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not session.get("member_id"):
                return redirect(url_for("login"))
            return f(*args, **kwargs)
        return decorated

    # ── Auth ──────────────────────────────────────────────────────────────────

    @app.get("/login")
    def login():
        if session.get("member_id"):
            return redirect(url_for("member_home"))
        return render_template("login.html")

    @app.post("/login")
    def login_post():
        email    = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()
        if not email or not password:
            flash("Please enter both email and password.")
            return redirect(url_for("login"))
        member = _member_by_email(load_members(), email)
        if not member:
            flash("No account found with that email.")
            return redirect(url_for("login"))
        session["member_id"] = member["member_id"]
        return redirect(url_for("member_home"))

    @app.get("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.get("/login/google")
    def login_google():
        flash("Google Sign-In is not configured yet.")
        return redirect(url_for("login"))

    @app.get("/")
    def index():
        return redirect(url_for("member_home") if session.get("member_id") else url_for("login"))

    # ── Workbench — workflow landing page ─────────────────────────────────────

    @app.get("/workbench")
    @login_required
    def member_home():
        member    = _member_by_id(load_members(), session["member_id"])
        registry  = load_registry()

        # Warn if AWS credentials are not set
        cred_warnings = config.validate(raise_on_missing_aws=False)

        workflows_out = []
        for entry in registry:
            wf = dict(entry)

            # Load job counts from the workflow's local data file
            dd_path = PLATFORM_ROOT / "workflows" / wf["workflow_id"] / "data" / "dashboard_data.json"
            dd = json.loads(dd_path.read_text(encoding="utf-8")) if dd_path.exists() else {}

            batches = dd.get("batches", [])
            wf["total_batches"]     = len(batches)
            wf["completed_batches"] = sum(1 for b in batches if b.get("status") == "completed")
            wf["pending_batches"]   = wf["total_batches"] - wf["completed_batches"]

            # Status label for UI
            wf["display_status"] = "deployed" if wf.get("url") else "not deployed"

            workflows_out.append(wf)

        return render_template(
            "member_home.html",
            member=member,
            workflows=workflows_out,
            cred_warnings=cred_warnings,
        )

    # ── Open workflow — redirect to Lambda URL ────────────────────────────────

    @app.get("/open/<workflow_id>")
    @login_required
    def open_workflow(workflow_id: str):
        """Redirect browser to the workflow's Lambda URL."""
        registry = load_registry()
        entry    = next((w for w in registry if w["workflow_id"] == workflow_id), None)

        if not entry:
            flash(f"Workflow '{workflow_id}' not found.")
            return redirect(url_for("member_home"))

        url = entry.get("url")
        if not url:
            flash(f"'{entry['name']}' has not been deployed yet. Click Generate XAPP.")
            return redirect(url_for("member_home"))

        # Append /manager so the user lands on the manager dashboard directly
        target = url.rstrip("/") + f"/manager?workflow_id={workflow_id}"
        return redirect(target)

    # ── Setup — upload / generate XAPP ───────────────────────────────────────

    @app.get("/setup")
    @login_required
    def home():
        return render_template("upload.html")

    # ── API: Generate XAPP ────────────────────────────────────────────────────

    @app.post("/api/generate")
    @login_required
    def api_generate():
        """
        POST body: { "workflow": <nodes JSON>, "scope": "..." }

        1. scaffold_workflow() in generate_xapp.py:
             a. Creates workflows/<id>/ folder
             b. Writes app.py, templates, static, data files
             c. Calls builders.build_and_deploy()
                  → pip install + zip
                  → upload zip to Lambda
                  → enable Function URL
                  → returns public HTTPS URL
        2. Saves URL to workflow_registry.json
        3. Returns { status, workflow_id, url }
        """
        from generate_xapp import scaffold_workflow

        data          = request.get_json(silent=True) or {}
        workflow_json = data.get("workflow") or {}
        scope         = str(data.get("scope") or "").strip()
        member_id     = session.get("member_id", "")

        nodes = workflow_json.get("nodes", [])
        if not nodes:
            return jsonify({"error": "No workflow nodes provided."}), 400

        name = (
            workflow_json.get("name")
            or workflow_json.get("workflow_name")
            or (scope[:40] if scope else "")
            or "Unnamed Workflow"
        )

        try:
            entry = scaffold_workflow(
                name           = name,
                scope          = scope,
                workflow_nodes = nodes,
                created_by     = member_id,
            )
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        return jsonify({
            "status":      "deployed",
            "workflow_id": entry["workflow_id"],
            "url":         entry["url"],
        })

    # ── API: Redeploy (rebuild zip + re-upload to Lambda) ─────────────────────

    @app.post("/api/redeploy")
    @login_required
    def api_redeploy():
        """
        Rebuild and redeploy an existing workflow to Lambda.
        Useful when you update base_workflow templates or requirements.
        POST body: { "workflow_id": "wf_lease_abstraction" }
        """
        from builders import build_and_deploy
        from utils import load_registry, save_registry

        workflow_id = (request.get_json(silent=True) or {}).get("workflow_id", "")
        if not workflow_id:
            return jsonify({"error": "workflow_id required"}), 400

        registry = load_registry()
        entry    = next((w for w in registry if w["workflow_id"] == workflow_id), None)
        if not entry:
            return jsonify({"error": f"Workflow '{workflow_id}' not found"}), 404

        try:
            blueprint = {
                "workflow_id": entry["workflow_id"],
                "name":        entry["name"],
                "scope":       entry.get("scope", ""),
                "port":        entry.get("port", 5001),
            }
            url = build_and_deploy(workflow_id, blueprint)
            entry["url"]    = url
            entry["status"] = "deployed"
            save_registry(registry)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        return jsonify({"status": "deployed", "url": url})

    # ── API: Sample workflow ──────────────────────────────────────────────────

    @app.get("/api/sample/workflow")
    def get_sample_workflow():
        sample = PLATFORM_ROOT / "workflows" / "wf_lease_abstraction" / "config" / "workflow.json"
        if sample.exists():
            return json.loads(sample.read_text(encoding="utf-8"))
        return {"error": "Sample not found"}, 404

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
