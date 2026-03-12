"""
platform_core/config.py
════════════════════════════════════════════════════════════════════════════
SINGLE SOURCE OF TRUTH
All credentials, AWS settings, Lambda tunables, and LLM prompts live here.
Edit only this file — never hardcode values anywhere else.
════════════════════════════════════════════════════════════════════════════
"""
import os
from pathlib import Path

# ── Auto-load .env from platform root ────────────────────────────────────────
def _load_env() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip().lstrip("\ufeff")
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v

_load_env()


# ════════════════════════════════════════════════════════════════════════════
# AWS CREDENTIALS
# Set values in .env — never paste keys directly here.
# ════════════════════════════════════════════════════════════════════════════

AWS_REGION            = os.environ.get("AWS_REGION",            "us-east-1")
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID",     "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# IAM role Lambda uses to execute.
# Required policy: AWSLambdaBasicExecutionRole
# Format: arn:aws:iam::<account-id>:role/<role-name>
LAMBDA_EXEC_ROLE_ARN  = os.environ.get("LAMBDA_EXEC_ROLE_ARN",  "")


# ════════════════════════════════════════════════════════════════════════════
# LAMBDA SETTINGS
# Tune these to adjust runtime behaviour of deployed workflows.
# ════════════════════════════════════════════════════════════════════════════

LAMBDA_RUNTIME   = "python3.11"    # Python version inside Lambda
LAMBDA_TIMEOUT   = 30              # Max seconds per request
LAMBDA_MEMORY_MB = 256             # MB of RAM (128–10240)
LAMBDA_HANDLER   = "app.handler"   # module.function — must match mangum handler
LAMBDA_AUTH_TYPE = "NONE"          # "NONE" = public URL, no IAM auth required


# ════════════════════════════════════════════════════════════════════════════
# BUILD SETTINGS
# ════════════════════════════════════════════════════════════════════════════

# Folder (relative to platform root) where zips are staged before upload
BUILDS_DIR_NAME = "builds"

# These packages are injected into EVERY workflow Lambda package
# regardless of what the workflow's own requirements.txt says
LAMBDA_BASE_REQUIREMENTS = [
    "flask",
    "mangum",   # translates Lambda HTTP events → WSGI (Flask)
]


# ════════════════════════════════════════════════════════════════════════════
# GEMINI / LLM SETTINGS  (Phase 3 — AI code generation)
# ════════════════════════════════════════════════════════════════════════════

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL",   "gemini-2.5-flash")
GEMINI_TIMEOUT = 120   # seconds — generation can be slow for large blueprints


# ════════════════════════════════════════════════════════════════════════════
# PROMPTS  (Phase 3)
# Edit these strings to change what the LLM generates.
# {placeholders} are filled in by builders.py before sending.
# ════════════════════════════════════════════════════════════════════════════

PROMPT_GENERATE_APP = """
You are generating a complete Python Flask application for a HITL
(Human-in-the-Loop) workflow platform called xApp.

The app MUST:
1. Be a single self-contained Flask file with NO external config imports.
2. Include a Mangum handler at the bottom for AWS Lambda:
       from mangum import Mangum
       handler = Mangum(app, lifespan="off")
3. Define these constants near the top:
       WORKFLOW_ID   = "{workflow_id}"
       WORKFLOW_NAME = "{workflow_name}"
4. Read data ONLY from paths relative to the file:
       data/dashboard_data.json     — steps, jobs, batches, batch_steps
       platform_core/users_members.json  — login/auth
5. Implement these routes exactly:
       GET  /manager                  — manager dashboard
       GET  /member                   — member work queue
       POST /member/claim             — claim next batch
       GET  /member/batch/<batch_id>  — batch detail / HITL form
       POST /member/batch/<batch_id>/complete  — submit HITL form
       GET  /job                      — job detail view
       GET  /input-files/<path>       — serve uploaded PDFs

Workflow scope:
{scope}

Pipeline nodes (JSON):
{nodes_json}

Return ONLY valid Python code. No markdown fences. No explanation.
""".strip()

PROMPT_GENERATE_SCHEMA = """
You are generating a HITL payload schema for a workflow step in the xApp platform.

Workflow scope : {scope}
HITL step name : {hitl_step_name}
Step inputs    : {hitl_inputs}

Generate a Python list called PAYLOAD_SCHEMA.
Each item is a dict with these exact keys:
  field    : str   — snake_case identifier
  label    : str   — human-readable field label
  type     : str   — one of: text | number | date | currency | dropdown | textarea
  required : bool
  hint     : str   — one-line helper text shown under the field
  options  : list  — dropdown choices (empty list for all other types)

Return ONLY valid Python: PAYLOAD_SCHEMA = [...]
No markdown. No explanation.
""".strip()


# ════════════════════════════════════════════════════════════════════════════
# VALIDATION
# Call config.validate() on startup to catch missing credentials early.
# ════════════════════════════════════════════════════════════════════════════

def validate(raise_on_missing_aws: bool = True) -> list[str]:
    """
    Check all required credentials are present.
    Returns a list of human-readable warning strings (empty list = all good).
    Raises EnvironmentError if AWS credentials are missing and
    raise_on_missing_aws=True.
    """
    warnings = []

    if not AWS_ACCESS_KEY_ID:
        warnings.append("AWS_ACCESS_KEY_ID is not set in .env")
    if not AWS_SECRET_ACCESS_KEY:
        warnings.append("AWS_SECRET_ACCESS_KEY is not set in .env")
    if not LAMBDA_EXEC_ROLE_ARN:
        warnings.append("LAMBDA_EXEC_ROLE_ARN is not set in .env")
    if not GEMINI_API_KEY:
        warnings.append("GEMINI_API_KEY is not set in .env  [only needed for Phase 3]")

    if raise_on_missing_aws:
        aws_issues = [w for w in warnings if "GEMINI" not in w]
        if aws_issues:
            raise EnvironmentError(
                "Missing required AWS credentials:\n"
                + "\n".join(f"  • {w}" for w in aws_issues)
            )

    return warnings
