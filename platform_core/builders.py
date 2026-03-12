"""
platform_core/builders.py
════════════════════════════════════════════════════════════════════════════
THE MAGIC MODULE — turns a blueprint into a live Lambda-deployed workflow.

Phase 2 pipeline (this file):
  generate_app_code(blueprint)  →  app.py content string
  package_lambda(workflow_id)   →  builds/<id>.zip  (pip + zip, no Docker)
  deploy_lambda(workflow_id)    →  uploads zip, returns public HTTPS URL
  build_and_deploy(...)         →  convenience: runs all three in sequence

All credentials and settings are read from config.py.
To swap in Phase 3 (Gemini-generated code), replace generate_app_code() only.
Everything else — package, deploy, generate_xapp, master_dashboard — unchanged.
════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import config   # platform_core/config.py — single source of truth

PLATFORM_CORE_DIR = Path(__file__).resolve().parent
PLATFORM_ROOT     = PLATFORM_CORE_DIR.parent
BASE_WORKFLOW_DIR = PLATFORM_ROOT / "workflow_templates" / "base_workflow"
BUILDS_DIR        = PLATFORM_ROOT / config.BUILDS_DIR_NAME


# ════════════════════════════════════════════════════════════════════════════
# STEP 1  generate_app_code()
# Produces the app.py that runs inside Lambda.
# Phase 1/2: copy base template + inject tokens  ← now
# Phase 3:   replace with Gemini API call        ← future, same signature
# ════════════════════════════════════════════════════════════════════════════

def generate_app_code(blueprint: dict) -> str:
    """
    Read base_workflow/app.py and inject workflow identity tokens.

        {{WORKFLOW_ID}}    →  blueprint["workflow_id"]
        {{WORKFLOW_NAME}}  →  blueprint["name"]
        {{PORT}}           →  blueprint["port"]  (kept for local-run compat)

    Appends the Mangum Lambda handler if not already present.
    Returns the complete app.py as a string.
    """
    base_app = BASE_WORKFLOW_DIR / "app.py"
    if not base_app.exists():
        raise FileNotFoundError(f"Base template not found: {base_app}")

    content = base_app.read_text(encoding="utf-8")
    content = content.replace("{{WORKFLOW_ID}}",   blueprint["workflow_id"])
    content = content.replace("{{WORKFLOW_NAME}}", blueprint["name"])
    content = content.replace("{{PORT}}",          str(blueprint.get("port", 5001)))

    # Append Mangum handler — idempotent, safe to call multiple times
    if "from mangum import Mangum" not in content:
        content += (
            "\n\n# ── AWS Lambda entry point ──────────────────────────────────────\n"
            "# Mangum translates Lambda HTTP events → WSGI so Flask routes work\n"
            "# unchanged. The 'handler' name must match LAMBDA_HANDLER in config.py.\n"
            "from mangum import Mangum\n"
            "handler = Mangum(app, lifespan=\"off\")\n"
        )

    return content


# ════════════════════════════════════════════════════════════════════════════
# STEP 2  package_lambda()
# Install dependencies into the build folder and zip everything.
# Lambda accepts plain zip files — no Docker required.
# ════════════════════════════════════════════════════════════════════════════

def package_lambda(workflow_id: str) -> Path:
    """
    Package a workflow into a Lambda-deployable zip file.

      1. Copy workflows/<id>/  →  builds/<id>/
      2. Bundle users_members.json inside the package
         (Lambda has no access to the host filesystem)
      3. pip install flask + mangum + workflow deps into the build folder
         (packages must sit alongside app.py, not in system site-packages)
      4. Zip everything  →  builds/<id>.zip

    Returns the Path to the zip file.
    Raises RuntimeError if the unzipped size exceeds Lambda's 250 MB limit.
    """
    src_dir   = PLATFORM_ROOT / "workflows" / workflow_id
    build_dir = BUILDS_DIR / workflow_id
    zip_path  = BUILDS_DIR / f"{workflow_id}.zip"

    if not src_dir.exists():
        raise FileNotFoundError(f"Workflow folder not found: {src_dir}")

    # 1. Clean + recreate build directory
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    # 2. Copy workflow source files
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store")
    for item in src_dir.iterdir():
        dst = build_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dst, ignore=ignore)
        else:
            shutil.copy2(item, dst)

    # 3. Bundle users_members.json
    # workflow app.py reads members from ../../platform_core/users_members.json
    # relative to its own location. Inside Lambda the package root IS the cwd,
    # so we replicate that relative path: platform_core/users_members.json
    bundle_core = build_dir / "platform_core"
    bundle_core.mkdir(exist_ok=True)
    members_src = PLATFORM_CORE_DIR / "users_members.json"
    if members_src.exists():
        shutil.copy2(members_src, bundle_core / "users_members.json")

    # 4. Build deduplicated requirements list (base + workflow-specific)
    reqs = list(config.LAMBDA_BASE_REQUIREMENTS)
    wf_req = src_dir / "requirements.txt"
    if wf_req.exists():
        for line in wf_req.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                reqs.append(line)
    seen, deduped = set(), []
    for r in reqs:
        key = r.split("==")[0].split(">=")[0].split("~=")[0].lower().strip()
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    # 5. pip install packages into build_dir (alongside app.py)
    print(f"[builders] Installing: {deduped}")
    subprocess.run(
        [sys.executable, "-m", "pip", "install",
         *deduped, "--target", str(build_dir),
         "--quiet", "--no-cache-dir"],
        check=True,
    )

    # 6. Zip
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in build_dir.rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to(build_dir))

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"[builders] Package ready: {zip_path.name} ({size_mb:.1f} MB)")

    if size_mb > 250:
        raise RuntimeError(
            f"Package is {size_mb:.1f} MB — exceeds Lambda's 250 MB limit.\n"
            "Remove heavy dependencies or switch to container deployment."
        )

    return zip_path


# ════════════════════════════════════════════════════════════════════════════
# STEP 3  deploy_lambda()
# Upload the zip to AWS Lambda, enable public Function URL, return the URL.
# ════════════════════════════════════════════════════════════════════════════

def deploy_lambda(workflow_id: str, zip_path: Path) -> str:
    """
    Upload the zip to AWS Lambda and return the public HTTPS Function URL.

    - Function already exists  →  update code + sync config
    - Function does not exist  →  create from scratch
    - Either way               →  ensure a public Function URL (AuthType=NONE)

    All settings (region, role ARN, timeout, memory) come from config.py.
    Returns the URL string, e.g. https://abc123.lambda-url.us-east-1.on.aws/
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        raise ImportError("boto3 not installed. Run:  pip install boto3")

    config.validate(raise_on_missing_aws=True)   # fail fast on missing creds

    lam = boto3.client(
        "lambda",
        region_name           = config.AWS_REGION,
        aws_access_key_id     = config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY,
    )

    fn   = workflow_id
    code = zip_path.read_bytes()

    print(f"[builders] Deploying '{fn}' to Lambda ({config.AWS_REGION}) ...")

    try:
        lam.update_function_code(FunctionName=fn, ZipFile=code)
        print(f"[builders] Updated existing function: {fn}")
        _wait_ready(lam, fn)
        lam.update_function_configuration(
            FunctionName = fn,
            Timeout      = config.LAMBDA_TIMEOUT,
            MemorySize   = config.LAMBDA_MEMORY_MB,
            Handler      = config.LAMBDA_HANDLER,
        )
        _wait_ready(lam, fn)

    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        print(f"[builders] Creating new function: {fn}")
        lam.create_function(
            FunctionName = fn,
            Runtime      = config.LAMBDA_RUNTIME,
            Role         = config.LAMBDA_EXEC_ROLE_ARN,
            Handler      = config.LAMBDA_HANDLER,
            Code         = {"ZipFile": code},
            Timeout      = config.LAMBDA_TIMEOUT,
            MemorySize   = config.LAMBDA_MEMORY_MB,
            Description  = f"xApp workflow: {fn}",
        )
        _wait_ready(lam, fn)

    url = _ensure_function_url(lam, fn)
    print(f"[builders] ✓ Deployed. URL: {url}")
    return url


def _wait_ready(lam, fn: str, timeout: int = 90) -> None:
    """Poll until Lambda function state is Active (not Pending/Updating)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cfg  = lam.get_function_configuration(FunctionName=fn)
        if (cfg.get("State") == "Active" and
                cfg.get("LastUpdateStatus", "Successful") in ("Successful", "")):
            return
        time.sleep(3)
    raise TimeoutError(f"Lambda '{fn}' not ready after {timeout}s")


def _ensure_function_url(lam, fn: str) -> str:
    """Return existing Function URL or create a new public one."""
    from botocore.exceptions import ClientError
    try:
        return lam.get_function_url_config(FunctionName=fn)["FunctionUrl"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    url = lam.create_function_url_config(
        FunctionName = fn,
        AuthType     = config.LAMBDA_AUTH_TYPE,
    )["FunctionUrl"]

    try:
        lam.add_permission(
            FunctionName        = fn,
            StatementId         = "allow-public-url",
            Action              = "lambda:InvokeFunctionUrl",
            Principal           = "*",
            FunctionUrlAuthType = "NONE",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise   # Permission already exists — fine

    return url


# ════════════════════════════════════════════════════════════════════════════
# CONVENIENCE  build_and_deploy()
# Called by generate_xapp.py after the workflow folder is scaffolded.
# ════════════════════════════════════════════════════════════════════════════

def build_and_deploy(workflow_id: str, blueprint: dict) -> str:
    """
    Full pipeline in one call:
      1. generate_app_code()  →  write app.py with mangum handler
      2. package_lambda()     →  install deps + zip
      3. deploy_lambda()      →  upload to Lambda, return URL

    The returned URL is saved to workflow_registry.json by generate_xapp.py.
    Clicking a workflow card on the landing page goes directly to this URL.
    """
    print(f"[builders] Starting build+deploy for: {workflow_id}")

    app_code = generate_app_code(blueprint)
    app_path = PLATFORM_ROOT / "workflows" / workflow_id / "app.py"
    app_path.write_text(app_code, encoding="utf-8")
    print(f"[builders] app.py written: {app_path}")

    zip_path = package_lambda(workflow_id)
    url      = deploy_lambda(workflow_id, zip_path)

    return url
