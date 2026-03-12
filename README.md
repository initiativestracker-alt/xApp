# xApp Platform — Phase 2

HITL (Human-in-the-Loop) workflow platform.
Each workflow is an independent Flask app deployed to AWS Lambda.
No Docker. No servers to manage. No ports.

---

## Folder Structure

```
xapp-platform/
│
├── .env                                  ← ALL secrets go here (never commit)
├── requirements.txt                      ← flask, boto3, mangum
│
├── platform_core/                        ← brain of the platform
│   ├── config.py                         ← SINGLE SOURCE OF TRUTH
│   │                                        credentials, AWS settings, prompts
│   ├── builders.py                       ← THE MAGIC MODULE
│   │                                        generate_app_code → package → deploy
│   ├── generate_xapp.py                  ← orchestrator: scaffold + deploy
│   ├── workflow_manager.py               ← kept for local dev (unused in Phase 2)
│   ├── utils.py                          ← shared helpers, port/slug utils
│   ├── users_members.json                ← login store (shared by all workflows)
│   ├── workflow_registry.json            ← workflow_id, url, status per workflow
│   └── applications.json                 ← full blueprint store
│
├── builds/                               ← staging area (auto-created, gitignore)
│   ├── wf_lease_abstraction/             ← unpacked build folder
│   └── wf_lease_abstraction.zip          ← zip uploaded to Lambda
│
├── workflow_templates/
│   └── base_workflow/                    ← master template — never run directly
│       ├── app.py                        ← Flask app with {{WORKFLOW_ID}} tokens
│       │                                    + mangum handler at bottom
│       ├── requirements.txt
│       ├── config/workflow.json          ← empty shell
│       ├── data/dashboard_data.json      ← empty shell
│       ├── templates/
│       └── static/
│
├── workflows/
│   └── wf_lease_abstraction/             ← generated workflow (local copy)
│       ├── app.py                        ← tokens injected, mangum handler added
│       ├── config/workflow.json          ← pipeline node definitions
│       ├── data/dashboard_data.json      ← jobs, batches, steps (live data)
│       ├── templates/
│       ├── static/
│       └── input_files/                  ← source PDFs
│
└── master_dashboard/                     ← port 5000, always on your server
    ├── app.py                            ← login + landing page + /api/generate
    └── templates/
```

---

## One-time AWS Setup

Before deploying, complete these steps once:

```
1. AWS Console → IAM → Roles → Create Role
   • Trusted entity  : Lambda
   • Policy          : AWSLambdaBasicExecutionRole
   • Name            : xapp-lambda-role
   • Copy the ARN    : arn:aws:iam::<account-id>:role/xapp-lambda-role

2. AWS Console → IAM → Users → your user → Security credentials
   • Create access key
   • Copy Access Key ID and Secret Access Key

3. Paste all three into .env
```

---

## Configuration — edit .env only

```
.env
├── FLASK_SECRET_KEY          master dashboard session key
├── AWS_REGION                e.g. us-east-1
├── AWS_ACCESS_KEY_ID         from IAM user
├── AWS_SECRET_ACCESS_KEY     from IAM user
├── LAMBDA_EXEC_ROLE_ARN      arn:aws:iam::<account>:role/xapp-lambda-role
├── GEMINI_API_KEY            for Phase 3 LLM generation
└── GEMINI_MODEL              gemini-2.5-flash
```

All other settings (Lambda timeout, memory, runtime) are in `platform_core/config.py`.
Prompts sent to Gemini in Phase 3 are also there.

---

## How to Run

```bash
# Install dependencies
pip install flask boto3 mangum

# Start the master dashboard
cd master_dashboard
python app.py
# → http://localhost:5000
```

Workflow apps run on AWS Lambda — no local start needed.

---

## Generate XAPP Flow

```
User fills scope + uploads workflow JSON at /setup
            │
            ▼
POST /api/generate  →  master_dashboard/app.py
            │
            ▼
generate_xapp.scaffold_workflow()
  ├── 1. Derive workflow_id  =  "wf_" + slug(name)
  ├── 2. Create  workflows/<id>/  folder
  │         config/workflow.json        ← uploaded nodes
  │         data/dashboard_data.json   ← steps from nodes, empty jobs/batches
  │         templates/  static/        ← copied from base_workflow/
  │         app.py                     ← tokens injected by generate_app_code()
  │
  └── 3. Call builders.build_and_deploy()
                │
                ├── a. generate_app_code(blueprint)
                │        reads base_workflow/app.py
                │        replaces {{WORKFLOW_ID}}, {{WORKFLOW_NAME}}, {{PORT}}
                │        appends mangum handler:
                │            from mangum import Mangum
                │            handler = Mangum(app, lifespan="off")
                │        writes app.py → workflows/<id>/app.py
                │
                ├── b. package_lambda(workflow_id)
                │        copies workflows/<id>/ → builds/<id>/
                │        bundles platform_core/users_members.json inside package
                │        pip install flask mangum [workflow deps]
                │            --target builds/<id>/        ← alongside app.py
                │        zips everything → builds/<id>.zip
                │
                └── c. deploy_lambda(workflow_id, zip_path)
                         boto3.client("lambda")
                         if function exists  → update_function_code()
                         if function new     → create_function()
                         wait until State=Active
                         create_function_url_config(AuthType=NONE)
                         add_permission(lambda:InvokeFunctionUrl, Principal=*)
                         returns URL:
                         https://<id>.lambda-url.<region>.on.aws/
            │
            ▼
URL saved to platform_core/workflow_registry.json
{ "workflow_id": "wf_lease_abstraction",
  "url": "https://abc123.lambda-url.us-east-1.on.aws/",
  "status": "deployed" }
            │
            ▼
Workflow card appears on landing page
Status badge: ✓ Deployed
```

---

## User Flow (after deploy)

```
User opens http://localhost:5000
            │
            ▼
Login page  →  email + password
            │
            ▼
/workbench  — workflow landing page
  Shows cards for every workflow in workflow_registry.json
  Each card displays:  name · workflow_id · pending count · deploy status
            │
            ▼
User clicks a workflow card
            │
            ▼
GET /open/<workflow_id>  →  master_dashboard/app.py
  reads entry["url"] from workflow_registry.json
            │
            ▼
302 Redirect →
  https://<id>.lambda-url.<region>.on.aws/manager?workflow_id=<id>
            │
            ▼
AWS Lambda receives the HTTPS request
  invokes  handler(event, context)   ← mangum
  mangum translates event → WSGI
  Flask routes handle it normally
  renders manager_dashboard.html
            │
            ▼
User sees full manager / member / job / batch experience
Master dashboard (port 5000) is no longer involved
Lambda handles all subsequent requests independently
```

---

## Redeploy an Existing Workflow

Use this when you update `base_workflow/app.py` or change requirements:

```bash
curl -X POST http://localhost:5000/api/redeploy \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "wf_lease_abstraction"}'
```

This runs `build_and_deploy()` again:
- Regenerates `app.py` from the updated template
- Re-packages (fresh pip install)
- Re-uploads zip to Lambda (`update_function_code`)
- Returns the same URL (unchanged)

---

## What Lambda Receives vs What Flask Sees

```
Browser                    AWS Lambda                  Flask (via Mangum)
──────                     ──────────                  ──────────────────
GET /manager    ──────►   HTTP event JSON   ──────►   request.path = "/manager"
  ?workflow_id=X            { httpMethod,               request.args["workflow_id"]
                              path, headers,
                              queryStringParameters }
◄──────────────           response JSON     ◄──────   render_template(...)
  HTML page                 { statusCode,
                              headers, body }
```

Mangum handles the translation. Your Flask routes are unchanged.

---

## Phase Roadmap

```
Phase 1 (done)   generate_app_code()   copy base template + inject tokens
                 workflow_manager      subprocess on localhost
                 workflow card         → http://localhost:500X

Phase 2 (now)    generate_app_code()   same
                 package_lambda()      pip install → zip (no Docker)
                 deploy_lambda()       boto3 → Lambda Function URL
                 workflow card         → https://<id>.lambda-url.*.on.aws/

Phase 3 (next)   generate_app_code()   send blueprint to Gemini → custom app.py
                 package + deploy      unchanged from Phase 2
```

Only `generate_app_code()` in `builders.py` changes for Phase 3.
Everything else — package, deploy, generate_xapp, master_dashboard — stays the same.

---

## Login Credentials (dev mode — any password works)

| Email | Role |
|---|---|
| naveena@xtract.io | member |
| sankarsundaram@xtract.io | member |
| aswatth.krishna@techmobius.com | member |
