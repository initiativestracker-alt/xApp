# xApp Platform

## Folder Structure

```
xapp-platform/
├── .env                              # GEMINI_API_KEY etc.
├── requirements.txt
│
├── platform_core/                    # Brain of the platform
│   ├── users_members.json            # Shared login store
│   ├── workflow_registry.json        # Port, PID, status per workflow
│   ├── applications.json             # Full blueprint store
│   ├── builders.py                   # generate_app_code() — Phase 1: copy+inject
│   ├── generate_xapp.py              # Scaffolds workflows/<id>/ on Generate XAPP
│   ├── workflow_manager.py           # Launch / stop workflow subprocesses
│   └── utils.py                      # Port assignment, shared helpers
│
├── workflow_templates/
│   └── base_workflow/                # Master copy — never run directly
│       ├── app.py                    # Full Flask app with {{WORKFLOW_ID}} tokens
│       ├── requirements.txt
│       ├── config/workflow.json
│       ├── data/dashboard_data.json
│       ├── templates/
│       └── static/
│
├── workflows/
│   └── lease_abstraction/            # Port 5001 — live workflow app
│       ├── app.py                    # Generated from base_workflow, tokens injected
│       ├── config/workflow.json      # Pipeline node definitions
│       ├── data/dashboard_data.json  # Jobs, batches, steps — live data
│       ├── templates/
│       ├── static/
│       └── input_files/              # Source PDFs
│
└── master_dashboard/                 # Port 5000 — always running
    ├── app.py                        # Login + workflow listing + Generate XAPP
    └── templates/
```

## How to Run

### 1. Install dependencies
```bash
pip install flask
```

### 2. Start the Master Dashboard (port 5000)
```bash
cd master_dashboard
python app.py
```
Open http://localhost:5000 — login, see the workflow landing page.

### 3. Start the Lease Abstraction workflow (port 5001)
Either click Launch in the master dashboard, or run directly:
```bash
cd workflows/lease_abstraction
python app.py
```
Open http://localhost:5001

### 4. Generate a new workflow
1. Go to http://localhost:5000/setup
2. Enter a project scope
3. Upload a workflow JSON
4. Click Generate XAPP
   → Creates `workflows/<new_id>/` folder
   → Starts the app on the next available port
   → Workflow card appears on the landing page

## Login credentials (dev mode — any password works)
- naveena@xtract.io
- sankarsundaram@xtract.io
- aswatth.krishna@techmobius.com

## Flow
```
Login (port 5000)
    ↓
Workflow landing page — cards for each workflow
    ↓
Click a workflow card
    ↓
Goes to http://localhost:500X/manager  (workflow's own port)
    ↓
Full manager/member/job/batch experience inside that workflow
```
