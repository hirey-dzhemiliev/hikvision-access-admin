#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).parent
APP_PATH = ROOT / "hikvision_admin_app.py"
CONFIG_PATH = ROOT / "config.test.yaml"
DB_PATH = ROOT / "app.db"


def load_module():
    spec = importlib.util.spec_from_file_location("appmod", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    mod = load_module()
    config = mod.ConfigManager(CONFIG_PATH)
    db = mod.Database(DB_PATH)
    app = mod.AdminApp(config, db)
    ctx = mod.RequestContext(environ={}, user="admin", form={}, files={})

    employees = db.list_employees(include_inactive=True)
    panels = db.list_panels(enabled_only=True)
    sync_status = app.build_employee_sync_status(employees[:5], panels) if employees and panels else {}
    latest_runs = db.latest_sync_runs(limit=5)
    latest_run = latest_runs[0] if latest_runs else None
    run_actions = db.list_sync_actions(latest_run["id"]) if latest_run else []

    template_payloads = {
        "dashboard.html": {
            "title": "Dashboard",
            "ctx": ctx,
            "stats": db.get_dashboard_stats(),
            "events": db.list_journal({"show_unmatched": True}, limit=5),
            "sync_runs": latest_runs,
            "sync_errors": db.latest_sync_errors(limit=5),
            "panel_states": db.list_panel_health_status(),
            "today_events": db.list_journal({"show_unmatched": True}, limit=5),
            "unsynced_employees": employees[:3],
            "panels": panels,
            "sync_status": sync_status,
        },
        "employees.html": {
            "title": "Employees",
            "ctx": ctx,
            "employees": employees,
            "panels": panels,
            "sync_status": sync_status,
            "include_inactive": True,
            "search": "",
            "view_mode": "cards",
        },
        "employee_form.html": {
            "title": "Employee",
            "ctx": ctx,
            "employee": employees[0] if employees else None,
            "events": db.latest_employee_events(employees[0]["id"]) if employees else [],
            "panels": panels,
            "sync_status": sync_status.get(employees[0]["id"], {}) if employees else {},
            "error": None,
        },
        "panels.html": {
            "title": "Panels",
            "ctx": ctx,
            "panels": db.list_panels(),
            "status_by_id": {row["panel_id"]: row for row in db.list_panel_health_status()},
        },
        "panel_detail.html": {
            "title": "Panel detail",
            "ctx": ctx,
            "panel": db.list_panels()[0] if db.list_panels() else None,
            "preview": app.sync_service.sync_preview(panel_ids=[db.list_panels()[0]["id"]]) if db.list_panels() else None,
            "events": db.list_journal({"show_unmatched": True}, limit=5),
            "panel_status": db.list_panel_health_status()[0] if db.list_panel_health_status() else None,
            "panel_summary": {"employees_total": len(db.list_employees(include_inactive=False)), "needs_attention": 0, "synced": len(db.list_employees(include_inactive=False))},
        },
        "journal.html": {
            "title": "Journal",
            "ctx": ctx,
            "rows": db.list_journal({"show_unmatched": True}, limit=5),
            "stats": db.journal_stats(),
            "filters": {
                "employee": "",
                "panel_id": "",
                "result": "",
                "unlock_method": "",
                "date_from": "",
                "date_to": "",
                "show_unmatched": "1",
            },
            "panels": db.list_panels(),
            "quick": "week",
        },
        "sync.html": {
            "title": "Sync",
            "ctx": ctx,
            "employees": db.list_employees(include_inactive=False),
            "panels": panels,
            "preview": {"panels": []},
            "sync_result": {"summary": {"created": 1, "updated": 1, "deleted": 1, "errors": 0}},
            "run_actions": run_actions,
            "selected_panel_ids": [],
            "selected_employee_ids": [],
        },
        "sync_runs.html": {
            "title": "Sync runs",
            "ctx": ctx,
            "runs": latest_runs,
            "selected_run_id": latest_run["id"] if latest_run else None,
            "run": latest_run,
            "actions": run_actions,
            "panels": db.list_panels(),
            "filters": {"panel_id": None, "employee_id": "", "status": ""},
        },
        "discrepancies.html": {
            "title": "Discrepancies",
            "ctx": ctx,
            "employee_issues": [],
            "panel_only": [],
            "employees": {},
        },
        "settings.html": {
            "title": "Settings",
            "ctx": ctx,
            "config": config.data,
            "db_stats": db.journal_stats(),
            "media_stats": app.media_stats(),
        },
    }

    for template_name, payload in template_payloads.items():
        app.templates.get_template(template_name).render(**payload)

    print("smoke-ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
