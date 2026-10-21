"""
include/nammamart/alerts.py
---------------------------
Failure alerting + run audit helpers shared by both NammaMart DAGs.

The Slack webhook is read from the Airflow Variable SLACK_WEBHOOK_URL at run time,
never from code. If it is not set, the alert is printed to the task log instead.
"""

import os
import shutil
import time

from nammamart import config as cfg
from nammamart.db import connect, write_pipeline_run


def notify_failure(context):
    """on_failure_callback: fires after the last retry of any task has failed."""
    from airflow.sdk import Variable
    from slack_notifier import send_slack_alert

    ti = context["ti"]
    message = (
        f":red_circle: NammaMart pipeline failure\n"
        f"*DAG:* {ti.dag_id}  *Task:* {ti.task_id}  *Try:* {ti.try_number}\n"
        f"*Run:* {context['run_id']}\n"
        f"*Error:* {str(context.get('exception'))[:500]}\n"
        f"Check the task log in the Airflow UI and the pipeline_runs table."
    )
    send_slack_alert(message, webhook_url=Variable.get("SLACK_WEBHOOK_URL", default=None))


def audit_run(context, upstream_ids, rows_extracted, rows_loaded, rows_quarantined):
    """Write one pipeline_runs row. Returns the list of upstream tasks that produced no result."""
    from airflow.exceptions import AirflowFailException

    ti, dag_run = context["ti"], context["dag_run"]
    results = {t: ti.xcom_pull(task_ids=t) for t in upstream_ids}
    failed = [t for t, r in results.items() if r is None]
    row = {
        "run_id": dag_run.run_id, "dag_id": dag_run.dag_id,
        "start_time": dag_run.start_date.replace(tzinfo=None) if dag_run.start_date else None,
        "end_time": __import__("datetime").datetime.utcnow(),
        "rows_extracted": rows_extracted(results), "rows_loaded": rows_loaded(results),
        "rows_quarantined": rows_quarantined(results),
        "status": "FAILED" if failed else "SUCCESS",
        "failed_tasks": ", ".join(failed) or None,
    }
    with connect() as con:
        write_pipeline_run(con, row)
        history = con.execute(
            "SELECT * FROM pipeline_runs WHERE dag_id = ? ORDER BY start_time DESC LIMIT 5", [dag_run.dag_id]).df()
    print(f"[Audit] {row}")
    print("[Audit] Last 5 runs:\n" + history.to_string(index=False))
    _cleanup_old_tmp()
    if failed:
        # A green audit task would make the whole run look green: fail it on purpose.
        raise AirflowFailException(f"Run incomplete, tasks without a result: {failed}")
    return row["status"]


def _cleanup_old_tmp(max_age_days=3):
    """Intermediate files are kept a few days so a failed task can be cleared and re-run."""
    for root in [os.path.join(cfg.TMP_DIR, f"etl_{cfg.LEARNER}")]:
        if not os.path.isdir(root):
            continue
        for d in os.listdir(root):
            path = os.path.join(root, d)
            if time.time() - os.path.getmtime(path) > max_age_days * 86400:
                shutil.rmtree(path, ignore_errors=True)
