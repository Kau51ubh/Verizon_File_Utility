from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 0,
}

def extract_and_log_summary(**context):
    import re
    ti = context["ti"]
    resp = ti.xcom_pull(task_ids="invoke_compare_function")
    html = resp.get("html_summary", "")
    log = ti.log

    # ANSI color codes for log prettification
    def colorize(text, color):
        colors = {
            "red": "\033[91m",
            "green": "\033[92m",
            "yellow": "\033[93m",
            "cyan": "\033[96m",
            "end": "\033[0m",
        }
        return f"{colors.get(color, '')}{text}{colors['end']}"

    log.info("")
    log.info("="*70)
    log.info(colorize("               ★★  GCS FILE COMPARISON SUMMARY  ★★", "cyan"))
    log.info("="*70)
    log.info("")

    if not html:
        log.info(colorize("No HTML summary returned. Raw XCom: %s", "red"), resp)
        for field in [
            "Job Name", "File Name", "TD Row Count | Column Count | Missing Columns",
            "BQ Row Count | Column Count | Missing Columns", "Count Variance", "Header Validation",
            "Count Validation", "File Extension Validation", "Checksum Validation", "Status",
            "Passed Columns", "Mismatched Columns"
        ]:
            log.info(f"{field:40}: N/A")
        log.info("="*70)
        return

    soup = BeautifulSoup(html, "html.parser")
    summary = {}
    for row in soup.find_all("tr"):
        cols = row.find_all(["th", "td"])
        if len(cols) == 2:
            key = cols[0].get_text(strip=True)
            val = cols[1].get_text(strip=True)
            summary[key] = val

    # Beautify Passed Columns
    passed_cols = summary.get("Passed Columns", "N/A").replace(",,", ",")
    if passed_cols.endswith(","):
        passed_cols = passed_cols[:-1]
    # Break up long column lists for readability
    def col_list(colstring):
        cols = [c for c in colstring.split(",") if c.strip()]
        if not cols:
            return "N/A"
        # Display as multi-line if more than 6 columns
        if len(cols) > 6:
            return "\n    " + ", ".join(cols)
        return ", ".join(cols)

    # Colorize pass/fail statuses
    def color_status(val):
        if isinstance(val, str) and val.strip().upper() == "PASS":
            return colorize(val, "green")
        if isinstance(val, str) and val.strip().upper() == "FAIL":
            return colorize(val, "red")
        return val

    fields = [
        ("Job Name", "Job Name"),
        ("File Name", "File Name"),
        ("TD Row Count | Column Count | Missing Columns", "TD Row Count | Column Count | Missing Columns"),
        ("BQ Row Count | Column Count | Missing Columns", "BQ Row Count | Column Count | Missing Columns"),
        ("Count Variance", "Count Variance"),
        ("Header Validation", "Header Validation"),
        ("Count Validation", "Count Validation"),
        ("File Extension Validation", "File Extension Validation"),
        ("Checksum Validation", "Checksum Validation"),
        ("Status", "Status"),
        ("Passed Columns", col_list(passed_cols)),
        ("Mismatched Columns", col_list(summary.get("Mismatched Columns", "N/A"))),
    ]

    for label, key in fields:
        val = key if label == "Passed Columns" else summary.get(key, "N/A")
        if label == "Passed Columns":
            val = col_list(passed_cols)
        if label == "Mismatched Columns":
            val = col_list(summary.get("Mismatched Columns", "N/A"))
        # Colorize for status fields
        if label.endswith("Validation") or label == "Status":
            val = color_status(val)
        # Multi-line for columns
        if isinstance(val, str) and "\n" in val:
            log.info(f"{label:40}:")
            for line in val.splitlines():
                if line.strip():
                    log.info(f"    {line}")
        else:
            log.info(f"{label:40}: {val}")

    log.info("")
    log.info("="*70)

with DAG(
    dag_id="compare_gcs_files_runtime_config",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={"dummy": "setme"},
    tags=["gcs", "file-compare"],
) as dag:

    invoke_function = HttpOperator(
        task_id='invoke_compare_function',
        http_conn_id='compare_func_http',
        endpoint='/',
        method='POST',
        headers={"Content-Type":"application/json"},
        data="{{ dag_run.conf | tojson }}",
        response_filter=lambda r: r.json(),
        log_response=True,
        do_xcom_push=True,
    )

    extract_summary = PythonOperator(
        task_id='extract_and_log_summary',
        python_callable=extract_and_log_summary,
        provide_context=True,
    )

    invoke_function >> extract_summary