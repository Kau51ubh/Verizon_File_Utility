from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 0,
}

def invoke_cloud_run(**context):
    import json

    payload = context["dag_run"].conf or {}
    service_url = "https://compare-files-463031084485.us-central1.run.app/"

    # Generate ID token for Cloud Run
    target_audience = service_url
    auth_req = Request()
    token = id_token.fetch_id_token(auth_req, target_audience)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(service_url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json()

def extract_and_log_summary(**context):
    import sys
    from bs4 import BeautifulSoup

    ti = context["ti"]
    resp = ti.xcom_pull(task_ids="invoke_compare_function")
    html = resp.get("html_summary", "")
    log = ti.log

    # ANSI escape codes for styling
    BLUE_BG = "\033[44;97;1m"
    ENDC = "\033[0m"
    GREEN = "\033[32;1m"
    RED = "\033[31;1m"
    BOLD = "\033[1m"
    HEADER = f"{BLUE_BG}  ★★  GCS FILE COMPARISON SUMMARY  ★★  {ENDC}"

    # Pretty summary bar
    bar = f"{BOLD}{'=' * 70}{ENDC}"

    log.info(bar)
    log.info(HEADER)
    log.info(bar)

    if not html:
        log.info("No HTML summary returned. Raw XCom: %s", resp)
        for field in [
            "Job Name", "File Name", "TD Row Count | Column Count | Missing Columns",
            "BQ Row Count | Column Count | Missing Columns", "Count Variance", "Header Validation",
            "Count Validation", "File Extension Validation", "Checksum Validation", "Status",
            "Passed Columns", "Mismatched Columns"
        ]:
            log.info(f"{field:45}: N/A")
        log.info(bar)
        return

    soup = BeautifulSoup(html, "html.parser")
    summary = {}
    for row in soup.find_all("tr"):
        cols = row.find_all(["th", "td"])
        if len(cols) == 2:
            key = cols[0].get_text(strip=True)
            val = cols[1].get_text(strip=True)
            summary[key] = val

    # Colorize PASS/FAIL/N/A with icons
    def status_color(val):
        up = (val or "").strip().upper()
        if up == "PASS":
            return f"{GREEN}✅ PASS{ENDC}"
        elif up == "FAIL":
            return f"{RED}❌ FAIL{ENDC}"
        elif up in {"N/A", ""}:
            return f"\033[33;1m⚠️  N/A{ENDC}"
        return val

    # Remove double commas and trailing commas in "Passed Columns"
    passed_cols = summary.get("Passed Columns", "N/A").replace(",,", ",").rstrip(",")
    mismatched_cols = summary.get("Mismatched Columns", "N/A")

    fields = [
        ("Job Name", summary.get("Job Name", "N/A")),
        ("File Name", summary.get("File Name", "N/A")),
        ("TD Row Count | Column Count | Missing Columns", summary.get("TD Row Count | Column Count | Missing Columns", "N/A")),
        ("BQ Row Count | Column Count | Missing Columns", summary.get("BQ Row Count | Column Count | Missing Columns", "N/A")),
        ("Count Variance", summary.get("Count Variance", "N/A")),
        ("Header Validation", status_color(summary.get("Header Validation", "N/A"))),
        ("Count Validation", status_color(summary.get("Count Validation", "N/A"))),
        ("File Extension Validation", status_color(summary.get("File Extension Validation", "N/A"))),
        ("Checksum Validation", status_color(summary.get("Checksum Validation", "N/A"))),
        ("Status", BOLD + status_color(summary.get("Status", "N/A")) + ENDC),
        ("Passed Columns", passed_cols),
        ("Mismatched Columns", mismatched_cols),
    ]

    for label, val in fields:
        log.info(f"{BOLD}{label:45}:{ENDC} {val}")

    log.info(bar)

with DAG(
    dag_id="compare_gcs_files_runtime_config",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={"dummy": "setme"},
    tags=["gcs", "file-compare"],
) as dag:

    #invoke_function = HttpOperator(
    #    task_id='invoke_compare_function',
    #    http_conn_id='compare_func_http',
    #    endpoint='/',
    #    method='POST',
    #    headers={"Content-Type":"application/json"},
    #    data="{{ dag_run.conf | tojson }}",
    #    response_filter=lambda r: r.json(),
    #    log_response=True,
    #    do_xcom_push=True,
    #)
	
    invoke_function = PythonOperator(
        task_id='invoke_compare_function',
        python_callable=invoke_cloud_run,
        provide_context=True,
    )

    extract_summary = PythonOperator(
        task_id='extract_and_log_summary',
        python_callable=extract_and_log_summary,
        provide_context=True,
    )

    invoke_function >> extract_summary