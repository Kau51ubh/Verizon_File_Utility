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
    ti = context["ti"]
    resp = ti.xcom_pull(task_ids="invoke_compare_function")
    html = resp.get("html_summary", "")
    log = ti.log

    log.info("===== GCS FILE COMPARISON SUMMARY =====")
    if not html:
        log.info("No HTML summary returned. Raw XCom: %s", resp)
        for field in [
            "Job Name", "File Name", "TD Row Count | Column Count | Missing Columns",
            "BQ Row Count | Column Count | Missing Columns", "Count Variance", "Header Validation",
            "Count Validation", "File Extension Validation", "Checksum Validation", "Status",
            "Passed Columns", "Mismatched Columns"
        ]:
            log.info(f"{field:45}: N/A")
        log.info("="*40)
        return

    soup = BeautifulSoup(html, "html.parser")
    summary = {}
    for row in soup.find_all("tr"):
        cols = row.find_all(["th", "td"])
        if len(cols) == 2:
            key = cols[0].get_text(strip=True)
            val = cols[1].get_text(strip=True)
            summary[key] = val

    # Remove double commas and trailing commas in "Passed Columns"
    passed_cols = summary.get("Passed Columns", "N/A")
    passed_cols = passed_cols.replace(",,", ",")
    if passed_cols.endswith(","):
        passed_cols = passed_cols[:-1]

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
        ("Passed Columns", passed_cols),
        ("Mismatched Columns", summary.get("Mismatched Columns", "N/A")),
    ]

    for label, key in fields:
        val = key if label == "Passed Columns" else summary.get(key, "N/A")
        if label == "Passed Columns":
            val = passed_cols
        log.info(f"{label:45}: {val}")

    log.info("="*40)

with DAG(
    dag_id="compare_gcs_files_runtime_config",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
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