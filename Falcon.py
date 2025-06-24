"""
File: Falcon_dag.py
Creator: Kaustubh Thakre
Created: 2025-06-10
Last Modified: 2025-06-22

Description:
This Apache Airflow DAG orchestrates the execution of a file-comparison workflow leveraging Google Cloud Platform (GCP) services. It primarily coordinates interactions between Airflow, Cloud Run, and Google Cloud Storage (GCS).

Workflow Overview:
- Accepts user-defined runtime parameters through DAG configuration.
- Authenticates via Google ID tokens to securely invoke the Cloud Run service.
- Triggers a Cloud Run container that compares two input files (TD and BQ files) from GCS buckets.
- Receives a summary response from Cloud Run containing validation results.
- Parses the returned HTML summary, logging detailed validation outcomes directly into Airflow logs for review and auditing.

Airflow Tasks:
- `fetch_token`: Generates an ID token for secure invocation of Cloud Run.
- `invoke_compare`: Calls the Cloud Run endpoint via HTTP POST, triggering the file comparison script execution.
- `extract_and_log_summary`: Processes the HTML summary returned from Cloud Run and logs formatted results for quick verification.

Dependencies:
- airflow
- google-auth
- airflow-providers-http
- beautifulsoup4

GCP Services Integrated:
- Google Cloud Composer (Managed Airflow service)
- Google Cloud Run (Containerized script execution environment)
- Google Cloud Storage (Input/output storage for file comparison artifacts)

Parameters (Configurable at DAG runtime):
- `job_name`: Identifier for the comparison job.
- `TD_File`: Path to the TD source file in GCS.
- `BQ_File`: Path to the BQ source file in GCS.
- `delimiter`: Delimiter used in input files (e.g., comma).
- `widths`: Fixed-width definitions for input files, if applicable.
- `htc`: Flags indicating presence of Header, Trailer, Column Names (format: 'HTC').

Usage:
Trigger manually via Airflow UI or CLI with custom parameters, enabling dynamic execution.

"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import logging
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='Falcon',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'job_name':  'VZ_File_Validation',
        'TD_File':     'udm_rev1/TD File Name',
        'BQ_File':     'udm_rev1/BQ File Name',
        'delimiter': '""',
        'widths':    '""',
        'htc':       '""',
    },
    tags=['falcon', 'file-compare'],
) as dag:

    def fetch_token(**context):
        target = 'https://compare-files-463031084485.us-central1.run.app/'
        return id_token.fetch_id_token(Request(), target)

    fetch_token_op = PythonOperator(
        task_id='fetch_token',
        python_callable=fetch_token,
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=1)
    )

    invoke_compare = HttpOperator(
        task_id='invoke_compare',
        http_conn_id='compare_func_http',
        endpoint='/',
        method='POST',
        headers={
            'Content-Type':  'application/json',
            'Authorization': 'Bearer {{ ti.xcom_pull("fetch_token") }}'
        },
        data='{{ dag_run.conf | tojson }}',
        response_filter=lambda r: r.json(),
        log_response=True,
        do_xcom_push=True,
        execution_timeout=timedelta(hours=2),  # allow up to 2h for Cloud Run
    )

    def extract_and_log_summary(**context):
        ti   = context['ti']
        resp = ti.xcom_pull(task_ids='invoke_compare') or {}
        html = resp.get('html_summary', '')
        log  = ti.log
		
        BLUE_BG = "\033[44;97;1m"
        ENDC = "\033[0m"
        BOLD = "\033[1m"
		
        HEADER = f"{BLUE_BG}  ★★  GCS FILE COMPARISON SUMMARY  ★★  {ENDC}"
		# Pretty summary bar
        bar = f"{BOLD}{'=' * 70}{ENDC}"

        log.info(bar)
        log.info(HEADER)
        log.info(bar)

        if not html:
            log.error('No HTML summary returned!')
            return

        soup = BeautifulSoup(html, 'html.parser')
        summary = {
            cols[0].get_text(strip=True): cols[1].get_text(strip=True)
            for row in soup.find_all('tr')
            for cols in [row.find_all(['th','td'])]
            if len(cols) == 2
        }

        def status_color(val):
            up = val.strip().upper()
            return '✅ PASS' if up=='PASS' else ('❌ FAIL' if up=='FAIL' else '⚠️  N/A')

        keys = [
            'Job Name','File Name',
            'TD Row Count | Column Count | Missing Columns',
            'BQ Row Count | Column Count | Missing Columns',
            'Count Variance','Header Validation','Trailer Validation',
            'Column Name Validation','Count Validation','File Extension Validation',
            'Checksum Validation','Status','Passed Columns','Mismatched Columns'
        ]
        for k in keys:
            v = summary.get(k, 'N/A')
            if 'Validation' in k or k=='Status':
                v = status_color(v)
            log.info(f"{BOLD}{k:45}:{ENDC} {v}")
        log.info(bar)

    extract_summary_op = PythonOperator(
        task_id='extract_and_log_summary',
        python_callable=extract_and_log_summary,
        execution_timeout=timedelta(minutes=10)
    )

    fetch_token_op >> invoke_compare >> extract_summary_op
