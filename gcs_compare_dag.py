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
    dag_id='compare_gcs_files_sync_parse',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        'job_name':  'VZ_File_Validation',
        'file1':     'udm_rev1/TD File Name',
        'file2':     'udm_rev1/BQ File Name',
        'delimiter': '',
        'widths':    '',
        'htc':       '',
    },
    tags=['gcs', 'file-compare'],
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

        sep = '=' * 70
        log.info(sep)
        log.info('★★ GCS FILE COMPARISON SUMMARY ★★')
        log.info(sep)

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
            'Column Validation','Count Validation','File Extension Validation',
            'Checksum Validation','Status','Passed Columns','Mismatched Columns'
        ]
        for k in keys:
            v = summary.get(k, 'N/A')
            if 'Validation' in k or k=='Status':
                v = status_color(v)
            log.info(f"{k:45}: {v}")
        log.info(sep)

    extract_summary_op = PythonOperator(
        task_id='extract_and_log_summary',
        python_callable=extract_and_log_summary,
        execution_timeout=timedelta(minutes=10)
    )

    fetch_token_op >> invoke_compare >> extract_summary_op
