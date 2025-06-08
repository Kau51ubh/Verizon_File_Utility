from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import json, re

def extract_and_log_summary(**context):
    # 1) pull raw JSON text from the invoke task
    raw = context['ti'].xcom_pull(task_ids='invoke_compare_function')
    payload = json.loads(raw)
    html = payload.get('html_summary','')

    # helper to grab any <th>…</th><td>…</td> cell
    def extract(label):
        pat = rf'<th>{re.escape(label)}</th>\s*<td>(.*?)</td>'
        m = re.search(pat, html, flags=re.DOTALL)
        return m.group(1).strip() if m else ''

    # parse a "X | Y | Z" cell into cleaned parts
    def parse_triplet(cell):
        parts = [p.strip() for p in cell.split('|')]
        # ensure 3 elements
        while len(parts)<3:
            parts.append('')
        row,col,missing = parts[:3]
        # strip stray commas from missing
        missing = missing.lstrip(',').strip()
        if not missing:
            missing = 'None'
        return row, col, missing

    td_cell = extract("TD Row Count | Column Count | Missing Columns")
    bq_cell = extract("BQ Row Count | Column Count | Missing Columns")
    td_row, td_col, td_missing = parse_triplet(td_cell)
    bq_row, bq_col, bq_missing = parse_triplet(bq_cell)

    # passed & mismatched columns lists
    def clean_list(cell):
        items = [c.strip() for c in cell.split(',') if c.strip()]
        return ','.join(items) if items else 'None'

    passed = clean_list(extract("Passed Columns"))
    mismatched = clean_list(extract("Mismatched Columns"))

    # now print in your exact format
    print("\n===== GCS FILE COMPARISON SUMMARY =====")
    print(f"{'Job Name' :35}: {extract('Job Name')}")
    print(f"{'File Name' :35}: {extract('File Name')}")
    print(f"{'TD Row | Columns | Missing' :35}: {td_row} | {td_col} | {td_missing}")
    print(f"{'BQ Row | Columns | Missing' :35}: {bq_row} | {bq_col} | {bq_missing}")
    print(f"{'Count Variance' :35}: {extract('Count Variance')}")
    print(f"{'Header Validation' :35}: {extract('Header Validation')}")
    print(f"{'Count Validation' :35}: {extract('Count Validation')}")
    print(f"{'File Extension Validation' :35}: {extract('File Extension Validation')}")
    print(f"{'Checksum Validation' :35}: {extract('Checksum Validation')}")
    print(f"{'Status' :35}: {extract('Status')}")
    print(f"{'Passed Columns' :35}: {passed}")
    print(f"{'Mismatched Columns' :35}: {mismatched}")
    print("="*40 + "\n")

default_args = {
    'owner':'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='compare_gcs_files_runtime_config',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gcs','file-compare']
) as dag:

    invoke_function = HttpOperator(
        task_id='invoke_compare_function',
        http_conn_id='compare_func_http',
        endpoint='/',
        method='POST',
        headers={"Content-Type":"application/json"},
        data="{{ dag_run.conf | tojson }}",
        response_filter=lambda resp: resp.text,
        log_response=True,
    )

    extract_and_log = PythonOperator(
        task_id='extract_and_log_summary',
        python_callable=extract_and_log_summary,
        provide_context=True,
    )

    invoke_function >> extract_and_log