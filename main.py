"""
File: main.py
Creator: Kaustubh Thakre
Created: 2025-06-10
Last Modified: 2025-06-22

Description:
This Python script is designed to be executed in a Google Cloud Run environment. It orchestrates
the execution of a shell script that compares two input files stored in Google Cloud Storage (GCS).

Workflow:
- Receives runtime parameters via HTTP requests triggered from Cloud Composer (Airflow DAG).
- Retrieves input files (e.g., TD and BQ extracts) from GCS buckets.
- Executes a shell script to compare files and generate validation summaries.
- Writes detailed validation outputs (HTML summary, Excel report, raw logs, column-wise mismatch details) back to a specified GCS bucket.
- Sends minimal summary response (status and summary link) back to Composer logs for pipeline orchestration.

Cloud Services Integration:
- Google Cloud Composer (for workflow management and DAG orchestration)
- Google Cloud Run (containerized execution environment)
- Google Cloud Storage (storage for input/output files and logs)

Dependencies:
- functions-framework==3.*
- google-cloud-storage
- pandas
- flask
- openpyxl

"""

import os
import subprocess
import datetime
import json
import functions_framework
import traceback
from flask import Response

@functions_framework.http
def compare_files(request):
    try:
        # Step 1: Extract request payload
        data = request.get_json(force=True)
        job_name = data.get("job_name", "Validation_Job")
        TD_File = data["TD_File"]
        BQ_File = data["BQ_File"]
        delimiter = data.get("delimiter", ",")
        widths = data.get("widths", "")
        htc = data.get("htc", "")
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Step 2: Print environment and file structure for debugging
        print("DEBUG: ENV:", dict(os.environ))
        print("DEBUG: Files in /:", os.listdir("/"))
        print("DEBUG: Files in /workspace:", os.listdir("/workspace"))

        # Step 3: Log input parameters
        print(f"DEBUG: Running script with: job_name={job_name}, TD_File={TD_File}, BQ_File={BQ_File}, delimiter={delimiter}, widths={widths}, htc={htc}, timestamp={timestamp}")

        # Step 4: Confirm TD and BQ file existence
        td_full = f"/mnt/bucket_td/{TD_File}"
        bq_full = f"/mnt/bucket_bq/{BQ_File}"

        print(f"DEBUG: TD file exists? {os.path.exists(td_full)} {td_full}")
        print(f"DEBUG: BQ file exists? {os.path.exists(bq_full)} {bq_full}")

        # Step 5: Run the shell script
        script = "/workspace/Falcon.sh"
        cmd = ["bash", script, job_name, TD_File, BQ_File, delimiter, widths, htc, timestamp]
        proc = subprocess.run(cmd, capture_output=True, text=True)

        print("DEBUG: SCRIPT completed with return code:", proc.returncode)
        print("DEBUG: SCRIPT STDOUT:")
        print(proc.stdout)
        print("DEBUG: SCRIPT STDERR:")
        print(proc.stderr)

        # Step 6: Parse HTML summary path from STDOUT
        html_summary = ""
        summary_path = ""
        for line in proc.stdout.splitlines():
            if "HTML Summary generated:" in line:
                summary_path = line.split(":", 1)[1].strip()
                break
        # Fallback if not found
        if not summary_path:
            summary_path = f"/mnt/bucket_bq/logs/{job_name}/HTML/{job_name}_Summary.html"

        # Read the summary HTML if it exists
        if os.path.exists(summary_path):
            with open(summary_path) as f:
                html_summary = f.read()
        else:
            print("DEBUG: Summary file not found at:", summary_path)

        # Step 7: Build public GCS URL for the summary
        # Expect an env var GCS_BUCKET_BQ like 'gs://my-bucket'
        bucket_uri = os.environ.get("GCS_BUCKET_BQ", "").strip()
        bucket_name = bucket_uri[5:] if bucket_uri.lower().startswith("gs://") else bucket_uri
        # Convert local path '/mnt/bucket_bq/...' to GCS key 'logs/...'
        if summary_path.startswith("/mnt/bucket_bq/"):
            gcs_key = summary_path[len("/mnt/bucket_bq/"):].lstrip("/")
        else:
            gcs_key = summary_path.lstrip("/")
        summary_url = f"https://storage.googleapis.com/{bucket_name}/{gcs_key}"

        # Step 8: Build response
        status = "success" if proc.returncode == 0 else "failed"
        resp = {
            "status": status,
            "html_summary": html_summary,
            "summary_url": summary_url
        }
        if proc.returncode != 0:
            resp["error_tail"] = proc.stderr.strip().splitlines()[-5:]

        return Response(
            response=json.dumps(resp),
            status=200,
            mimetype="application/json"
        )

    except Exception as e:
        print("DEBUG: Exception occurred:", str(e))
        traceback.print_exc()
        return Response(
            response=json.dumps({
                "status": "failed",
                "message": str(e)
            }),
            status=500,
            mimetype="application/json"
        )
