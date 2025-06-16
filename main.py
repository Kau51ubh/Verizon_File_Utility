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
        file1 = data["file1"]
        file2 = data["file2"]
        delimiter = data.get("delimiter", ",")
        widths = data.get("widths", "")
        htc = data.get("htc", "")
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Step 2: Print environment and file structure for debugging
        print("DEBUG: ENV:", dict(os.environ))
        print("DEBUG: Files in /:", os.listdir("/"))
        print("DEBUG: Files in /workspace:", os.listdir("/workspace"))

        # Step 3: Log input parameters
        print(f"DEBUG: Running script with: job_name={job_name}, file1={file1}, file2={file2}, delimiter={delimiter}, widths={widths}, htc={htc}, timestamp={timestamp}")

        # Step 4: Confirm TD and BQ file existence
        td_full = f"/mnt/bucket_td/{file1}"
        bq_full = f"/mnt/bucket_bq/{file2}"

        td_exists = os.path.exists(td_full)
        bq_exists = os.path.exists(bq_full)

        print(f"DEBUG: TD file exists? {td_exists} {td_full}")
        print(f"DEBUG: BQ file exists? {bq_exists} {bq_full}")

        if td_exists:
            with open(td_full) as f:
                print("DEBUG: First lines of TD:", [next(f).strip() for _ in range(1)])
        if bq_exists:
            with open(bq_full) as f:
                print("DEBUG: First lines of BQ:", [next(f).strip() for _ in range(1)])

        # Step 5: Run the shell script
        script = "/workspace/Falcon_DS.sh"
        cmd = ["bash", script, job_name, file1, file2, delimiter, widths, htc, timestamp]
        proc = subprocess.run(cmd, capture_output=True, text=True)

        print("DEBUG: SCRIPT completed with return code:", proc.returncode)
        print("DEBUG: SCRIPT STDOUT:")
        print(proc.stdout)
        print("DEBUG: SCRIPT STDERR:")
        print(proc.stderr)

        # Step 6: Parse HTML summary from STDOUT
        html_summary = ""
        summary_path = ""
        for line in proc.stdout.splitlines():
            if "HTML Summary generated:" in line:
                summary_path = line.split(":", 1)[1].strip()
                break

        if not summary_path:
            summary_path = f"/mnt/bucket_bq/logs/{job_name}/HTML/{job_name}_Summary.html"

        if os.path.exists(summary_path):
            with open(summary_path) as f:
                html_summary = f.read()
        else:
            print("DEBUG: Summary file not found at:", summary_path)

        # Step 7: Return result
        resp = {
            "status": "success" if proc.returncode == 0 else "failed",
            "html_summary": html_summary
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