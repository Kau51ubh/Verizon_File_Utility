import os
import subprocess
import datetime
import json
import functions_framework
import traceback

@functions_framework.http
def compare_files(request):
    try:
        data = request.get_json(force=True)
        job_name = data.get("job_name", "Validation_Job")
        file1 = data["file1"]
        file2 = data["file2"]
        delimiter = data.get("delimiter", ",")
        widths = data.get("widths", "")
        htc = data.get("htc", "")  # New HTC param
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        print("DEBUG: ENV:", dict(os.environ))
        print("DEBUG: Files in /:", os.listdir("/"))
        print("DEBUG: Files in /workspace:", os.listdir("/workspace"))
        print(f"DEBUG: Running script with: job_name={job_name}, file1={file1}, file2={file2}, delimiter={delimiter}, widths={widths}, htc={htc}, timestamp={timestamp}")

        td_full = f"/mnt/bucket_td/{file1}"
        bq_full = f"/mnt/bucket_bq/{file2}"
        print(f"DEBUG: TD file exists? {os.path.exists(td_full)} {td_full}")
        print(f"DEBUG: BQ file exists? {os.path.exists(bq_full)} {bq_full}")
        if os.path.exists(td_full):
            with open(td_full) as f:
                lines = []
                try:
                    for _ in range(2):
                        lines.append(next(f).rstrip('\n'))
                except StopIteration:
                    pass
                print("DEBUG: First lines of TD:", lines)
        if os.path.exists(bq_full):
            with open(bq_full) as f:
                lines = []
                try:
                    for _ in range(2):
                        lines.append(next(f).rstrip('\n'))
                except StopIteration:
                    pass
                print("DEBUG: First lines of BQ:", lines)

        script = "/workspace/Falcon_DS.sh"
        cmd = [
            "bash", script,
            job_name, file1, file2, delimiter, widths, htc, timestamp  # <-- Added htc
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        print("DEBUG: SCRIPT STDOUT:", proc.stdout)
        print("DEBUG: SCRIPT STDERR:", proc.stderr)

        resp = {
            "status": "success" if proc.returncode == 0 else "failed",
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "html_summary": ""
        }

        for line in proc.stdout.splitlines():
            if "HTML Summary generated:" in line:
                path = line.split(":", 1)[1].strip()
                if os.path.exists(path):
                    with open(path) as h:
                        resp["html_summary"] = h.read()
                break

        return (json.dumps(resp), 200, {"Content-Type": "application/json"})

    except Exception as e:
        print("DEBUG: Exception:", str(e))
        traceback.print_exc()
        return (json.dumps({
            "status": "failed",
            "message": str(e)
        }), 500, {"Content-Type": "application/json"})
