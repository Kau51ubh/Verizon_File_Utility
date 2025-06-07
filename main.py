import subprocess
import os
from flask import Flask, request, jsonify
from datetime import datetime
from google.cloud import storage

app = Flask(__name__)

@app.route("/", methods=["POST"])
def compare_files(request):            # ← MUST accept `request`
    # parse JSON
    data = request.get_json(silent=True) or {}

    # required
    if "file1" not in data or "file2" not in data:
        return jsonify({
            "status": "failed",
            "message": "Missing required 'file1' or 'file2'"
        }), 400

    job_name  = data.get("job_name", "Validation_Job")
    file1     = data["file1"]
    file2     = data["file2"]
    delimiter = data.get("delimiter", ",")
    widths    = data.get("widths", "")

    # timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # call your shell script
    script = "/workspace/Falcon_DS.sh"
    cmd = [
        "bash", script,
        job_name,
        file1,
        file2,
        delimiter,
        widths,
        timestamp
    ]

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd="/workspace"
    )

    # find HTML summary path
    html_path = ""
    for line in proc.stdout.splitlines():
        if line.startswith("HTML Summary generated:"):
            html_path = line.split(":",1)[1].strip()
            break

    html_content = ""
    if html_path and os.path.isfile(html_path):
        with open(html_path) as f:
            html_content = f.read()

    return jsonify({
        "status":      "success" if proc.returncode == 0 else "failed",
        "stdout":      proc.stdout,
        "stderr":      proc.stderr,
        "html_summary": html_content
    }), (200 if proc.returncode == 0 else 500)

if __name__ == "__main__":
    # Functions-Framework will actually ignore this when deployed,
    # but it lets you run locally with `python main.py`
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)