#!/bin/bash

set -e

JOB_NAME="$1"
TD_FILE="$2"
BQ_FILE="$3"
DELIM="$4"
WIDTHS="$5"
TS="$6"

# These should point to the correct GCSFuse mount locations!
TD_PATH="/mnt/bucket_td/$TD_FILE"
BQ_PATH="/mnt/bucket_bq/$BQ_FILE"

# Output log and summary directory (change as needed)
LOG_DIR="/mnt/bucket_bq/logs/${JOB_NAME}_${TS}"
mkdir -p "$LOG_DIR"
HTML_FILE="$LOG_DIR/${JOB_NAME}_summary.html"

# --- LOG ENV ---
echo "========== Script start =========="
echo "PWD: $PWD"
echo "USER: $(whoami)"
echo "ARGS: $@"
echo "ENVIRONMENT:"
env

echo "Job Name   : $JOB_NAME"
echo "TD File    : $TD_FILE"
echo "BQ File    : $BQ_FILE"
echo "Delimiter  : $DELIM"
echo "Widths     : $WIDTHS"
echo "Timestamp  : $TS"

# --- Existence Check ---
if [[ ! -f "$TD_PATH" ]]; then
    echo "[ERROR] TD file not found: $TD_PATH"
    exit 1
fi
if [[ ! -f "$BQ_PATH" ]]; then
    echo "[ERROR] BQ file not found: $BQ_PATH"
    exit 1
fi

echo "TD file exists? $(ls -lh "$TD_PATH")"
echo "BQ file exists? $(ls -lh "$BQ_PATH")"

# --- Preview first lines for debug ---
echo "First 2 lines of TD file:"
head -2 "$TD_PATH"
echo "First 2 lines of BQ file:"
head -2 "$BQ_PATH"

# --- Header Extraction ---
td_header=$(head -1 "$TD_PATH" | tr -d '\r\n')
bq_header=$(head -1 "$BQ_PATH" | tr -d '\r\n')

# --- Column Counts & Headers ---
readarray -t td_cols < <(awk -F'!\\|' '{for(i=1;i<=NF;i++) print $i}' <<< "$td_header")
readarray -t bq_cols < <(awk -F'!\\|' '{for(i=1;i<=NF;i++) print $i}' <<< "$bq_header")

td_col_count=${#td_cols[@]}
bq_col_count=${#bq_cols[@]}

# --- Row Counts (minus header) ---
td_row_count=$(($(wc -l < "$TD_PATH") - 1))
bq_row_count=$(($(wc -l < "$BQ_PATH") - 1))

# --- Column Comparison ---
missing_in_bq=()
passed_columns=()
for col in "${td_cols[@]}"; do
    if [[ " ${bq_cols[*]} " == *" $col "* ]]; then
        passed_columns+=("$col")
    else
        missing_in_bq+=("$col")
    fi
done

missing_in_td=()
for col in "${bq_cols[@]}"; do
    if [[ " ${td_cols[*]} " != *" $col "* ]]; then
        missing_in_td+=("$col")
    fi
done

# --- Mismatched Columns ---
mismatched_columns="N/A"
if [[ ${#missing_in_bq[@]} -gt 0 || ${#missing_in_td[@]} -gt 0 ]]; then
    mismatched_columns=""
    [[ ${#missing_in_bq[@]} -gt 0 ]] && mismatched_columns+="Missing in BQ: ${missing_in_bq[*]} "
    [[ ${#missing_in_td[@]} -gt 0 ]] && mismatched_columns+="Missing in TD: ${missing_in_td[*]}"
fi

# --- Passed Columns as CSV, remove double commas ---
passed_columns_csv=$(printf "%s," "${passed_columns[@]}")
passed_columns_csv="${passed_columns_csv%,}"

# --- Validations ---
header_status="FAIL"
[[ "$td_header" == "$bq_header" ]] && header_status="PASS"

count_status="FAIL"
[[ "$td_row_count" == "$bq_row_count" ]] && count_status="PASS"

td_ext="${TD_FILE##*.}"
bq_ext="${BQ_FILE##*.}"
ext_status="FAIL"
[[ "$td_ext" == "$bq_ext" ]] && ext_status="PASS"

td_checksum=$(md5sum "$TD_PATH" | awk '{print $1}')
bq_checksum=$(md5sum "$BQ_PATH" | awk '{print $1}')
checksum_status="FAIL"
[[ "$td_checksum" == "$bq_checksum" ]] && checksum_status="PASS"

overall_status="FAIL"
if [[ "$header_status" == "PASS" && "$count_status" == "PASS" && "$ext_status" == "PASS" && "$checksum_status" == "PASS" ]]; then
  overall_status="PASS"
fi

count_variance="N/A"
if [[ "$td_row_count" -ne 0 ]]; then
    count_variance=$(awk -v t="$td_row_count" -v b="$bq_row_count" 'BEGIN{printf "%.2f%%", t==0?0:(t-b)/t*100}')
fi

# --- OUTPUT HTML SUMMARY ---
cat <<EOF > "$HTML_FILE"
<html><body>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Job Name</th><td>${JOB_NAME}</td></tr>
<tr><th>File Name</th><td>$(basename "$TD_FILE")</td></tr>
<tr><th>TD Row Count | Column Count | Missing Columns</th><td>${td_row_count} | ${td_col_count} | ${missing_in_bq[*]:-None}</td></tr>
<tr><th>BQ Row Count | Column Count | Missing Columns</th><td>${bq_row_count} | ${bq_col_count} | ${missing_in_td[*]:-None}</td></tr>
<tr><th>Count Variance</th><td>${count_variance}</td></tr>
<tr><th>Header Validation</th><td>${header_status}</td></tr>
<tr><th>Count Validation</th><td>${count_status}</td></tr>
<tr><th>File Extension Validation</th><td>${ext_status}</td></tr>
<tr><th>Checksum Validation</th><td>${checksum_status}</td></tr>
<tr><th>Status</th><td>${overall_status}</td></tr>
<tr><th>Passed Columns</th><td>${passed_columns_csv}</td></tr>
<tr><th>Mismatched Columns</th><td>${mismatched_columns}</td></tr>
</table>
</body></html>
EOF

echo "HTML Summary generated: $HTML_FILE"
echo "========== Script complete =========="