#!/bin/bash

#==================================================================================================
# Script Name : Falcon.sh
# Creator     : Kaustubh Thakre
# Created     : 2025-06-10
# Last Update : 2025-06-22
#
# Description :
#   This shell script is executed via a Google Cloud Run container as part of a file-validation workflow.
#   It performs a comprehensive comparison between two data files (TD and BQ extracts) stored on GCS buckets.
#
# Workflow :
#   - Receives input parameters: Job Name, TD File Path, BQ File Path, Delimiter, Widths, HTC flags, Timestamp.
#   - Downloads files from Google Cloud Storage (GCS), mounted via Cloud Run using GCSFuse.
#   - Supports multiple file formats: delimited (including multi-character delimiters) and fixed-width files.
#   - Performs validations: header, trailer, column names, row counts, file extensions, checksum validation.
#   - Generates detailed logs, HTML summary reports, and Excel mismatch reports highlighting differences.
#   - Stores all results and intermediate artifacts back to GCS for auditing and reporting purposes.
#   - Provides concise summary statuses (PASS/FAIL) for further pipeline orchestration in Cloud Composer.
#
# Input Parameters :
#   1. JOB       - Unique identifier for the job (used in logs and file names)
#   2. TD_FILE   - GCS path to the source TD file
#   3. BQ_FILE   - GCS path to the source BQ file
#   4. DELIM     - Column delimiter in input files (e.g., ',', '\t', '!|')
#   5. WIDTHS    - Comma-separated field widths for fixed-width files (optional)
#   6. HTC       - Flags indicating presence of Header, Trailer, Column names (Format: 'H|T|C', e.g., 'Y|N|Y')
#   7. TS        - Timestamp used for log directory organization
#
# Output Artifacts (stored in GCS) :
#   - Trimmed input files
#   - Column-level mismatch details (text files)
#   - HTML validation summary report
#   - Detailed column mismatch HTML report
#   - Excel file with mismatches highlighted
#
# Dependencies :
#   - GNU Awk, sed, head, tail, wc, cksum
#   - Python 3 with pandas, openpyxl, google-cloud-storage installed (for Excel generation)
#   - GCSFuse (for mounting GCS buckets in Cloud Run)
#
# Usage :
#   ./Falcon.sh "<JOB>" "<TD_FILE>" "<BQ_FILE>" "<DELIM>" "<WIDTHS>" "<HTC>" "<TIMESTAMP>"
#
# Example :
#   ./Falcon.sh "ValidationJob001" "input_td.csv" "input_bq.csv" "," "" "Y|N|Y" "20250618_104108"
#
# Integrated Cloud Services :
#   - Google Cloud Run (script execution environment)
#   - Google Cloud Storage (persistent storage for logs, inputs, and outputs)
#   - Google Cloud Composer (triggering and orchestration)
#
#==================================================================================================


set -euo pipefail  # strict mode for safer scripting

START_TS=$(date +%s)

# ----------- CONFIG & ARGS -----------
JOB="${1:-N/A}"
TD_FILE="${2:-N/A}"
BQ_FILE="${3:-N/A}"
DELIM="${4:-}"
WIDTHS="${5:-}"
HTC="${6:-}"  # New HTC param
TS="${7:-}"

log() {
  echo "$JOB : $1"
}

TD_PATH="/mnt/bucket_td/$TD_FILE"
BQ_PATH="/mnt/bucket_bq/$BQ_FILE"
LOG_DIR="/mnt/bucket_bq/logs/${JOB}_${TS}"
mkdir -p "$LOG_DIR/HTML"
SUMMARY_HTML="${LOG_DIR}/HTML/${JOB}_summary.html"
COLUMN_DETAIL_HTML="${LOG_DIR}/HTML/${JOB}_column_mismatch_detail.html"
MAX_SAMPLE=10

FILENAME=$(basename "$TD_FILE")

log "========== Script start =========="
log "PWD: $PWD"
log "USER: $(whoami)"
log "ARGS: $*"
log "Job Name   : $JOB"
log "TD File    : $TD_FILE"
log "BQ File    : $BQ_FILE"
log "Delimiter  : $DELIM"
log "Widths     : $WIDTHS"
log "HTC Config : $HTC"
log "Timestamp  : $TS"

# ----------- FILE EXISTENCE -----------
ls -lh "$TD_PATH"
ls -lh "$BQ_PATH"

# ----------- HTC DECODE --------------
IFS='|' read -r HAS_HEADER HAS_TRAILER HAS_COLNAMES <<< "$HTC"

# Normalize blank/undefined to N
HAS_HEADER=${HAS_HEADER:-N}
HAS_TRAILER=${HAS_TRAILER:-N}
HAS_COLNAMES=${HAS_COLNAMES:-N}

# Determine start line and end offset for TRIM
TD_SKIP=0
BQ_SKIP=0
[[ "$HAS_HEADER" == "Y" ]] && ((TD_SKIP++)) && ((BQ_SKIP++))
[[ "$HAS_COLNAMES" == "Y" ]] && ((TD_SKIP++)) && ((BQ_SKIP++))

TD_LINES=$(wc -l < "$TD_PATH")
BQ_LINES=$(wc -l < "$BQ_PATH")
TD_TAIL=0
BQ_TAIL=0
[[ "$HAS_TRAILER" == "Y" ]] && ((TD_TAIL=1)) && ((BQ_TAIL=1))

TD_TRIM="$LOG_DIR/TD_TRIM.txt"
BQ_TRIM="$LOG_DIR/BQ_TRIM.txt"

# First: create trimmed versions
if [[ $TD_TAIL -eq 1 ]]; then
  head -n $((TD_LINES - TD_TAIL)) "$TD_PATH" | tail -n +$TD_SKIP > "$TD_TRIM"
else
  tail -n +$TD_SKIP "$TD_PATH" > "$TD_TRIM"
fi

if [[ $BQ_TAIL -eq 1 ]]; then
  head -n $((BQ_LINES - BQ_TAIL)) "$BQ_PATH" | tail -n +$BQ_SKIP > "$BQ_TRIM"
else
  tail -n +$BQ_SKIP "$BQ_PATH" > "$BQ_TRIM"
fi

# SORT TRIMMED FILES FOR DETERMINISTIC ROW ALIGNMENT
sort "$TD_TRIM" -o "$TD_TRIM"
sort "$BQ_TRIM" -o "$BQ_TRIM"

# Then: apply fixed-width parsing if WIDTHS is provided
if [[ -n "$WIDTHS" ]]; then
  log "Converting fixed-width files to tab-delimited using WIDTHS: $WIDTHS"

  # Convert comma-separated widths to array
  IFS=',' read -r -a FIELD_WIDTHS <<< "$WIDTHS"

  convert_fixed_to_tab() {
  local infile="$1"
  local outfile="$2"

  awk -v OFS='\t' -v widths_csv="$WIDTHS" '
    BEGIN {
      n = split(widths_csv, w, ",");
      wstart[1] = 1;
      for (i = 2; i <= n; i++) {
        wstart[i] = wstart[i - 1] + w[i - 1];
      }
    }
    {
      for (i = 1; i <= n; i++) {
        printf "%s%s", substr($0, wstart[i], w[i]), (i == n ? "\n" : OFS);
      }
    }
  ' "$infile" > "$outfile"
}

  convert_fixed_to_tab "$TD_TRIM" "$TD_TRIM.tmp" && mv "$TD_TRIM.tmp" "$TD_TRIM"
  convert_fixed_to_tab "$BQ_TRIM" "$BQ_TRIM.tmp" && mv "$BQ_TRIM.tmp" "$BQ_TRIM"
  log "Fixed-width files successfully converted to tab-delimited."

  DELIM=$'\t'
  SPLIT_DELIM=$'\t'
fi

# Extract headers from files (adjusted for HAS_HEADER + HAS_COLNAMES)
TD_HEADER_LINE=1
BQ_HEADER_LINE=1
[[ "$HAS_HEADER" == "Y" ]] && ((TD_HEADER_LINE++)) && ((BQ_HEADER_LINE++))
td_header=$(sed -n "${TD_HEADER_LINE}p" "$TD_PATH" | tr -d '\r\n')
bq_header=$(sed -n "${BQ_HEADER_LINE}p" "$BQ_PATH" | tr -d '\r\n')

# --------- Universal delimiter handling ---------
if [[ "$DELIM" == "\\t" ]]; then
  DELIM=$'\t'
fi

if [[ "${#DELIM}" -gt 1 ]]; then
  td_header_fixed="${td_header//${DELIM}/$'\t'}"
  bq_header_fixed="${bq_header//${DELIM}/$'\t'}"
  SPLIT_DELIM=$'\t'
else
  td_header_fixed="$td_header"
  bq_header_fixed="$bq_header"
  SPLIT_DELIM="$DELIM"
fi

# Determine column names
td_cols=()
bq_cols=()

if [[ "$HAS_COLNAMES" == "Y" ]]; then
  # Extract headers from header line
  IFS=$'\n' read -d '' -r -a td_cols < <(
  echo "$td_header_fixed" |
  awk -F"$SPLIT_DELIM" '{
    for(i=1;i<=NF;i++) {
      if ($i ~ /^[[:space:]]*$/) {
        printf("COL%d\n", i)
      } else {
        print $i
      }
    }
  }'
  printf '\0'
)

IFS=$'\n' read -d '' -r -a bq_cols < <(
  echo "$bq_header_fixed" |
  awk -F"$SPLIT_DELIM" '{
    for(i=1;i<=NF;i++) {
      if ($i ~ /^[[:space:]]*$/) {
        printf("COL%d\n", i)
      } else {
        print $i
      }
    }
  }'
  printf '\0'
)
else
  # Generate dummy columns from first data row
  td_col_count=$(head -1 "$TD_TRIM" | awk -F"$SPLIT_DELIM" '{print NF}')
  bq_col_count=$(head -1 "$BQ_TRIM" | awk -F"$SPLIT_DELIM" '{print NF}')
  for ((i=1; i<=td_col_count; i++)); do
    colname="COL$i"
    td_cols+=("$colname")
    bq_cols+=("$colname")
  done
fi

# Now assign column count correctly after array creation
td_col_count=${#td_cols[@]}
bq_col_count=${#bq_cols[@]}

# log "TD header: $td_header"
# log "BQ header: $bq_header"
# log "TD col count: $td_col_count"
# log "BQ col count: $bq_col_count"

# ----------- ROW COUNTS ---------------
TD_ROWS=$(awk 'END{print NR-1}' "$TD_TRIM")
BQ_ROWS=$(awk 'END{print NR-1}' "$BQ_TRIM")

# ----------- FIND MISSING COLUMNS ------
td_missing=""
bq_missing=""

for i in "${td_cols[@]}"; do
    found=0
    for j in "${bq_cols[@]}"; do
        [[ "$i" == "$j" ]] && found=1 && break
    done
    [[ $found -eq 0 ]] && td_missing="$td_missing $i"
done

for j in "${bq_cols[@]}"; do
    found=0
    for i in "${td_cols[@]}"; do
        [[ "$j" == "$i" ]] && found=1 && break
    done
    [[ $found -eq 0 ]] && bq_missing="$bq_missing $j"
done

[[ -z "$td_missing" ]] && td_missing="None"
[[ -z "$bq_missing" ]] && bq_missing="None"

# ----------- COUNT VARIANCE ------------
if [[ $TD_ROWS -eq 0 || $BQ_ROWS -eq 0 ]]; then
    COUNT_VAR="N/A"
else
    COUNT_VAR=$(awk -v td="$TD_ROWS" -v bq="$BQ_ROWS" 'BEGIN{printf "%.2f%%", ((td-bq)/td)*100}')
fi

# Default validations
HEADER_VAL="N/A"
TRAILER_VAL="N/A"
COLUMN_VAL="N/A"

# ----------- HEADER VALIDATION ----------
if [[ "$HAS_HEADER" == "Y" ]]; then
  hdr_td=$(sed -n '1p' "$TD_PATH" || true)
  hdr_bq=$(sed -n '1p' "$BQ_PATH" || true)
  if [[ -n "$hdr_td" && -n "$hdr_bq" && "$hdr_td" == "$hdr_bq" ]]; then
    HEADER_VAL="PASS"
  else
    HEADER_VAL="FAIL"
  fi
fi

# ----------- TRAILER VALIDATION ----------
if [[ "$HAS_TRAILER" == "Y" ]]; then
  trl_td=$(tail -n 1 "$TD_PATH" || true)
  trl_bq=$(tail -n 1 "$BQ_PATH" || true)
  if [[ -n "$trl_td" && -n "$trl_bq" && "$trl_td" == "$trl_bq" ]]; then
    TRAILER_VAL="PASS"
  else
    TRAILER_VAL="FAIL"
  fi
fi

# ----------- COLUMN VALIDATION ----------
if [[ "$HAS_COLNAMES" == "Y" ]]; then
  if [[ "$td_header" == "$bq_header" ]]; then
    COLUMN_VAL="PASS"
  else
    COLUMN_VAL="FAIL"
  fi
fi

COUNT_VAL="PASS"
if [[ $TD_ROWS -ne $BQ_ROWS ]]; then
  COUNT_VAL="FAIL"
fi

# ----------- EXTENSION VALIDATION ------
td_ext="${FILENAME##*.}"
bq_ext="${BQ_FILE##*.}"
FILE_EXT_VAL="PASS"
if [[ "$td_ext" != "$bq_ext" ]]; then
  FILE_EXT_VAL="FAIL"
fi

# ----------- FAST FILE CHECKSUM VALIDATION -----------
TD_FILE_CHK=$(cksum "$TD_TRIM" | awk '{print $1}')
BQ_FILE_CHK=$(cksum "$BQ_TRIM" | awk '{print $1}')
FAST_CHECKSUM_MATCHED=0
CHECKSUM_VAL="N/A"

if [[ "$TD_FILE_CHK" == "$BQ_FILE_CHK" ]]; then
    CHECKSUM_VAL="PASS"
    FAST_CHECKSUM_MATCHED=1
else
    CHECKSUM_VAL="FAIL"
fi

# ----------- EFFICIENT STREAMING COLUMN/ROW COMPARISON -----------
passed_columns=()
mismatched_columns=()

if [[ $FAST_CHECKSUM_MATCHED -eq 0 && $td_col_count -eq $bq_col_count && $TD_ROWS -eq $BQ_ROWS ]]; then

  # Prepare temp files without headers
  TD_TEMP="${LOG_DIR}/TD_TEMP_DATA.txt"
  BQ_TEMP="${LOG_DIR}/BQ_TEMP_DATA.txt"

  if [[ ${#DELIM} -gt 1 ]]; then
    tail -n +2 "$TD_TRIM" | sed "s|${DELIM}|	|g" > "$TD_TEMP"
    tail -n +2 "$BQ_TRIM" | sed "s|${DELIM}|	|g" > "$BQ_TEMP"
    SPLIT_DELIM=$'\t'
  else
    tail -n +2 "$TD_TRIM" > "$TD_TEMP"
    tail -n +2 "$BQ_TRIM" > "$BQ_TEMP"
    SPLIT_DELIM="$DELIM"
  fi

  colnames_csv=$(IFS=, ; echo "${td_cols[*]}")

# Column row comparison logic 

awk -F"$SPLIT_DELIM" -v logdir="$LOG_DIR" -v job="$JOB" -v cnames="$colnames_csv" '
BEGIN {
  n = split(cnames, colname, ",")
  for (i = 1; i <= n; i++) {
    if (length(colname[i]) == 0) {
      # Skip empty column name
      continue
    }
    td_file[i] = sprintf("%s/%s_%s_mismatch_td.txt", logdir, job, colname[i])
    bq_file[i] = sprintf("%s/%s_%s_mismatch_bq.txt", logdir, job, colname[i])
  }
}
NR==FNR {
  for (i = 1; i <= NF; i++) {
    td_rows[FNR, i] = $i
  }
  next
}
{
  row_num = FNR  # For BQ file
  for (i = 1; i <= NF; i++) {
    if (!(i in td_file)) continue  # Skip if filename was not created
    if ($i != td_rows[row_num, i]) {
      print "Row " row_num+1 ": " td_rows[row_num, i] >> td_file[i]
      print "Row " row_num+1 ": " $i >> bq_file[i]
    }
  }
}
' "$TD_TEMP" "$BQ_TEMP"

  # Determine passed/mismatched columns based on files created
  for ((i=1; i<=td_col_count; i++)); do
    col="${td_cols[$((i-1))]}"
    mismatch_file="${LOG_DIR}/${JOB}_${col}_mismatch_td.txt"
    if [[ -s "$mismatch_file" ]]; then
      mismatched_columns+=("$col")
    else
      passed_columns+=("$col")
    fi
  done

else
  # fast checksum matched → all columns passed
  if [[ $FAST_CHECKSUM_MATCHED -eq 1 ]]; then
    passed_columns=("${td_cols[@]}")
    mismatched_columns=()
  fi
fi

PASSED_COLUMNS=$(IFS=, ; echo "${passed_columns[*]:-N/A}")
MISMATCHED_COLUMNS=$(IFS=, ; echo "${mismatched_columns[*]:-N/A}")

mismatched_columns_with_counts=()
for col in "${mismatched_columns[@]}"; do
  td_mis_c=$(wc -l < "${LOG_DIR}/${JOB}_${col}_mismatch_td.txt")
  mismatched_columns_with_counts+=("${col}(${td_mis_c})")
done

MISMATCHED_COLUMNS_WITH_COUNTS=$(IFS=, ; echo "${mismatched_columns_with_counts[*]}")

# ----------- STATUS AGGREGATE ----------
STATUS="PASS"
for s in "$COUNT_VAL" "$FILE_EXT_VAL" "$CHECKSUM_VAL"; do
    if [[ "$s" == "FAIL" ]]; then
        STATUS="FAIL"
        break
    fi
done

log "========== Summary  =========="
log "Job Name: $JOB"
log "File Name: $FILENAME"
log "TD Row Count | Column Count | Missing Columns: $TD_ROWS | $td_col_count | $bq_missing"
log "BQ Row Count | Column Count | Missing Columns: $BQ_ROWS | $bq_col_count | $td_missing"
log "Count Variance: $COUNT_VAR"
log "Header Validation: $HEADER_VAL"
log "Trailer Validation: $TRAILER_VAL"
log "Column Name Validation: $COLUMN_VAL"
log "Count Validation: $COUNT_VAL"
log "File Extension Validation: $FILE_EXT_VAL"
log "Checksum Validation: $CHECKSUM_VAL"
log "Validation Status: $STATUS"
log "Passed Columns: $PASSED_COLUMNS"
log "Mismatched Columns: $MISMATCHED_COLUMNS_WITH_COUNTS"
log "=============================="

# ----------- WRITE SUMMARY HTML -----------
cat <<EOF > "$SUMMARY_HTML"
<html><body>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Job Name</th><td>$JOB</td></tr>
<tr><th>File Name</th><td>$FILENAME</td></tr>
<tr><th>TD Row Count | Column Count | Missing Columns</th><td>$TD_ROWS | $td_col_count | $bq_missing</td></tr>
<tr><th>BQ Row Count | Column Count | Missing Columns</th><td>$BQ_ROWS | $bq_col_count | $td_missing</td></tr>
<tr><th>Count Variance</th><td>$COUNT_VAR</td></tr>
<tr><th>Header Validation</th><td>$HEADER_VAL</td></tr>
<tr><th>Trailer Validation</th><td>$TRAILER_VAL</td></tr>
<tr><th>Column Name Validation</th><td>$COLUMN_VAL</td></tr>
<tr><th>Count Validation</th><td>$COUNT_VAL</td></tr>
<tr><th>File Extension Validation</th><td>$FILE_EXT_VAL</td></tr>
<tr><th>Checksum Validation</th><td>$CHECKSUM_VAL</td></tr>
<tr><th>Status</th><td>$STATUS</td></tr>
<tr><th>Passed Columns</th><td>$PASSED_COLUMNS</td></tr>
<tr><th>Mismatched Columns</th><td>$MISMATCHED_COLUMNS_WITH_COUNTS</td></tr>
</table>
</body></html>
EOF

echo "HTML Summary generated: $SUMMARY_HTML"

log "HTML Summary generated: $SUMMARY_HTML"

# ----------- WRITE COLUMN DETAIL HTML -----------
cat <<EOT > "$COLUMN_DETAIL_HTML"
<html>
<head>
  <meta charset="UTF-8" />
  <style>
    body {
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      background: #f9f9fc;
      margin: 0;
      color: #322b4f;
      line-height: 1.5;
    }
    .hdr {
      background: #322b4f;
      color: #fff;
      font-size: 2.4em;
      font-weight: 700;
      padding: 22px 40px;
      border-radius: 0 0 12px 12px;
      box-shadow: 0 3px 14px rgba(50, 43, 79, 0.3);
      user-select: none;
    }
    .section {
      margin: 2.5em 4% 1.5em 4%;
      font-size: 1.3em;
      font-weight: 700;
      color: #322b4f;
      border-left: 5px solid #322b4f;
      padding-left: 14px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      user-select: none;
    }
    .scroll-table-container {
      width: 92%;
      margin: 0 auto 2em auto;
      border-radius: 14px;
      box-shadow: 0 8px 30px rgba(79, 64, 128, 0.12);
      background: #fff;
      overflow-x: auto;
      max-height: 360px;
      overflow-y: auto;
      border: 1px solid #dddcec;
    }
    table.tbl {
      border-collapse: separate;
      border-spacing: 0;
      width: 100%;
      min-width: 900px;
      font-size: 0.95em;
      color: #322b4f;
    }
    table.tbl th, table.tbl td {
      padding: 14px 20px;
      text-align: left;
      white-space: nowrap;
      border-bottom: 1px solid #edeaf1;
      vertical-align: middle;
    }
    table.tbl th {
      background: #322b4f;
      color: #fff;
      font-weight: 700;
      position: sticky;
      top: 0;
      z-index: 3;
      letter-spacing: 0.04em;
    }
    .rowname {
      font-weight: 700;
      background: #edeaf1 !important;
      color: #000000 !important;
      position: sticky;
      left: 0;
      z-index: 2;
      border-right: 1px solid #dddcec;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      user-select: none;
      min-width: 38px;
      text-align: center;
    }
    .miss {
      background: #ffe1e1 !important;
      color: #b93d3d !important;
      font-weight: 700;
      border-radius: 6px;
      box-shadow: 0 0 10px #f5b7b1 inset;
    }
    .colhead {
      background: #edeaf1;
      color: #322b4f;
      font-weight: 700;
      text-align: left;
      letter-spacing: 0.03em;
    }
    /* Scrollbar styling */
    .scroll-table-container::-webkit-scrollbar {
      height: 10px;
      background: #edeaf1;
      border-radius: 6px;
    }
    .scroll-table-container::-webkit-scrollbar-thumb {
      background: #c0b6e1;
      border-radius: 6px;
    }
    /* Zebra striping for readability */
    table.tbl tr:nth-child(even) td {
      background: #f9f7ff;
    }
    /* TD/BQ label cells */
    .td-label, .bq-label {
      background: #fff;
      color: #322b4f;
      font-weight: 700;
      text-align: center;
      border-radius: 4px;
      min-width: 40px;
    }
  </style>
</head>
<body>
<div class="hdr">Sample Data Mismatch - $JOB - $FILENAME - ${TS} </div>
EOT

# Write detailed mismatch info into HTML
for col in "${mismatched_columns[@]}"; do
  idx=0
  for i in "${!td_cols[@]}"; do
    [[ "${td_cols[$i]}" == "$col" ]] && idx=$((i+1)) && break
  done

  td_file="${LOG_DIR}/${JOB}_${col}_mismatch_td.txt"
  bq_file="${LOG_DIR}/${JOB}_${col}_mismatch_bq.txt"
  [[ ! -s "$td_file" ]] && continue
  
  td_mis_cnt=$(wc -l < "$td_file")
  bq_mis_cnt=$(wc -l < "$bq_file")
  
  # Extract row numbers from mismatch file - these are original file line numbers (including header)
  readarray -t lines < <(awk '{ gsub(/^Row[[:space:]]/,""); sub(/:.*$/,""); print }' "$td_file" | head -n $MAX_SAMPLE)
  
  printf '<div class="section"><strong>Column:</strong> %s - Mismatched Records (%d)</div>\n' "$col" "$td_mis_cnt" >> "$COLUMN_DETAIL_HTML"

  for ln in "${lines[@]}"; do
    adjusted_ln=$((ln - 1))

    td_row=$(awk -F"$SPLIT_DELIM" -v r="$adjusted_ln" 'NR==r{print}' "$TD_TEMP")
    bq_row=$(awk -F"$SPLIT_DELIM" -v r="$adjusted_ln" 'NR==r{print}' "$BQ_TEMP")

    [[ -z "$td_row" && -z "$bq_row" ]] && continue

    IFS="$SPLIT_DELIM" read -r -a td_arr <<< "$td_row"
    IFS="$SPLIT_DELIM" read -r -a bq_arr <<< "$bq_row"

    cat >> "$COLUMN_DETAIL_HTML" <<EOF
  <div class="scroll-table-container">
    <table class="tbl">
      <tr><th class="rowname">Row $ln</th>
$(for h in "${td_cols[@]}"; do printf '        <th class="colhead">%s</th>\n' "$h"; done)
      </tr>
      <tr><td class="td-label">TD</td>
$(for j in "${!td_arr[@]}"; do
     if (( j+1 == idx )); then
       printf '        <td class="miss">%s</td>\n' "${td_arr[j]}"
     else
       printf '        <td>%s</td>\n' "${td_arr[j]}"
     fi
   done)
      </tr>
      <tr><td class="bq-label">BQ</td>
$(for j in "${!bq_arr[@]}"; do
     if (( j+1 == idx )); then
       printf '        <td class="miss">%s</td>\n' "${bq_arr[j]}"
     else
       printf '        <td>%s</td>\n' "${bq_arr[j]}"
     fi
   done)
      </tr>
    </table>
  </div>
  <br/>
EOF
  done
done

# Close HTML tags
cat >> "$COLUMN_DETAIL_HTML" <<EOF
</body>
</html>
EOF

log "Column detail HTML generated: $COLUMN_DETAIL_HTML"

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

if [[ $ELAPSED -lt 60 ]]; then
  log "Validation completed in ${ELAPSED} seconds"
else
  mins=$((ELAPSED / 60))
  secs=$((ELAPSED % 60))
  log "Validation completed in ${mins} minutes, ${secs} seconds"
fi

# ----------- GENERATE EXCEL WITH ALL MISMATCHED ROWS IN CONTEXT -----------
if [[ ${#mismatched_columns[@]} -gt 0 ]]; then
  log "Generating Excel summary with all mismatched rows"

  # Ensure these vars are available inside Python
  export LOG_DIR JOB TD_TEMP BQ_TEMP colnames_csv SPLIT_DELIM GCS_BUCKET_BQ

  python3 <<EOF
import pandas as pd
from openpyxl.styles import PatternFill
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import os
from google.cloud import storage

# --- Inputs from shell ---
log_dir    = os.environ["LOG_DIR"]
job        = os.environ["JOB"]
td_temp    = os.environ["TD_TEMP"]
bq_temp    = os.environ["BQ_TEMP"]
colnames   = os.environ["colnames_csv"].split(',')
sep        = os.environ["SPLIT_DELIM"]

# --- Read files ---
df_td = pd.read_csv(td_temp, sep=sep, header=None, names=colnames, dtype=str, keep_default_na=False)
df_bq = pd.read_csv(bq_temp, sep=sep, header=None, names=colnames, dtype=str, keep_default_na=False)

# --- Identify mismatches ---
mask      = df_td.ne(df_bq)
rows      = mask.any(axis=1)
df_td_f   = df_td[rows].copy()
df_bq_f   = df_bq[rows].copy()
mask_f    = mask[rows]

# --- Build Excel workbook ---
wb = Workbook()
wb.remove(wb.active)
highlight = PatternFill(start_color='FF9999', end_color='FF9999', fill_type='solid')

def add_sheet(ws, df, mask_df):
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), start=1):
        for c_idx, val in enumerate(row, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            # highlight data rows where mask is True
            if r_idx > 1 and mask_df.iat[r_idx-2, c_idx-1]:
                cell.fill = highlight

ws_td = wb.create_sheet(title="TD_Mismatches")
add_sheet(ws_td, df_td_f, mask_f)

ws_bq = wb.create_sheet(title="BQ_Mismatches")
add_sheet(ws_bq, df_bq_f, mask_f)

# --- Save Excel locally ---
excel_filename = f"{job}_mismatches_highlighted.xlsx"
excel_path     = os.path.join(log_dir, excel_filename)
wb.save(excel_path)

# --- Upload to GCS into the HTML folder under the same log dir ---
bucket_uri = os.environ["GCS_BUCKET_BQ"].replace("gs://", "")
client     = storage.Client()
bucket     = client.bucket(bucket_uri)

# Derive the GCS path from the mount path, e.g.
# /mnt/bucket_bq/logs/MyJob_20250618_104108 -> logs/MyJob_20250618_104108
rel_dir   = log_dir.replace("/mnt/bucket_bq/", "").rstrip("/")
dest_blob = f"{rel_dir}/HTML/{excel_filename}"

blob = bucket.blob(dest_blob)
blob.upload_from_filename(
    excel_path,
    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# Ensure the download name is .xlsx
blob.content_disposition = f'attachment; filename="{excel_filename}"'
blob.patch()

print(f"Excel mismatch summary created: {excel_path}")
EOF

  log "Excel mismatch summary created: ${LOG_DIR}/${JOB}_mismatches_highlighted.xlsx"
fi
rm ${LOG_DIR}/${JOB}_mismatches_highlighted.xlsx
log "========== Script complete =========="
