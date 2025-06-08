#!/bin/bash

set -e

# ----------- CONFIG & ARGS -----------
JOB="$1"
TD_FILE="$2"
BQ_FILE="$3"
DELIM="$4"
WIDTHS="$5"
TS="$6"

TD_PATH="/mnt/bucket_td/$TD_FILE"
BQ_PATH="/mnt/bucket_bq/$BQ_FILE"
LOG_DIR="/mnt/bucket_bq/logs/${JOB}_${TS}"
mkdir -p "$LOG_DIR"
SUMMARY_HTML="${LOG_DIR}/${JOB}_summary.html"

FILENAME=$(basename "$TD_FILE")
echo "========== Script start =========="
echo "PWD: $PWD"
echo "USER: $(whoami)"
echo "ARGS: $@"
echo "ENVIRONMENT:"
env | grep -E 'PYTHONUNBUFFERED|LANGUAGE|K_REVISION|SERVER_SOFTWARE|CLOUD_RUN_TIMEOUT_SECONDS|PWD|FUNCTION_SIGNATURE_TYPE|PORT|CNB_STACK_ID|CNB_GROUP_ID|HOME|LANG|K_SERVICE|GAE_RUNTIME|SHLVL|CNB_USER_ID|PYTHONDONTWRITEBYTECODE|LD_LIBRARY_PATH|LC_ALL|PATH|PYTHONUSERBASE|FUNCTION_TARGET|K_CONFIGURATION|_'
echo "Job Name   : $JOB"
echo "TD File    : $TD_FILE"
echo "BQ File    : $BQ_FILE"
echo "Delimiter  : $DELIM"
echo "Widths     : $WIDTHS"
echo "Timestamp  : $TS"

# ----------- FILE EXISTENCE -----------
ls -lh "$TD_PATH"
ls -lh "$BQ_PATH"

# ----------- READ HEADERS -------------
td_header=$(head -1 "$TD_PATH" | tr -d '\r\n')
bq_header=$(head -1 "$BQ_PATH" | tr -d '\r\n')

# --------- Universal delimiter handling ---------
# Converts all multi-char delimiters to TAB for processing.
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

# ----------- EXTRACT COLUMNS -----------
IFS=$'\n' read -d '' -r -a td_cols < <(echo "$td_header_fixed" | awk -F"$SPLIT_DELIM" '{for(i=1;i<=NF;i++)print $i}' ; printf '\0')
IFS=$'\n' read -d '' -r -a bq_cols < <(echo "$bq_header_fixed" | awk -F"$SPLIT_DELIM" '{for(i=1;i<=NF;i++)print $i}' ; printf '\0')

td_col_count=${#td_cols[@]}
bq_col_count=${#bq_cols[@]}

echo "TD header: $td_header"
echo "BQ header: $bq_header"
echo "TD col count: $td_col_count"
echo "BQ col count: $bq_col_count"

# ----------- ROW COUNTS ---------------
TD_ROWS=$(awk 'END{print NR-1}' "$TD_PATH")
BQ_ROWS=$(awk 'END{print NR-1}' "$BQ_PATH")

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

# ----------- HEADER/COL VALIDATION -----
HEADER_VAL="PASS"
[[ "$td_header" != "$bq_header" ]] && HEADER_VAL="FAIL"

COUNT_VAL="PASS"
[[ $TD_ROWS -ne $BQ_ROWS ]] && COUNT_VAL="FAIL"

# ----------- EXTENSION VALIDATION ------
td_ext="${FILENAME##*.}"
bq_ext="${BQ_FILE##*.}"
FILE_EXT_VAL="PASS"
[[ "$td_ext" != "$bq_ext" ]] && FILE_EXT_VAL="FAIL"

# ----------- FAST FILE CHECKSUM VALIDATION -----------
TD_FILE_CHK=$(cksum "$TD_PATH" | awk '{print $1}')
BQ_FILE_CHK=$(cksum "$BQ_PATH" | awk '{print $1}')
FAST_CHECKSUM_MATCHED=0
CHECKSUM_VAL="N/A"

if [[ "$TD_FILE_CHK" == "$BQ_FILE_CHK" ]]; then
    CHECKSUM_VAL="PASS"
    FAST_CHECKSUM_MATCHED=1
else
    CHECKSUM_VAL="FAIL"
fi

# ----------- COLUMN DATA COMPARISON (MISMATCH/PASS) -----------
passed_columns=()
mismatched_columns=()
if [[ $FAST_CHECKSUM_MATCHED -eq 0 && $td_col_count -eq $bq_col_count && $TD_ROWS -eq $BQ_ROWS && "$HEADER_VAL" == "PASS" ]]; then

    # If multi-char delimiter, preprocess whole files to TAB
    TD_TEMP_DATA="$TD_PATH"
    BQ_TEMP_DATA="$BQ_PATH"
    if [[ "${#DELIM}" -gt 1 ]]; then
      TD_TEMP_DATA="${LOG_DIR}/TD_TEMP_DATA.txt"
      BQ_TEMP_DATA="${LOG_DIR}/BQ_TEMP_DATA.txt"
      tail -n +2 "$TD_PATH" | sed "s/${DELIM}/$'\t'/g" > "$TD_TEMP_DATA"
      tail -n +2 "$BQ_PATH" | sed "s/${DELIM}/$'\t'/g" > "$BQ_TEMP_DATA"
    fi

    for ((col=1; col<=td_col_count; col++)); do
        td_col="${td_cols[$((col-1))]}"
        bq_col="${bq_cols[$((col-1))]}"
        # Compare this column (skip if header mismatch)
        if [[ "$td_col" != "$bq_col" ]]; then
            mismatched_columns+=("$td_col")
            continue
        fi
        diff_found=0
        td_out="${LOG_DIR}/${JOB}_${td_col}_mismatch_td.txt"
        bq_out="${LOG_DIR}/${JOB}_${td_col}_mismatch_bq.txt"
        : > "$td_out"; : > "$bq_out"
        for ((row=1; row<=TD_ROWS; row++)); do
            td_val=$(awk -F"$SPLIT_DELIM" -v r="$row" -v c="$col" 'NR==r{print $c}' "$TD_TEMP_DATA")
            bq_val=$(awk -F"$SPLIT_DELIM" -v r="$row" -v c="$col" 'NR==r{print $c}' "$BQ_TEMP_DATA")
            if [[ "$td_val" != "$bq_val" ]]; then
                diff_found=1
                echo "Row $((row)): $td_val" >> "$td_out"
                echo "Row $((row)): $bq_val" >> "$bq_out"
            fi
        done
        if [[ $diff_found -eq 0 ]]; then
            passed_columns+=("$td_col")
            rm -f "$td_out" "$bq_out"
        else
            mismatched_columns+=("$td_col")
        fi
    done
else
    # If fast checksum passed, consider all columns passed
    if [[ $FAST_CHECKSUM_MATCHED -eq 1 ]]; then
        passed_columns=("${td_cols[@]}")
        mismatched_columns=()
    fi
fi

PASSED_COLUMNS=$(IFS=,; echo "${passed_columns[*]}")
MISMATCHED_COLUMNS=$(IFS=,; echo "${mismatched_columns[*]}")
PASSED_COLUMNS=${PASSED_COLUMNS:-N/A}
MISMATCHED_COLUMNS=${MISMATCHED_COLUMNS:-N/A}

# ----------- STATUS AGGREGATE ----------
STATUS="PASS"
for s in "$HEADER_VAL" "$COUNT_VAL" "$FILE_EXT_VAL" "$CHECKSUM_VAL"; do
    [[ "$s" == "FAIL" ]] && STATUS="FAIL" && break
done

# ----------- HTML SUMMARY --------------
cat <<EOF > "$SUMMARY_HTML"
<html><body>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Job Name</th><td>$JOB</td></tr>
<tr><th>File Name</th><td>$FILENAME</td></tr>
<tr><th>TD Row Count | Column Count | Missing Columns</th><td>$TD_ROWS | $td_col_count | $td_missing</td></tr>
<tr><th>BQ Row Count | Column Count | Missing Columns</th><td>$BQ_ROWS | $bq_col_count | $bq_missing</td></tr>
<tr><th>Count Variance</th><td>$COUNT_VAR</td></tr>
<tr><th>Header Validation</th><td>$HEADER_VAL</td></tr>
<tr><th>Count Validation</th><td>$COUNT_VAL</td></tr>
<tr><th>File Extension Validation</th><td>$FILE_EXT_VAL</td></tr>
<tr><th>Checksum Validation</th><td>$CHECKSUM_VAL</td></tr>
<tr><th>Status</th><td>$STATUS</td></tr>
<tr><th>Passed Columns</th><td>$PASSED_COLUMNS</td></tr>
<tr><th>Mismatched Columns</th><td>$MISMATCHED_COLUMNS</td></tr>
</table>
</body></html>
EOF

echo "HTML Summary generated: $SUMMARY_HTML"
echo "========== Script complete =========="
echo "Job Name                                     : $JOB"
echo "File Name                                    : $FILENAME"
echo "TD Row Count | Column Count | Missing Columns: $TD_ROWS | $td_col_count | $td_missing"
echo "BQ Row Count | Column Count | Missing Columns: $BQ_ROWS | $bq_col_count | $bq_missing"
echo "Count Variance                               : $COUNT_VAR"
echo "Header Validation                            : $HEADER_VAL"
echo "Count Validation                             : $COUNT_VAL"
echo "File Extension Validation                    : $FILE_EXT_VAL"
echo "Checksum Validation                          : $CHECKSUM_VAL"
echo "Status                                       : $STATUS"
echo "Passed Columns                               : $PASSED_COLUMNS"
echo "Mismatched Columns                           : $MISMATCHED_COLUMNS"

exit 0