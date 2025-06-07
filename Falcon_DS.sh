#!/bin/bash

#
# Falcon_DS.sh
#
# Usage:
#    Falcon_DS.sh JOB_NAME REL_PATH1 REL_PATH2 DELIMITER WIDTHS TIMESTAMP
#
#   - JOB_NAME:     e.g. "Multi_File_Validation"
#   - REL_PATH1:    e.g. "td_inbound/TD_FILE.txt"    (inside bucket vz-fstexp)
#   - REL_PATH2:    e.g. "bq_inbound/BQ_FILE.txt"    (inside bucket vz-outbound)
#   - DELIMITER:    e.g. "|", "!|", ",", "\t", or " "     (no quotes)
#   - WIDTHS:       (currently unused; pass empty string "")
#   - TIMESTAMP:    e.g. "20250607_003000"
#
# Buckets are mounted by GCSFuse at:
#   /mnt/bucket_td   → vz-fstexp
#   /mnt/bucket_bq   → vz-outbound
#

# Read positional arguments
JOB_NAME="$1"
REL_PATH1="$2"
REL_PATH2="$3"
DELIMITER="$4"
WIDTHS="$5"
TIMESTAMP="$6"

# Build absolute paths
FILE1_PATH="/mnt/bucket_td/${REL_PATH1}"
FILE2_PATH="/mnt/bucket_bq/${REL_PATH2}"

# Output directory under vz-outbound/logs
BASE_OUTPUT_DIR="/mnt/bucket_bq/logs/${JOB_NAME}_${TIMESTAMP}"
mkdir -p "$BASE_OUTPUT_DIR"

# All stdout/stderr → output log
OUTPUT_LOG="$BASE_OUTPUT_DIR/${JOB_NAME}_output.log"
exec > >(tee -a "$OUTPUT_LOG") 2>&1

echo "----------------------------------------"
echo "Starting comparison at $(date '+%Y-%m-%d %H:%M:%S')"
echo "JOB_NAME       : $JOB_NAME"
echo "FILE1_PATH     : $FILE1_PATH"
echo "FILE2_PATH     : $FILE2_PATH"
echo "DELIMITER      : '$DELIMITER'"
echo "WIDTHS (unused): '$WIDTHS'"
echo "TIMESTAMP      : $TIMESTAMP"
echo "Output Dir     : $BASE_OUTPUT_DIR"
echo "----------------------------------------"

# 1) Verify both files exist
if [ ! -f "$FILE1_PATH" ] || [ ! -f "$FILE2_PATH" ]; then
  echo "ERROR: One or both input files do not exist."
  echo "       $FILE1_PATH"
  echo "       $FILE2_PATH"
  exit 1
fi

# 2) Extract headers (first line)
TD_HEADER=$(head -n 1 "$FILE1_PATH")
BQ_HEADER=$(head -n 1 "$FILE2_PATH")

# 3) Split headers into arrays by the raw DELIMITER
IFS="$DELIMITER" read -ra TD_COLUMNS <<< "$TD_HEADER"
IFS="$DELIMITER" read -ra BQ_COLUMNS <<< "$BQ_HEADER"

TD_ROW_COUNT=$(tail -n +2 "$FILE1_PATH" | wc -l | awk '{print $1}')
BQ_ROW_COUNT=$(tail -n +2 "$FILE2_PATH" | wc -l | awk '{print $1}')

TD_COL_COUNT=${#TD_COLUMNS[@]}
BQ_COL_COUNT=${#BQ_COLUMNS[@]}

echo "TD Rows: $TD_ROW_COUNT | TD Cols: $TD_COL_COUNT"
echo "BQ Rows: $BQ_ROW_COUNT | BQ Cols: $BQ_COL_COUNT"

# 4) Find missing columns (alphabetically sorted compare)
printf "%s\n" "${TD_COLUMNS[@]}" | sort > /tmp/td_cols_sorted.txt
printf "%s\n" "${BQ_COLUMNS[@]}" | sort > /tmp/bq_cols_sorted.txt

# Columns in TD but not in BQ
TD_MISSING=$(comm -23 /tmp/td_cols_sorted.txt /tmp/bq_cols_sorted.txt | paste -sd "," -)
# Columns in BQ but not in TD
BQ_MISSING=$(comm -13 /tmp/td_cols_sorted.txt /tmp/bq_cols_sorted.txt | paste -sd "," -)

[ -z "$TD_MISSING" ] && TD_MISSING="None"
[ -z "$BQ_MISSING" ] && BQ_MISSING="None"

# 5) Header validation
HEADER_VALIDATION="PASS"
if [ "$TD_HEADER" != "$BQ_HEADER" ]; then
  HEADER_VALIDATION="FAIL"
fi

# 6) Count validation
COUNT_VALIDATION="PASS"
if [ "$TD_ROW_COUNT" -ne "$BQ_ROW_COUNT" ] || [ "$TD_COL_COUNT" -ne "$BQ_COL_COUNT" ]; then
  COUNT_VALIDATION="FAIL"
fi

# 7) Checksum validation (MD5)
TD_CHECKSUM=$(md5sum "$FILE1_PATH" | awk '{print $1}')
BQ_CHECKSUM=$(md5sum "$FILE2_PATH" | awk '{print $1}')
CHECKSUM_VALIDATION="PASS"
if [ "$TD_CHECKSUM" != "$BQ_CHECKSUM" ]; then
  CHECKSUM_VALIDATION="FAIL"
fi

# 8) Extension validation
TD_EXT="${FILE1_PATH##*.}"
BQ_EXT="${FILE2_PATH##*.}"
EXT_VALIDATION="PASS"
if [ "$TD_EXT" != "$BQ_EXT" ]; then
  EXT_VALIDATION="FAIL"
fi

# 9) Count variance (relative to BQ)
if [ "$BQ_ROW_COUNT" -ne 0 ]; then
  COUNT_VARIANCE=$(awk -v t="$TD_ROW_COUNT" -v b="$BQ_ROW_COUNT" 'BEGIN { printf "%.2f%%", ((t - b)/b)*100 }')
else
  COUNT_VARIANCE="NA"
fi

# 10) Final status
STATUS="PASS"
if [[ "$HEADER_VALIDATION" != "PASS" ]] \
  || [[ "$COUNT_VALIDATION" != "PASS" ]] \
  || [[ "$CHECKSUM_VALIDATION" != "PASS" ]]; then
  STATUS="FAIL"
fi

# 11) Passed columns (intersection)
PASSED_COLS=$(comm -12 /tmp/td_cols_sorted.txt /tmp/bq_cols_sorted.txt | paste -sd "," -)
[ -z "$PASSED_COLS" ] && PASSED_COLS="None"

# 12) Mismatched columns (for now “None”)
MISMATCHED_COLS="None"

# 13) Generate HTML summary
SUMMARY_HTML="$BASE_OUTPUT_DIR/${JOB_NAME}_summary.html"

cat <<EOF > "$SUMMARY_HTML"
<html>
  <body>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th>Job Name</th><td>$JOB_NAME</td></tr>
      <tr><th>File Name</th><td>$(basename "$FILE1_PATH")</td></tr>
      <tr><th>TD Row Count | Column Count | Missing Columns</th>
          <td>$TD_ROW_COUNT | $TD_COL_COUNT | $TD_MISSING</td></tr>
      <tr><th>BQ Row Count | Column Count | Missing Columns</th>
          <td>$BQ_ROW_COUNT | $BQ_COL_COUNT | $BQ_MISSING</td></tr>
      <tr><th>Count Variance</th><td>$COUNT_VARIANCE</td></tr>
      <tr><th>Header Validation</th><td>$HEADER_VALIDATION</td></tr>
      <tr><th>Count Validation</th><td>$COUNT_VALIDATION</td></tr>
      <tr><th>File Extension Validation</th><td>$EXT_VALIDATION</td></tr>
      <tr><th>Checksum Validation</th><td>$CHECKSUM_VALIDATION</td></tr>
      <tr><th>Status</th><td>$STATUS</td></tr>
      <tr><th>Passed Columns</th><td>$PASSED_COLS</td></tr>
      <tr><th>Mismatched Columns</th><td>$MISMATCHED_COLS</td></tr>
    </table>
  </body>
</html>
EOF

echo "HTML Summary generated: $SUMMARY_HTML"
exit 0