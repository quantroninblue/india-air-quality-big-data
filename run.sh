#!/usr/bin/env bash
# Run pipeline steps with the project venv (clears system PYTHONPATH, e.g. ROS).
#   ./run.sh              -> full pipeline on HDFS + PDF export
#   ./run.sh clean aqi    -> just those steps (python steps live in 04_Code/<step>.py)
# Requires the Hadoop cluster to be running: 04_Code/hadoop/start_cluster.sh
# Set CPCB_STORAGE=local to run without HDFS.
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python; [ -x "$PY" ] || PY=../.venv/bin/python
steps=("$@")
if [ ${#steps[@]} -eq 0 ]; then
  steps=(ingest clean aqi analysis rdd_analysis mapreduce hive_tables dataset_profile export_cleaned hdfs_report
         visualize build_phase_docs build_report build_slides
         make_notebook)
  export_pdf=1
fi
for step in "${steps[@]}"; do
  echo "=== $step"
  case "$step" in
    mapreduce)      bash 04_Code/hadoop/run_streaming_job.sh ;;
    export_cleaned) bash 04_Code/hadoop/export_cleaned.sh ;;
    hdfs_report)    bash 04_Code/hadoop/hdfs_report.sh ;;
    *)              if [ -f "04_Code/$step.py" ]; then env -u PYTHONPATH "$PY" "04_Code/$step.py"
                    else echo "skipped: 04_Code/$step.py not present"; fi ;;
  esac
done
if [ "${export_pdf:-0}" = 1 ]; then
  for f in 01_Proposal/*.docx 07_Report/*.docx 08_Presentation/*.pptx 09_Contribution_Log/*.docx; do
    (cd "$(dirname "$f")" && libreoffice --headless --convert-to pdf "$(basename "$f")" >/dev/null)
  done
  echo "[pdf] exported"
fi
