#!/usr/bin/env bash
# BRFSS 전체 파이프라인 실행 스크립트
# 로컬 실행:        bash run_pipeline.sh
# HDP Sandbox 실행: USE_HDFS=1 PYTHON_BIN=/usr/bin/python3.6 bash run_pipeline.sh
# 빠른 테스트:      MAX_CHUNKS=5 bash run_pipeline.sh

# 중간에 명령어가 실패하면 바로 스크립트를 종료한다.
set -e

# USE_HDFS=1이면 원본/정제 데이터를 HDFS 기준으로 읽고 쓴다.
USE_HDFS="${USE_HDFS:-0}"

# MAX_CHUNKS=0은 모든 청크를 사용한다.
MAX_CHUNKS="${MAX_CHUNKS:-0}"

# 분석 결과 CSV와 그래프 이미지가 저장되는 폴더이다.
RESULT_DIR="./data/analysis_results"

find_python() {
  # 사용자가 PYTHON_BIN을 지정했다면 먼저 사용하고, 없으면 흔한 Python 명령어를 순서대로 찾는다.
  for cmd in "${PYTHON_BIN:-}" python3 python3.6 python36 python; do
    [ -n "$cmd" ] || continue

    # 현재 환경에 없는 명령어는 건너뛴다.
    if command -v "$cmd" >/dev/null 2>&1; then
      # 코드에서 f-string을 사용하므로 Python 3.6 이상이 필요하다.
      if "$cmd" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 6) else 1)
PY
      then
        command -v "$cmd"
        return
      fi
    fi
  done

  echo "ERROR: Python 3.6 or newer is required." >&2
  echo "In HDP Sandbox, try: PYTHON_BIN=/usr/bin/python3.6 bash run_pipeline.sh" >&2
  exit 1
}

# 사용할 Python을 정하고, Spark 작업도 같은 Python을 사용하도록 맞춘다.
PYTHON_CMD="$(find_python)"
export PYTHONIOENCODING="${PYTHONIOENCODING:-utf-8}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PYTHON_CMD}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYTHON_CMD}"

# 분석 결과를 저장할 폴더를 미리 만든다.
mkdir -p "$RESULT_DIR"

echo "BRFSS pipeline"
echo "Python: $PYTHON_CMD"
echo "Max chunks: $MAX_CHUNKS"

if [ "$USE_HDFS" = "1" ]; then
  # Hadoop 설정에서 HDFS NameNode 주소를 읽어온다.
  HDFS_URI="$(hdfs getconf -confKey fs.defaultFS)"
  HDFS_URI="${HDFS_URI%/}"

  # 데이터 수집과 전처리에서 사용할 HDFS 폴더이다.
  RAW_DIR="/user/$USER/brfss/raw"
  CLEAN_DIR="/user/$USER/brfss/cleaned_cardio_data"

  echo "Mode: HDFS ($HDFS_URI)"

  echo ">>> 1. Collect data and upload to HDFS"
  "$PYTHON_CMD" data_collection.py \
    --max-chunks "$MAX_CHUNKS" \
    --upload-hdfs \
    --hdfs-raw-dir "$RAW_DIR"

  echo ">>> 2. Preprocess with Spark"
  spark-submit data_preprocessing.py \
    --input "${HDFS_URI}${RAW_DIR}/part_*.csv" \
    --output "${HDFS_URI}${CLEAN_DIR}"

  echo ">>> 3. Analyze and visualize"
  spark-submit data_visualization.py \
    --input "${HDFS_URI}${CLEAN_DIR}/*.csv" \
    --output-dir "$RESULT_DIR"
else
  echo "Mode: local"

  echo ">>> 1. Collect data"
  "$PYTHON_CMD" data_collection.py --max-chunks "$MAX_CHUNKS"

  echo ">>> 2. Preprocess with Spark"
  spark-submit data_preprocessing.py \
    --input "./data/part_*.csv" \
    --output "./data/cleaned_cardio_data"

  echo ">>> 3. Analyze and visualize"
  spark-submit data_visualization.py \
    --input "./data/cleaned_cardio_data/*.csv" \
    --output-dir "$RESULT_DIR"
fi

echo "Done. Results: $RESULT_DIR"