#!/usr/bin/env bash
# 에러 발생 시 스크립트 정지 
set -e 

USE_HDFS="${USE_HDFS:-0}"

# HDFS와 로컬에서 사용할 기본 경로
RAW_HDFS_DIR="/user/${USER}/brfss/raw"
HDFS_DIR="/user/${USER}/brfss/cleaned_cardio_data"

RAW_LOCAL_PATTERN="./data/part_*.csv"
LOCAL_DIR="./data/cleaned_cardio_data"
RESULT_DIR="./data/analysis_results"

echo " 빅데이터프로그래밍 기말 프로젝트 파이프라인 (60211701 조휘성)"

if [ "$USE_HDFS" = "1" ]; then
  echo "HDFS / Spark 서버로 실행"
  
  # 1. 데이터 수집 & HDFS 업로드
  echo ">>> 1. 데이터 수집하고 HDFS에 적재"
  python3 data_collection.py --upload-hdfs
  
  # 2. Spark 분산 전처리
  echo ">>> 2. Spark 데이터 정제"
  spark-submit data_preprocessing.py --input "${RAW_HDFS_DIR}/part_*.csv" --output "$HDFS_DIR"
  
  # 3. Spark 분석 & 시각화
  echo ">>> 3. Spark SQL 분석하고 시각화 도표 생성"
  spark-submit data_visualization.py --input "${HDFS_DIR}/*.csv" --output-dir "$RESULT_DIR"

else
  echo "로컬에서 테스트 실행"
  
  # 1. 수집
  echo ">>> 1단계: 데이터 수집"
  python data_collection.py
  
  # 2. 전처리
  echo ">>> 2단계: Spark 데이터 정제"
  spark-submit data_preprocessing.py --input "$RAW_LOCAL_PATTERN" --output "$LOCAL_DIR"
  
  # 3. 분석
  echo ">>> 3단계: Spark SQL 분석 및 시각화 생성"
  spark-submit data_visualization.py --input "${LOCAL_DIR}/*.csv" --output-dir "$RESULT_DIR"
fi

echo " 모든 파이프라인 실행 완료"
echo " ${RESULT_DIR} 폴더 확인"