# BRFSS 기반 심근경색 위험요인 빅데이터 분석

2026 명지대학교 빅데이터프로그래밍 학기말 프로젝트
60211701 조휘성

## 1. 프로젝트 개요

본 프로젝트는 Kaggle에 공개된 CDC BRFSS(Behavioral Risk Factor Surveillance System) 데이터를 활용하여 심근경색 경험 여부와 주요 건강 위험요인 사이의 관계를 분석하는 빅데이터 처리 및 분석 프로젝트이다.

BRFSS 데이터는 미국 성인을 대상으로 수집된 대규모 건강 설문 데이터로, 연도별 CSV 파일의 누적 용량이 100MB를 넘고 다양한 위험 요소 컬럼을 가지고 있기 떄문에
본 프로젝트의 주제로 선정하게 되었다.

## 2. 문제 정의

심근경색은 연령, 고혈압, 고콜레스테롤, 당뇨, 흡연, BMI 등 여러 요인의 영향을 받을 수 있다. 본 프로젝트에서는 BRFSS 데이터를 기반으로 다음 질문에 답하고자 한다.

1. 연령대가 높아질수록 심근경색 경험 비율은 어떻게 달라지는가?
2. 고혈압 진단 여부에 따라 심근경색 경험 비율은 차이가 있는가?
3. BMI 분포는 심근경색 경험자와 비경험자 사이에서 어떤 차이를 보이는가?
4. 고혈압, 고콜레스테롤, 당뇨, 흡연 위험요인이 많이 중첩될수록 심근경색 경험 비율은 증가하는가?
5. 조사 연도별 심근경색 경험 비율은 어떤 추이를 보이는가?
6. 여러 위험요인을 동시에 고려했을 때 심근경색 경험 여부에 상대적으로 크게 작용하는 변수는 무엇인가?

## 3. 데이터 출처

- 데이터셋: CDC Behavioral Risk Factor Surveillance System
- 출처: Kaggle Datasets
- Kaggle dataset id: `cdc/behavioral-risk-factor-surveillance-system`
- 데이터 형식: CSV
- 현재 수집 데이터: 2011년부터 2015년까지의 BRFSS 분할 데이터
- 로컬 수집 데이터 용량: 약 731MB

## 4. 기술 스택

- 데이터 수집: Python, Kaggle API, pandas
- 데이터 저장: 로컬 CSV, HDFS
- 데이터 전처리: PySpark DataFrame
- 데이터 분석: Spark SQL, Spark MLlib(Logistic Regression)
- 시각화: Matplotlib, Seaborn
- 자동화: Bash script(`run_pipeline.sh`)
- 버전 관리: GitHub

## 5. 데이터 파이프라인

```text
Kaggle Dataset
    -> Python 수집 스크립트(data_collection.py)
    -> 연도별 CSV 다운로드
    -> 대용량 CSV 분할 및 YEAR 컬럼 추가
    -> 선택 옵션: HDFS 적재
    -> Spark DataFrame 로드
    -> 컬럼 선택 / 타입 변환 / 파생 변수 생성
    -> Spark SQL 분석
    -> Spark ML 기반 다변량 로지스틱 회귀 분석
    -> 분석 결과 CSV 및 그래프 이미지 저장
```

HDP Sandbox에서는 `--upload-hdfs` 옵션과 HDFS 경로를 사용하여 로컬 파일이 아닌 HDFS 기반으로 Spark 작업을 실행할 수 있다.

## 6. Repository 구조

```text
bigdataprogramming/
├── README.md
├── requirements.txt
├── run_pipeline.sh
├── brfss_config.py          # 데이터셋 경로, 컬럼 목록, 모델 설정
├── data_collection.py       # Kaggle 데이터 다운로드, CSV 분할, HDFS 업로드 옵션
├── data_preprocessing.py    # Spark 기반 전처리 및 파생 변수 생성
├── data_visualization.py    # 분석 실행 흐름 관리
├── analysis_utils.py        # 시각화, SQL 저장, Spark ML 보조 함수
└── data/
    ├── README.md            # 데이터 출처, 스키마, 저장 정책
    └── sample_brfss_1000.csv # GitHub 확인용 소규모 샘플
```

## 7. 실행 방법

### 7.1 Kaggle API 설정

Kaggle API를 사용하려면 Kaggle 계정의 `kaggle.json` 인증 파일이 필요하다.

Windows 예시:

```powershell
mkdir $env:USERPROFILE\.kaggle
copy kaggle.json $env:USERPROFILE\.kaggle\kaggle.json
```

Linux / HDP Sandbox 예시:

```bash
mkdir -p ~/.kaggle
cp kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

 아래 명령어는 이미 인증이 완료된 상태라면 생략

```bash
ls -l ~/.kaggle/kaggle.json
```

### 7.2 패키지 설치

로컬 Python 3 환경:

```bash
pip install -r requirements.txt
```

HDP Sandbox에서 Python 3.6을 직접 사용한다.

```bash
python --version
python3.6 --version
python3.6 -m pip install --user -r requirements.txt
export PATH="$HOME/.local/bin:$PATH"
```

Spark 작업에서 사용할 Python도 Python 3.6으로 고정한다.

```bash
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON=/usr/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
```

### 7.3 데이터 수집 및 분할

로컬 기준:

```bash
python data_collection.py --max-chunks 0
```

HDP Sandbox에서 Python 3.6을 직접 사용할 경우:

```bash
python3.6 data_collection.py --max-chunks 0
```

### 7.4 HDFS 적재 포함 수집

```bash
python3.6 data_collection.py \
  --max-chunks 0 \
  --upload-hdfs \
  --hdfs-raw-dir /user/$USER/brfss/raw
```

업로드 확인:

```bash
hdfs dfs -ls /user/$USER/brfss/raw | head
hdfs dfs -du -h /user/$USER/brfss/raw
```

```bash
python3.6 data_collection.py \
  --skip-download \
  --upload-hdfs \
  --hdfs-raw-dir /user/$USER/brfss/raw
```

### 7.5 데이터 전처리

로컬 CSV 기준:

```bash
spark-submit data_preprocessing.py \
  --input './data/part_*.csv' \
  --output './data/cleaned_cardio_data'
```

HDFS 기준에서는 Spark가 HDFS 경로를 정확히 인식하도록 `fs.defaultFS` 값을 붙여 사용한다.

```bash
HDFS_URI=$(hdfs getconf -confKey fs.defaultFS)

spark-submit data_preprocessing.py \
  --input "$HDFS_URI/user/$USER/brfss/raw/part_*.csv" \
  --output "$HDFS_URI/user/$USER/brfss/cleaned_cardio_data"
```

전처리 결과 확인:

```bash
hdfs dfs -ls /user/$USER/brfss/cleaned_cardio_data
```

`_SUCCESS`와 `part-...csv` 파일이 생성되면 성공이다. 전처리 결과에는 `Has_Disease`, `BMI_Real`, `Risk_Score`, `YEAR` 등의 분석용 컬럼이 포함된다.

### 7.6 분석 및 시각화

로컬 정제 데이터 기준:

```bash
spark-submit data_visualization.py \
  --input './data/cleaned_cardio_data/*.csv' \
  --output-dir './data/analysis_results'
```

HDFS 정제 데이터 기준:

```bash
HDFS_URI=$(hdfs getconf -confKey fs.defaultFS)

spark-submit data_visualization.py \
  --input "$HDFS_URI/user/$USER/brfss/cleaned_cardio_data/*.csv" \
  --output-dir './data/analysis_results'
```

실행 결과 `data/analysis_results` 폴더에 분석 CSV와 그래프 PNG가 저장된다.

주요 결과 파일:

- `analysis_age_disease_rate.csv`, `plot1_age.png`
- `analysis_blood_pressure_disease_rate.csv`, `plot2_blood_pressure.png`
- `analysis_risk_score_disease_rate.csv`, `plot3_risk_score.png`
- `analysis_year_disease_rate.csv`, `plot4_year.png`
- `analysis_bmi_distribution_sample.csv`, `plot5_bmi_distribution.png`
- `analysis_age_risk_matrix.csv`, `plot6_age_risk_heatmap.png`
- `analysis_model_metrics.csv`, `analysis_model_coefficients.csv`, `plot7_model_coefficients.png`

### 7.7 전체 파이프라인 자동 실행

`run_pipeline.sh`는 Python 3.6 이상 실행 파일을 자동으로 찾고, Spark Python 환경변수와 UTF-8 출력 설정을 함께 적용한다.

로컬 기준:

```bash
bash run_pipeline.sh
```

HDP Sandbox / HDFS 기준:

```bash
USE_HDFS=1 bash run_pipeline.sh
```


```bash
USE_HDFS=1 PYTHON_BIN=/usr/bin/python3.6 bash run_pipeline.sh
```

## 8. HDP Sandbox 문제 해결

### `SyntaxError: invalid syntax`가 f-string 줄에서 발생하는 경우

`spark-submit` 또는 `python` 명령이 Python 2.7을 사용하고 있을 가능성이 높다.

```bash
python --version
python3.6 --version
export PYSPARK_PYTHON=/usr/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
```

직접 실행할 때는 `python` 대신 `python3.6`을 사용한다.

### `UnicodeEncodeError: 'ascii' codec can't encode characters`가 발생하는 경우

샌드박스 터미널 출력 인코딩이 ASCII로 잡힌 경우이다.

```bash
export PYTHONIOENCODING=utf-8
```

### `Path does not exist: file:/user/...`가 발생하는 경우

Spark가 HDFS 경로를 로컬 파일 경로로 해석한 것이다. HDFS 기본 주소를 붙여 실행한다.

```bash
HDFS_URI=$(hdfs getconf -confKey fs.defaultFS)
echo $HDFS_URI
```

### `Incomplete HDFS URI, no host`가 발생하는 경우

`hdfs:///user/...`처럼 NameNode 주소가 빠진 경우이다. `hdfs getconf -confKey fs.defaultFS` 결과를 앞에 붙여야 한다.

```bash
spark-submit data_preprocessing.py \
  --input "$HDFS_URI/user/$USER/brfss/raw/part_*.csv" \
  --output "$HDFS_URI/user/$USER/brfss/cleaned_cardio_data"
```

## 9. 현재 구현 상태

완료된 항목:

- Kaggle API 기반 데이터 다운로드 스크립트 작성
- 100MB 이상의 대용량 데이터 확보
- 연도별 CSV 파일 분할 저장 및 `YEAR` 컬럼 추가
- HDFS 업로드 옵션 추가
- Spark DataFrame 기반 주요 컬럼 선택 및 타입 변환
- 심근경색 여부, 실제 BMI, 복합 위험요인 점수 파생 변수 생성
- Spark SQL 기반 분석 결과 CSV 저장
- 연령대와 복합 위험요인을 함께 고려한 교차 분석 히트맵 생성
- Spark ML 로지스틱 회귀 모델을 활용한 다변량 영향요인 분석 추가
- 연령대, 고혈압, BMI, 복합 위험요인, 연도별 시각화 생성
- Bash 기반 전체 파이프라인 자동화 스크립트 추가
- GitHub 제출용 샘플 데이터(`data/sample_brfss_1000.csv`) 생성
- HDP Sandbox에서 HDFS 업로드, 전처리, 분석 및 시각화 실행 검증


## 10. 기대 결과

본 프로젝트를 통해 연령대와 주요 건강 위험요인이 심근경색 경험 비율에 어떤 영향을 주는지 정량적으로 확인한다. 특히 단일 위험요인보다 여러 위험요인이 동시에 존재할 때 심근경색 경험 비율이 어떻게 달라지는지 분석하여 예방 관점의 인사이트를 도출하는 것을 목표로 한다.

## 11. 참고 자료

- 데이터셋 및 원천 자료
  - Kaggle BRFSS Dataset: https://www.kaggle.com/datasets/cdc/behavioral-risk-factor-surveillance-system
  - CDC BRFSS 공식 페이지: https://www.cdc.gov/brfss/
  - CDC BRFSS Survey Data & Documentation: https://www.cdc.gov/brfss/data_documentation/index.htm
  - CDC BRFSS Annual Survey Data: https://www.cdc.gov/brfss/annual_data/annual_data.htm
  - CDC 2011 BRFSS Survey Data and Documentation: https://www.cdc.gov/brfss/annual_data/annual_2011.htm
  - CDC 2011 BRFSS Methodologic Changes: https://www.cdc.gov/brfss/annual_data/2011/methodology2011.html

- 데이터 수집 및 저장
  - Kaggle API 공식 문서: https://github.com/Kaggle/kaggle-api
  - Apache Hadoop HDFS Commands Guide: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html

- 빅데이터 처리 및 분석
  - Apache Spark Documentation: https://spark.apache.org/docs/latest/
  - Spark SQL, DataFrames and Datasets Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
  - MLlib: Main Guide: https://spark.apache.org/docs/latest/ml-guide.html
  - PySpark MLlib API Reference: https://spark.apache.org/docs/latest/api/python/reference/pyspark.ml.html

- Python 데이터 분석 및 시각화
  - pandas Documentation: https://pandas.pydata.org/docs/
  - Matplotlib Documentation: https://matplotlib.org/stable/
  - Seaborn Documentation: https://seaborn.pydata.org/

## 12. AI Tool Usage

- ChatGPT/Codex: 참고 자료 탐색, Sandbox 환경에서 발생한 파이썬 버전 관련 오류 디버깅, 프로젝트 파이프라인 최적화, 시각화 자료 가독성 향상 방안 제시, README 구조 점검