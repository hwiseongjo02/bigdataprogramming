# BRFSS 기반 심근경색 위험요인 빅데이터 분석

2026 명지대학교 빅데이터 프로그래밍 기말 프로젝트

## 1. 프로젝트 개요

본 프로젝트는 Kaggle에 공개된 CDC BRFSS(Behavioral Risk Factor Surveillance System) 데이터를 활용하여 심근경색 경험 여부와 주요 건강 위험요인 사이의 관계를 분석하는 빅데이터 처리 및 분석 프로젝트이다.

BRFSS 데이터는 미국 성인을 대상으로 수집된 대규모 건강 설문 데이터로, 연도별 CSV 파일의 누적 용량이 100MB를 넘기 때문에 빅데이터 처리 파이프라인을 설계하고 Spark 기반으로 분석하기에 적합하다.

## 2. 문제 정의

심근경색은 연령, 고혈압, 고콜레스테롤, 당뇨, 흡연, BMI 등 여러 요인의 영향을 받을 수 있다. 본 프로젝트에서는 BRFSS 데이터를 기반으로 다음 질문에 답하고자 한다.

1. 연령대가 높아질수록 심근경색 경험 비율은 어떻게 달라지는가?
2. 고혈압 진단 여부에 따라 심근경색 경험 비율은 차이가 있는가?
3. BMI 분포는 심근경색 경험자와 비경험자 사이에서 어떤 차이를 보이는가?
4. 고혈압, 고콜레스테롤, 당뇨, 흡연 위험요인이 많이 중첩될수록 심근경색 경험 비율은 증가하는가?
5. 조사 연도별 심근경색 경험 비율은 어떤 추이를 보이는가?

## 3. 데이터 출처

- 데이터셋: CDC Behavioral Risk Factor Surveillance System
- 출처: Kaggle Datasets
- Kaggle dataset id: `cdc/behavioral-risk-factor-surveillance-system`
- 데이터 형식: CSV
- 현재 수집 데이터: 2011년부터 2015년까지의 BRFSS 분할 데이터
- 로컬 수집 데이터 용량: 약 731MB

대용량 원본 데이터는 GitHub에 직접 커밋하지 않고, 수집 스크립트를 통해 재생성하는 것을 원칙으로 한다.

## 4. 기술 스택

- 데이터 수집: Python, Kaggle API, pandas
- 데이터 저장: 로컬 CSV, HDFS
- 데이터 전처리: PySpark DataFrame
- 데이터 분석: Spark SQL
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
    -> 분석 결과 CSV 및 그래프 이미지 저장
```

HDP Sandbox에서는 `--upload-hdfs` 옵션과 HDFS 경로를 사용하여 로컬 파일이 아닌 HDFS 기반으로 Spark 작업을 실행할 수 있다.

## 6. Repository 구조

```text
bigdataprogramming/
├── README.md
├── requirements.txt
├── run_pipeline.sh
├── data_collection.py       # Kaggle 데이터 다운로드, CSV 분할, HDFS 업로드 옵션
├── data_collectinon.py      # 기존 오타 파일명 호환용 래퍼
├── data_preprocessing.py    # Spark 기반 전처리 및 파생 변수 생성
├── data_visualization.py    # Spark SQL 분석 및 시각화
└── data/
    └── README.md            # 데이터 출처, 스키마, 저장 정책
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

### 7.2 패키지 설치

```bash
pip install -r requirements.txt
```

HDP Sandbox에서는 Spark가 이미 설치되어 있다면 PySpark 설치 단계는 생략할 수 있다.

### 7.3 데이터 수집 및 분할

```bash
python data_collection.py
```

실행 결과 `./data` 폴더에 `part_연도_번호.csv` 형식의 분할 CSV 파일이 생성된다. 기존 오타 파일명으로 실행해야 하는 환경에서는 다음 명령도 동작한다.

```bash
python data_collectinon.py
```

### 7.4 HDFS 적재 포함 수집

```bash
python data_collection.py --upload-hdfs --hdfs-raw-dir /user/$USER/brfss/raw
```

이미 로컬에 분할 CSV가 있고 HDFS 업로드만 다시 하고 싶다면 다음처럼 실행한다.

```bash
python data_collection.py --skip-download --upload-hdfs --hdfs-raw-dir /user/$USER/brfss/raw
```

### 7.5 데이터 전처리

로컬 CSV 기준:

```bash
spark-submit data_preprocessing.py --input './data/part_*.csv' --output './data/cleaned_cardio_data'
```

HDFS 기준:

```bash
spark-submit data_preprocessing.py --input '/user/$USER/brfss/raw/part_*.csv' --output '/user/$USER/brfss/cleaned_cardio_data'
```

전처리 결과에는 `Has_Disease`, `BMI_Real`, `Risk_Score`, `YEAR` 등의 분석용 컬럼이 포함된다.

### 7.6 분석 및 시각화

로컬 정제 데이터 기준:

```bash
spark-submit data_visualization.py --input './data/cleaned_cardio_data/*.csv' --output-dir './data/analysis_results'
```

HDFS 정제 데이터 기준:

```bash
spark-submit data_visualization.py --input '/user/$USER/brfss/cleaned_cardio_data/*.csv' --output-dir './data/analysis_results'
```

실행 결과 `data/analysis_results` 폴더에 분석 CSV와 그래프 PNG가 저장된다.

주요 결과 파일:

- `analysis_age_disease_rate.csv`, `plot1_age.png`
- `analysis_blood_pressure_disease_rate.csv`, `plot2_bp.png`
- `analysis_bmi_distribution_sample.csv`, `plot3_bmi.png`
- `analysis_risk_score_disease_rate.csv`, `plot4_risk_factors.png`
- `analysis_year_disease_rate.csv`, `plot5_year.png`

### 7.7 전체 파이프라인 자동 실행

로컬 기준:

```bash
bash run_pipeline.sh
```

HDFS 기준:

```bash
USE_HDFS=1 bash run_pipeline.sh
```

## 8. 현재 구현 상태

완료된 항목:

- Kaggle API 기반 데이터 다운로드 스크립트 작성
- 100MB 이상의 대용량 데이터 확보
- 연도별 CSV 파일 분할 저장 및 `YEAR` 컬럼 추가
- HDFS 업로드 옵션 추가
- Spark DataFrame 기반 주요 컬럼 선택 및 타입 변환
- 심근경색 여부, 실제 BMI, 복합 위험요인 점수 파생 변수 생성
- Spark SQL 기반 분석 결과 CSV 저장
- 연령대, 고혈압, BMI, 복합 위험요인, 연도별 시각화 생성
- Bash 기반 전체 파이프라인 자동화 스크립트 추가
- GitHub 제출용 샘플 데이터(`data/sample_brfss_1000.csv`) 생성

추가 보완 예정 항목:

- HDP Sandbox에서 전체 파이프라인 재실행 검증
- 발표 슬라이드 작성
- 최종 보고서 작성

## 9. 기대 결과

본 프로젝트를 통해 연령대와 주요 건강 위험요인이 심근경색 경험 비율에 어떤 영향을 주는지 정량적으로 확인한다. 특히 단일 위험요인보다 여러 위험요인이 동시에 존재할 때 심근경색 경험 비율이 어떻게 달라지는지 분석하여 예방 관점의 인사이트를 도출하는 것을 목표로 한다.

## 10. 참고 자료

- Kaggle Datasets: https://www.kaggle.com/datasets
- CDC BRFSS: https://www.cdc.gov/brfss/
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Matplotlib Documentation: https://matplotlib.org/
- Seaborn Documentation: https://seaborn.pydata.org/

## 11. AI Tool Usage

- ChatGPT/Codex: 과제 PDF 요구사항 정리, README 구조 점검, 코드 개선 방향 제안 및 디버깅 보조에 사용함.

