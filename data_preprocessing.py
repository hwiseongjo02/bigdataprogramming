import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, when


#로컬 테스트 용
INPUT_PATH = "./data/part_*.csv"
OUTPUT_PATH = "./data/cleaned_cardio_data"

# 빅데프 기말 프로젝트 분석에 핵심이 될 컬럼들
SELECTED_COLS = [
    "YEAR",
    "CVDINFR4",  # 심근경색 경험이 있는가? => 1=Yes, 2=No
    "BPHIGH4",   # 고혈압을 진단받은 적이 있는가? => 1=Yes, 3=No
    "TOLDHI2",   # 고콜레스테롤인가? => 1=Yes, 2=No
    "DIABETE3",  # 당뇨 진단을 받은 적이 있는가? => 1=Yes, 3=No
    "SMOKE100",  # 흡연자인가? => 1=Yes, 2=No
    "SEX",
    "_AGEG5YR",
    "_BMI5",
]

# 설문조사 결과를 단순하게 있나, 없나로 구분
def convert_to_binary(column_name, yes_val, no_val):
    return when(col(column_name) == yes_val, 1).when(col(column_name) == no_val, 0)

def main():
    parser = argparse.ArgumentParser(description="BRFSS 데이터 전처리 스크립트")
    parser.add_argument("--input", default=INPUT_PATH, help="원본 CSV 파일 경로")
    parser.add_argument("--output", default=OUTPUT_PATH, help="정제된 데이터 저장 경로")
    args = parser.parse_args()

    input_path = args.input[0] if isinstance(args.input, list) else args.input

    print("Spark 데이터 전처리 시작")

    spark = SparkSession.builder \
        .appName("BRFSS_Data_Preprocessing") \
        .getOrCreate()

    print(f"1. 원본 데이터 불러오는 중")
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # 필요한 컬럼 빼고 나머지 전부 정수형 변환
    df_selected = df_raw.select([col(c) for c in SELECTED_COLS])
    for c in SELECTED_COLS:
        df_selected = df_selected.withColumn(c, col(c).cast("int"))

    # 데이터 정제
    print("2. 결측치 및 타겟 변수 정제 진행")
    
    # 심근경색(CVDINFR4) 응답이 아예 없는(NULL) 데이터 삭제
    df_clean = df_selected.dropna(subset=["CVDINFR4"])
    
    # 모름/응답거부(7, 9) 값 제외하고 확실히 응답(1, 2)만 필터링
    df_clean = df_clean.filter(col("CVDINFR4").isin([1, 2]))

    # 파생 변수 1: 타겟 변수 (심근경색 걸렸으면 1, 아니면 0)
    df_clean = df_clean.withColumn("Has_Disease", when(col("CVDINFR4") == 1, 1).otherwise(0))
    
    # 파생 변수 2: BMI 수치
    df_clean = df_clean.withColumn("BMI_Real", col("_BMI5") / 100)

    # 파생 변수 3: 위험 요인들 이진화 작업 (고혈압, 콜레스테롤, 당뇨, 흡연)
    df_clean = df_clean.withColumn("BP_Risk", convert_to_binary("BPHIGH4", 1, 3))
    df_clean = df_clean.withColumn("Cholesterol_Risk", convert_to_binary("TOLDHI2", 1, 2))
    df_clean = df_clean.withColumn("Diabetes_Risk", convert_to_binary("DIABETE3", 1, 3))
    df_clean = df_clean.withColumn("Smoking_Risk", convert_to_binary("SMOKE100", 1, 2))

    # 파생 변수 4: 복합 위험 점수 (0 ~ 4점)
    # 위험 요인이 몇 개나 겹치는지 확인
    df_clean = df_clean.withColumn(
        "Risk_Score",
        col("BP_Risk") + col("Cholesterol_Risk") + col("Diabetes_Risk") + col("Smoking_Risk")
    )

    print(f"정제 완료. 최종 데이터 건수: {df_clean.count()}건")
    df_clean.show(5)

    # HDFS 또는 로컬에 저장
    print(f"3. 정제된 데이터 저장")
    df_clean.write.mode("overwrite").csv(args.output, header=True)
    
    spark.stop()
    print("데이터 전처리 완료")

if __name__ == "__main__":
    main()