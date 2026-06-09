import argparse
import glob
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, lit, regexp_extract, when

from brfss_config import DATA_DIR, SELECTED_COLUMNS


INPUT_PATH = f"{DATA_DIR}/part_*.csv"
OUTPUT_PATH = f"{DATA_DIR}/cleaned_cardio_data"

RISK_COLUMN_RULES = {
    "BP_Risk": ("BPHIGH4", 1, 3),
    "Cholesterol_Risk": ("TOLDHI2", 1, 2),
    "Diabetes_Risk": ("DIABETE3", 1, 3),
    "Smoking_Risk": ("SMOKE100", 1, 2),
}

RISK_COLUMNS = list(RISK_COLUMN_RULES)


def parse_args():
    parser = argparse.ArgumentParser(description="BRFSS 데이터 전처리 스크립트")
    parser.add_argument("--input", nargs="+", default=[INPUT_PATH], help="원본 CSV 파일 경로")
    parser.add_argument("--output", default=OUTPUT_PATH, help="정제된 데이터 저장 경로")
    return parser.parse_args()


def normalize_input_path(input_path):
    if isinstance(input_path, list):
        return input_path[0] if len(input_path) == 1 else input_path
    return input_path


def list_input_files(spark, input_path):
    input_path = normalize_input_path(input_path)
    if isinstance(input_path, list):
        return input_path

    if any(mark in input_path for mark in ["*", "?", "["]):
        local_matches = sorted(glob.glob(input_path))
        if local_matches:
            return local_matches

        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(input_path)
        statuses = path_obj.getFileSystem(hadoop_conf).globStatus(path_obj)
        if statuses:
            return [str(status.getPath()) for status in statuses]

    return [input_path]


def convert_to_binary(column_name, yes_val, no_val):
    return when(col(column_name) == yes_val, 1).when(col(column_name) == no_val, 0)


def add_missing_columns(df):
    for column_name in SELECTED_COLUMNS:
        if column_name not in df.columns:
            df = df.withColumn(column_name, lit(None))
    return df


def read_one_file(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.withColumn(
        "YEAR",
        regexp_extract(input_file_name(), r"part_(\d{4})_", 1).cast("int"),
    )

    df = add_missing_columns(df)
    return df.select([col(column_name) for column_name in SELECTED_COLUMNS])


def cast_columns(df):
    for column_name in SELECTED_COLUMNS:
        if column_name == "_BMI5":
            df = df.withColumn(column_name, col(column_name).cast("float"))
        else:
            df = df.withColumn(column_name, col(column_name).cast("int"))
    return df


def add_analysis_columns(df):
    df = df.withColumn("Has_Disease", when(col("CVDINFR4") == 1, 1).otherwise(0))
    df = df.withColumn("BMI_Real", col("_BMI5") / 100)

    for output_col, (source_col, yes_val, no_val) in RISK_COLUMN_RULES.items():
        df = df.withColumn(output_col, convert_to_binary(source_col, yes_val, no_val))

    risk_score = reduce(lambda left, right: left + right, [col(name) for name in RISK_COLUMNS])
    return df.withColumn("Risk_Score", risk_score)


def read_input_data(spark, input_paths):
    input_files = list_input_files(spark, input_paths)
    print("읽을 파일 개수:", len(input_files))
    if not input_files:
        raise ValueError("입력 CSV 파일을 찾지 못했습니다.")

    raw_dfs = [read_one_file(spark, file_path) for file_path in input_files]
    return reduce(lambda left, right: left.unionByName(right), raw_dfs)


def clean_disease_rows(df):
    return df.dropna(subset=["CVDINFR4"]).filter(col("CVDINFR4").isin([1, 2]))


def main():
    args = parse_args()
    print("Spark 데이터 전처리 시작")

    spark = SparkSession.builder.appName("BRFSS_Data_Preprocessing").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("1. 원본 데이터 불러오는 중")
    df_raw = cast_columns(read_input_data(spark, args.input))

    print("2. 결측치 및 타겟 변수 정제 진행")
    df_clean = add_analysis_columns(clean_disease_rows(df_raw))

    print("연도별 정제 데이터 건수")
    df_clean.groupBy("YEAR").count().orderBy("YEAR").show()

    print("데이터 정제 완료")
    print("정제 데이터 건수:", df_clean.count())
    df_clean.show(5)

    print("3. 정제된 데이터 저장")
    df_clean.coalesce(1).write.mode("overwrite").csv(args.output, header=True)

    spark.stop()
    print("데이터 전처리 완료")


if __name__ == "__main__":
    main()