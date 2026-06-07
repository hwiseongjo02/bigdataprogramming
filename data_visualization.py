import argparse
from pathlib import Path
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession


INPUT_PATH = "./data/cleaned_cardio_data/*.csv"
OUTPUT_DIR = "./data/analysis_results"

def run_and_save_query(spark, query_name, sql_query, output_dir):
    df_result = spark.sql(sql_query)
    pd_result = df_result.toPandas()
    
    # 한글 깨짐 방지
    csv_path = output_dir / "{}.csv".format(query_name)
    pd_result.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    print("분석 결과 저장 완료")
    return pd_result

def draw_bar_graph(data, x_col, y_col, title, x_label, y_label, file_path, color_palette):
    plt.figure(figsize=(10, 5))
    sns.barplot(x=x_col, y=y_col, data=data, palette=color_palette)
    
    plt.title(title, fontsize=15, fontweight="bold")
    plt.xlabel(x_label, fontsize=12)
    plt.ylabel(y_label, fontsize=12)
    plt.tight_layout()
    
    plt.savefig(file_path, dpi=150)
    plt.close()
    print("그래프 저장 완료")

def main():
    parser = argparse.ArgumentParser(description="BRFSS 심근경색 분석 및 시각화")
    parser.add_argument("--input", nargs="+", default=[INPUT_PATH])
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    input_path = args.input[0] if isinstance(args.input, list) else args.input
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Spark SQL 분석 & 시각화 시작")

    spark = SparkSession.builder.appName("BRFSS_Analysis").getOrCreate()
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.createOrReplaceTempView("cardio")

    # 1. 연령대별 심근경색 발병 비율 분석
    age_stats = run_and_save_query(
        spark, "analysis_age_disease_rate",
        """
        SELECT 
            _AGEG5YR AS age_group, 
            AVG(Has_Disease) AS disease_rate 
        FROM cardio 
        GROUP BY _AGEG5YR 
        ORDER BY _AGEG5YR
        """,
        out_dir
    )
    draw_bar_graph(
        age_stats, "age_group", "disease_rate", 
        "연령대별 심근경색 경험 비율", "연령대 그룹", "비율", 
        out_dir / "plot1_age.png", "Blues"
    )

    # 2. 고혈압 여부에 따른 발병 비율 분석
    bp_stats = run_and_save_query(
        spark, "analysis_blood_pressure",
        """
        SELECT 
            BP_Risk, 
            AVG(Has_Disease) AS disease_rate 
        FROM cardio 
        WHERE BP_Risk IS NOT NULL 
        GROUP BY BP_Risk
        """,
        out_dir
    )
    draw_bar_graph(
        bp_stats, "BP_Risk", "disease_rate", 
        "고혈압 유무별 심근경색 비율", "고혈압 유무", "비율", 
        out_dir / "plot2_bp.png", "Oranges"
    )

    # 3. 위험요인 중첩(점수)에 따른 발병 비율 분석
    risk_stats = run_and_save_query(
        spark, "analysis_risk_score",
        """
        SELECT 
            Risk_Score, 
            AVG(Has_Disease) AS disease_rate 
        FROM cardio 
        WHERE Risk_Score IS NOT NULL 
        GROUP BY Risk_Score 
        ORDER BY Risk_Score
        """,
        out_dir
    )
    draw_bar_graph(
        risk_stats, "Risk_Score", "disease_rate", 
        "위험요인 중첩 개수별 심근경색 발병 비율", "위험요인 개수(0~4개)", "비율", 
        out_dir / "plot4_risk_factors.png", "Reds"
    )

    # 4. 연도별 발병 비율 분석
    year_stats = run_and_save_query(
        spark, "analysis_year_disease_rate",
        """
        SELECT 
            YEAR AS year, 
            AVG(Has_Disease) AS disease_rate 
        FROM cardio 
        WHERE YEAR IS NOT NULL 
        GROUP BY YEAR 
        ORDER BY YEAR
        """,
        out_dir
    )
    draw_bar_graph(
        year_stats, "year", "disease_rate", 
        "연도별 심근경색 경험 비율 추이", "연도", "비율", 
        out_dir / "plot5_year.png", "Greens"
    )

    spark.stop()
    print("모든 분석 및 시각화 작업 종료")

if __name__ == "__main__":
    main()