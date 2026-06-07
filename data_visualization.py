import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

from pyspark.sql import SparkSession

from analysis_utils import (
    draw_bar_graph,
    draw_heatmap,
    normalize_input_path,
    run_and_save_query,
    run_logistic_regression,
    save_bmi_distribution,
    set_korean_font,
)
from brfss_config import DATA_DIR


INPUT_PATH = f"{DATA_DIR}/cleaned_cardio_data/*.csv"
OUTPUT_DIR = f"{DATA_DIR}/analysis_results"


BASIC_ANALYSES = [
    {
        "name": "analysis_age_disease_rate",
        "query": """
            SELECT
                _AGEG5YR AS age_group,
                COUNT(*) AS respondent_count,
                AVG(Has_Disease) AS disease_rate
            FROM cardio
            WHERE _AGEG5YR BETWEEN 1 AND 13
            GROUP BY _AGEG5YR
            ORDER BY _AGEG5YR
        """,
        "plot": {
            "x": "age_group",
            "y": "disease_rate",
            "title": "연령대별 심근경색 경험 비율",
            "xlabel": "연령대 그룹",
            "ylabel": "비율",
            "file": "plot1_age.png",
            "palette": "Blues",
        },
    },
    {
        "name": "analysis_blood_pressure_disease_rate",
        "query": """
            SELECT
                CASE BP_Risk
                    WHEN 1 THEN '고혈압 있음'
                    WHEN 0 THEN '고혈압 없음'
                END AS blood_pressure_group,
                COUNT(*) AS respondent_count,
                AVG(Has_Disease) AS disease_rate
            FROM cardio
            WHERE BP_Risk IS NOT NULL
            GROUP BY BP_Risk
            ORDER BY BP_Risk DESC
        """,
        "plot": {
            "x": "blood_pressure_group",
            "y": "disease_rate",
            "title": "고혈압 유무별 심근경색 경험 비율",
            "xlabel": "고혈압 유무",
            "ylabel": "비율",
            "file": "plot2_bp.png",
            "palette": "Oranges",
        },
    },
    {
        "name": "analysis_risk_score_disease_rate",
        "query": """
            SELECT
                Risk_Score AS risk_score,
                COUNT(*) AS respondent_count,
                AVG(Has_Disease) AS disease_rate
            FROM cardio
            WHERE Risk_Score IS NOT NULL
            GROUP BY Risk_Score
            ORDER BY Risk_Score
        """,
        "plot": {
            "x": "risk_score",
            "y": "disease_rate",
            "title": "위험요인 중첩 개수별 심근경색 경험 비율",
            "xlabel": "위험요인 개수(0~4개)",
            "ylabel": "비율",
            "file": "plot4_risk_factors.png",
            "palette": "Reds",
        },
    },
    {
        "name": "analysis_year_disease_rate",
        "query": """
            SELECT
                YEAR AS year,
                COUNT(*) AS respondent_count,
                AVG(Has_Disease) AS disease_rate
            FROM cardio
            WHERE YEAR IS NOT NULL
            GROUP BY YEAR
            ORDER BY YEAR
        """,
        "plot": {
            "x": "year",
            "y": "disease_rate",
            "title": "연도별 심근경색 경험 비율 추이",
            "xlabel": "연도",
            "ylabel": "비율",
            "file": "plot5_year.png",
            "palette": "Greens",
        },
    },
]


AGE_RISK_QUERY = """
    SELECT
        _AGEG5YR AS age_group,
        Risk_Score AS risk_score,
        COUNT(*) AS respondent_count,
        AVG(Has_Disease) AS disease_rate
    FROM cardio
    WHERE _AGEG5YR BETWEEN 1 AND 13
      AND Risk_Score IS NOT NULL
    GROUP BY _AGEG5YR, Risk_Score
    ORDER BY _AGEG5YR, Risk_Score
"""


def parse_args():
    parser = argparse.ArgumentParser(description="BRFSS 심근경색 분석 및 시각화")
    parser.add_argument("--input", nargs="+", default=[INPUT_PATH])
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--bmi-sample-size", type=int, default=100000)
    return parser.parse_args()


def draw_sql_result(result, plot_info, output_dir):
    draw_bar_graph(
        result,
        plot_info["x"],
        plot_info["y"],
        plot_info["title"],
        plot_info["xlabel"],
        plot_info["ylabel"],
        output_dir / plot_info["file"],
        plot_info["palette"],
    )


def run_basic_sql_analyses(spark, output_dir):
    for analysis in BASIC_ANALYSES:
        result = run_and_save_query(spark, analysis["name"], analysis["query"], output_dir)
        draw_sql_result(result, analysis["plot"], output_dir)


def run_advanced_sql_analysis(spark, output_dir):
    age_risk_stats = run_and_save_query(spark, "analysis_age_risk_matrix", AGE_RISK_QUERY, output_dir)
    draw_heatmap(
        age_risk_stats,
        "age_group",
        "risk_score",
        "disease_rate",
        "연령대와 위험요인 중첩에 따른 심근경색 경험 비율",
        output_dir / "plot6_age_risk_heatmap.png",
    )


def main():
    args = parse_args()
    input_path = normalize_input_path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    set_korean_font()
    print("Spark SQL 분석 & 시각화 시작")

    spark = SparkSession.builder.appName("BRFSS_Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.createOrReplaceTempView("cardio")

    run_basic_sql_analyses(spark, output_dir)
    save_bmi_distribution(df, output_dir, args.bmi_sample_size)
    run_advanced_sql_analysis(spark, output_dir)
    run_logistic_regression(df, output_dir)

    spark.stop()
    print("모든 분석 및 시각화 작업 종료")


if __name__ == "__main__":
    main()
