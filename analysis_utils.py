import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

from brfss_config import AGE_GROUP_LABELS, MODEL_FEATURE_NAMES, MODEL_FEATURES


def set_korean_font():
    font_paths = [
        Path("C:/Windows/Fonts/malgun.ttf"),
        Path("C:/Windows/Fonts/malgunbd.ttf"),
        Path("/usr/share/fonts/truetype/nanum/NanumGothic.ttf"),
        Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    ]

    for font_path in font_paths:
        if font_path.exists():
            font_manager.fontManager.addfont(str(font_path))
            font_name = font_manager.FontProperties(fname=str(font_path)).get_name()
            plt.rcParams["font.family"] = font_name
            plt.rcParams["font.sans-serif"] = [font_name, "DejaVu Sans"]
            plt.rcParams["axes.unicode_minus"] = False
            sns.set_theme(style="whitegrid", font=font_name, rc={"axes.unicode_minus": False})
            print("그래프 한글 폰트 설정:", font_name)
            return

    plt.rcParams["axes.unicode_minus"] = False
    sns.set_theme(style="whitegrid", rc={"axes.unicode_minus": False})
    print("한글 폰트를 찾지 못했습니다. 그래프 글자가 깨질 수 있습니다.")


def normalize_input_path(input_path):
    if isinstance(input_path, list):
        return input_path[0] if len(input_path) == 1 else input_path
    return input_path


def run_and_save_query(spark, query_name, sql_query, output_dir):
    result = spark.sql(sql_query).toPandas()
    csv_path = output_dir / f"{query_name}.csv"
    result.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print("분석 결과 저장 완료:", csv_path)
    return result


def apply_age_labels(data, column_name):
    age_order = [AGE_GROUP_LABELS[i] for i in range(1, 14)]
    labeled = data.copy()
    labeled["age_label"] = labeled[column_name].astype(int).map(AGE_GROUP_LABELS)
    labeled["age_label"] = pd.Categorical(labeled["age_label"], categories=age_order, ordered=True)
    return labeled, "age_label"


def draw_bar_graph(data, x_col, y_col, title, x_label, y_label, file_path, color_palette):
    plot_data = data.copy()
    if x_col == "age_group":
        plot_data, x_col = apply_age_labels(plot_data, x_col)

    plt.figure(figsize=(10, 5))
    sns.barplot(x=x_col, y=y_col, data=plot_data, palette=color_palette)
    plt.title(title, fontsize=15, fontweight="bold")
    plt.xlabel(x_label, fontsize=12)
    plt.ylabel(y_label, fontsize=12)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()
    print("그래프 저장 완료:", file_path)


def draw_heatmap(data, row_col, col_col, value_col, title, file_path):
    plot_data = data.copy()
    if row_col == "age_group":
        plot_data, row_col = apply_age_labels(plot_data, row_col)

    pivot_data = plot_data.pivot(index=row_col, columns=col_col, values=value_col)

    plt.figure(figsize=(9, 7))
    sns.heatmap(pivot_data, annot=True, fmt=".3f", cmap="Reds")
    plt.title(title, fontsize=15, fontweight="bold")
    plt.xlabel("위험요인 개수")
    plt.ylabel("연령대")
    plt.tight_layout()
    plt.savefig(file_path, dpi=150)
    plt.close()
    print("그래프 저장 완료:", file_path)


def save_bmi_distribution(df, output_dir, sample_size):
    bmi_data = (
        df.filter((col("BMI_Real") > 10) & (col("BMI_Real") < 80))
        .select("Has_Disease", "BMI_Real")
        .sample(False, 0.1, seed=42)
        .limit(sample_size)
        .toPandas()
    )
    bmi_data["disease_label"] = bmi_data["Has_Disease"].replace({1: "심근경색 경험자", 0: "비경험자"})
    bmi_data.to_csv(output_dir / "analysis_bmi_distribution_sample.csv", index=False, encoding="utf-8-sig")

    plt.figure(figsize=(8, 6))
    sns.boxplot(x="disease_label", y="BMI_Real", data=bmi_data, palette="Pastel1")
    plt.title("심근경색 경험 여부에 따른 BMI 분포", fontsize=15, fontweight="bold")
    plt.xlabel("")
    plt.ylabel("BMI", fontsize=12)
    plt.tight_layout()
    plt.savefig(output_dir / "plot3_bmi.png", dpi=150)
    plt.close()
    print("그래프 저장 완료:", output_dir / "plot3_bmi.png")


def make_model_dataset(df):
    model_df = df.select(
        col("Has_Disease").cast("double").alias("label"),
        *[col(feature).cast("double").alias(feature) for feature in MODEL_FEATURES],
    ).dropna()

    return model_df.filter(
        (col("_AGEG5YR").between(1, 13))
        & (col("BMI_Real").between(10, 80))
        & (col("SEX").isin([1, 2]))
    )


def save_model_metrics(predictions, model_count, output_dir):
    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc = evaluator.evaluate(predictions)
    test_count = predictions.count()
    correct_count = predictions.filter(col("prediction") == col("label")).count()
    accuracy = correct_count / test_count if test_count else 0

    metrics = pd.DataFrame(
        [
            {"metric": "model_input_rows", "value": model_count},
            {"metric": "test_rows", "value": test_count},
            {"metric": "auc", "value": auc},
            {"metric": "accuracy", "value": accuracy},
        ]
    )
    metrics.to_csv(output_dir / "analysis_model_metrics.csv", index=False, encoding="utf-8-sig")
    print("모델 성능 저장 완료:", output_dir / "analysis_model_metrics.csv")


def save_model_coefficients(model, output_dir):
    rows = []
    for feature, coef in zip(MODEL_FEATURES, list(model.coefficients)):
        rows.append(
            {
                "feature": feature,
                "feature_korean": MODEL_FEATURE_NAMES[feature],
                "coefficient": float(coef),
                "odds_ratio": math.exp(float(coef)),
            }
        )

    coef_df = pd.DataFrame(rows).sort_values("coefficient", ascending=False)
    coef_df.to_csv(output_dir / "analysis_model_coefficients.csv", index=False, encoding="utf-8-sig")
    print("모델 변수 영향도 저장 완료:", output_dir / "analysis_model_coefficients.csv")

    plt.figure(figsize=(9, 5))
    sns.barplot(x="coefficient", y="feature_korean", data=coef_df, palette="coolwarm")
    plt.axvline(0, color="black", linewidth=1)
    plt.title("로지스틱 회귀 기반 심근경색 영향 요인", fontsize=15, fontweight="bold")
    plt.xlabel("회귀계수: 양수일수록 심근경색 경험 가능성 증가")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(output_dir / "plot7_model_coefficients.png", dpi=150)
    plt.close()
    print("그래프 저장 완료:", output_dir / "plot7_model_coefficients.png")


def run_logistic_regression(df, output_dir):
    print("Spark ML 로지스틱 회귀 분석 시작")

    model_df = make_model_dataset(df)
    model_count = model_df.count()
    if model_count == 0:
        print("모델 학습에 사용할 데이터가 없어 로지스틱 회귀 분석을 건너뜁니다.")
        return

    train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)
    assembler = VectorAssembler(inputCols=MODEL_FEATURES, outputCol="features", handleInvalid="skip")

    train_vec = assembler.transform(train_df).select("label", "features")
    test_vec = assembler.transform(test_df).select("label", "features")

    model = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        regParam=0.01,
    ).fit(train_vec)

    predictions = model.transform(test_vec)
    save_model_metrics(predictions, model_count, output_dir)
    save_model_coefficients(model, output_dir)
