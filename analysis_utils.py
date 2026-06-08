import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import font_manager
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

from brfss_config import AGE_GROUP_LABELS, MODEL_FEATURE_NAMES, MODEL_FEATURES


NAVY = "#12304A"
BLUE = "#2D6CDF"
TEAL = "#14A3A5"
SKY = "#8EC9E8"
MINT = "#D7F0EC"
RED = "#D95858"
GRAY = "#64748B"
GRID = "#E6EEF5"
PRESENTATION_DPI = 190


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


def use_presentation_style():
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#D5E1EA",
            "axes.labelcolor": GRAY,
            "xtick.color": GRAY,
            "ytick.color": GRAY,
            "text.color": NAVY,
            "axes.titlecolor": NAVY,
            "font.size": 10,
        }
    )


def clean_axes(ax, *, y_grid=True, x_grid=False):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#D5E1EA")
    ax.spines["bottom"].set_color("#D5E1EA")
    ax.tick_params(colors=GRAY, labelsize=10)

    if y_grid:
        ax.yaxis.grid(True, color=GRID, linewidth=1)
    if x_grid:
        ax.xaxis.grid(True, color=GRID, linewidth=1)
    ax.set_axisbelow(True)


def save_presentation_plot(fig, file_path):
    fig.tight_layout()
    fig.savefig(file_path, dpi=PRESENTATION_DPI, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("그래프 저장 완료:", file_path)


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

    use_presentation_style()
    plot_data["rate_percent"] = plot_data[y_col] * 100

    if x_col == "year":
        fig, ax = draw_year_line_chart(plot_data, x_col)
    else:
        fig, ax = draw_rate_bar_chart(plot_data, x_col)

    ax.set_title(title, loc="left", fontsize=16, fontweight="bold", pad=14)
    ax.set_xlabel(x_label if x_col != "age_label" else "", fontsize=11)
    ax.set_ylabel("비율(%)" if y_label == "비율" else y_label, fontsize=11)
    clean_axes(ax)
    save_presentation_plot(fig, file_path)


def draw_year_line_chart(plot_data, x_col):
    fig, ax = plt.subplots(figsize=(6.8, 4.3))
    ax.plot(
        plot_data[x_col],
        plot_data["rate_percent"],
        color=TEAL,
        linewidth=3,
        marker="o",
        markersize=7,
    )
    ax.fill_between(
        plot_data[x_col],
        plot_data["rate_percent"],
        plot_data["rate_percent"].min() - 0.15,
        color=MINT,
        alpha=0.65,
    )
    ax.set_xticks(plot_data[x_col])
    ax.set_ylim(plot_data["rate_percent"].min() - 0.2, plot_data["rate_percent"].max() + 0.25)

    for x_value, y_value in zip(plot_data[x_col], plot_data["rate_percent"]):
        ax.text(x_value, y_value + 0.035, f"{y_value:.2f}%", ha="center", fontsize=9)

    return fig, ax


def draw_rate_bar_chart(plot_data, x_col):
    fig, ax = plt.subplots(figsize=(8.6, 4.6))
    labels = plot_data[x_col].astype(str)
    bars = ax.bar(labels, plot_data["rate_percent"], color=make_bar_colors(x_col, len(plot_data)), width=0.68)
    ax.set_ylim(0, plot_data["rate_percent"].max() * 1.22)

    for index, (bar, rate) in enumerate(zip(bars, plot_data["rate_percent"])):
        show_label = x_col != "age_label" or index in [0, len(plot_data) - 1]
        if show_label:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                rate + plot_data["rate_percent"].max() * 0.025,
                f"{rate:.1f}%",
                ha="center",
                fontsize=10,
                fontweight="bold",
            )

    if x_col == "age_label":
        ax.tick_params(axis="x", rotation=35)

    return fig, ax


def make_bar_colors(x_col, count):
    if x_col == "age_label":
        return plt.cm.Blues(np.linspace(0.35, 0.9, count))
    if x_col == "risk_score":
        return plt.cm.YlGnBu(np.linspace(0.35, 0.88, count))
    if x_col == "blood_pressure_group":
        return [TEAL, SKY][:count]
    return [BLUE] * count


def draw_heatmap(data, row_col, col_col, value_col, title, file_path):
    plot_data = data.copy()
    if row_col == "age_group":
        plot_data, row_col = apply_age_labels(plot_data, row_col)

    pivot_data = plot_data.pivot(index=row_col, columns=col_col, values=value_col)

    use_presentation_style()
    fig, ax = plt.subplots(figsize=(7.5, 5.2))
    sns.heatmap(
        pivot_data * 100,
        annot=True,
        fmt=".1f",
        cmap="YlGnBu",
        linewidths=0.4,
        linecolor="#F1F5F9",
        cbar_kws={"label": "비율(%)", "fraction": 0.035, "pad": 0.02},
        ax=ax,
    )
    ax.set_title(title, loc="left", fontsize=15, fontweight="bold", pad=14)
    ax.set_xlabel("위험요인 개수", fontsize=11)
    ax.set_ylabel("연령대", fontsize=11)
    ax.tick_params(colors=GRAY, labelsize=9)
    save_presentation_plot(fig, file_path)


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

    use_presentation_style()
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    sns.boxplot(
        x="disease_label",
        y="BMI_Real",
        hue="disease_label",
        data=bmi_data,
        palette=[MINT, "#D7E8FF"],
        showfliers=False,
        width=0.52,
        linewidth=1.2,
        legend=False,
        ax=ax,
    )
    ax.set_title("심근경색 경험 여부에 따른 BMI 분포", loc="left", fontsize=16, fontweight="bold", pad=14)
    ax.set_xlabel("")
    ax.set_ylabel("BMI", fontsize=11)
    clean_axes(ax)
    save_presentation_plot(fig, output_dir / "plot3_bmi.png")


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

    use_presentation_style()
    plot_df = coef_df.sort_values("coefficient")
    colors = [RED if value < 0 else TEAL if value > 0.55 else SKY for value in plot_df["coefficient"]]

    fig, ax = plt.subplots(figsize=(7.5, 4.7))
    ax.barh(plot_df["feature_korean"], plot_df["coefficient"], color=colors, height=0.62)
    ax.axvline(0, color="#94A3B8", linewidth=1.2)
    ax.set_title("로지스틱 회귀 기반 심근경색 영향 요인", loc="left", fontsize=16, fontweight="bold", pad=14)
    ax.set_xlabel("회귀계수", fontsize=11)
    ax.set_ylabel("")
    clean_axes(ax, y_grid=False, x_grid=True)

    for y_pos, value in enumerate(plot_df["coefficient"]):
        align = "left" if value >= 0 else "right"
        offset = 0.025 if value >= 0 else -0.025
        ax.text(value + offset, y_pos, f"{value:.2f}", va="center", ha=align, fontsize=9)

    save_presentation_plot(fig, output_dir / "plot7_model_coefficients.png")


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
