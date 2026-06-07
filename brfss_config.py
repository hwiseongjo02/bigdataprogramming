import os


DATASET = "cdc/behavioral-risk-factor-surveillance-system"
DATA_DIR = "./data"
DEFAULT_USER = os.environ.get("USER") or os.environ.get("USERNAME") or "maria_dev"
DEFAULT_HDFS_RAW_DIR = f"/user/{DEFAULT_USER}/brfss/raw"

CORE_COLUMNS = [
    "YEAR",
    "CVDINFR4",
    "BPHIGH4",
    "TOLDHI2",
    "DIABETE3",
    "SMOKE100",
    "SEX",
    "_AGEG5YR",
    "_BMI5",
]

SUPPORT_COLUMNS = [
    "GENHLTH",
    "PHYSHLTH",
    "MENTHLTH",
    "POORHLTH",
    "EXERANY2",
    "HLTHPLN1",
    "MEDCOST",
    "CHECKUP1",
    "ADDEPEV2",
    "_EDUCAG",
    "_INCOMG",
]

SELECTED_COLUMNS = CORE_COLUMNS + SUPPORT_COLUMNS

AGE_GROUP_LABELS = {
    1: "18-24세",
    2: "25-29세",
    3: "30-34세",
    4: "35-39세",
    5: "40-44세",
    6: "45-49세",
    7: "50-54세",
    8: "55-59세",
    9: "60-64세",
    10: "65-69세",
    11: "70-74세",
    12: "75-79세",
    13: "80세 이상",
}

MODEL_FEATURES = [
    "BP_Risk",
    "Cholesterol_Risk",
    "Diabetes_Risk",
    "Smoking_Risk",
    "SEX",
    "_AGEG5YR",
    "BMI_Real",
]

MODEL_FEATURE_NAMES = {
    "BP_Risk": "고혈압",
    "Cholesterol_Risk": "고콜레스테롤",
    "Diabetes_Risk": "당뇨",
    "Smoking_Risk": "흡연",
    "SEX": "성별",
    "_AGEG5YR": "연령대",
    "BMI_Real": "BMI",
}
