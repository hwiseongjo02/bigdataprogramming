# Data Directory

이 폴더는 Kaggle CDC BRFSS 데이터와 분석 결과를 저장하는 작업 디렉터리이다.

## 데이터 출처

- Dataset: CDC Behavioral Risk Factor Surveillance System
- Kaggle dataset id: `cdc/behavioral-risk-factor-surveillance-system`
- Original provider: CDC BRFSS

## 주요 입력 파일

`data_collection.py` 실행 후 다음 형식의 파일이 생성된다.

```text
part_2011_1.csv
part_2011_2.csv
...
part_2015_6.csv
```

각 파일에는 원본 설문 컬럼과 함께 분석 편의를 위한 `YEAR` 컬럼이 추가된다.

## 주요 사용 컬럼

- `YEAR`: 조사 연도
- `CVDINFR4`: 심근경색 경험 여부
- `BPHIGH4`: 고혈압 진단 여부
- `TOLDHI2`: 고콜레스테롤 진단 여부
- `DIABETE3`: 당뇨 진단 여부
- `SMOKE100`: 평생 100개비 이상 흡연 여부
- `SEX`: 성별
- `_AGEG5YR`: 연령대 그룹
- `_BMI5`: BMI 값. 실제 BMI는 `_BMI5 / 100`으로 계산한다.

## GitHub 저장 정책

대용량 raw 데이터와 Spark 출력 폴더는 GitHub에 커밋하지 않는다. 제출용으로 필요한 경우 100~1000줄 수준의 샘플 파일만 별도로 생성하여 커밋한다.
