import argparse
import re
import subprocess
import zipfile
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from brfss_config import DATA_DIR, DATASET, DEFAULT_HDFS_RAW_DIR, SELECTED_COLUMNS


def parse_args():
    parser = argparse.ArgumentParser(description="BRFSS Kaggle 데이터 수집 스크립트")
    parser.add_argument("--skip-download", action="store_true", help="이미 파일이 존재하면 다운로드 생략")
    parser.add_argument("--upload-hdfs", action="store_true", help="분할된 CSV를 HDFS에 업로드")
    parser.add_argument("--hdfs-raw-dir", default=DEFAULT_HDFS_RAW_DIR, help="HDFS raw 데이터 저장 경로")
    parser.add_argument("--chunk-size", type=int, default=20000, help="CSV 분할 단위")
    parser.add_argument("--max-chunks", type=int, default=5, help="연도별 저장할 최대 청크 수. 0이면 전체 저장")
    return parser.parse_args()


def download_dataset(data_dir):
    print("Kaggle에서 데이터셋 다운로드받는 중")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path=str(data_dir), unzip=False)

    zip_path = data_dir / f"{DATASET.split('/')[-1]}.zip"
    if not zip_path.exists():
        print("압축 파일을 찾지 못했습니다:", zip_path)
        return

    print("압축 푸는 중")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(data_dir)
    zip_path.unlink()
    print("데이터 다운로드 및 압축 해제 완료")


def find_year_csvs(data_dir):
    return sorted(
        path for path in data_dir.glob("*.csv")
        if re.match(r"^\d{4}", path.name)
    )


def keep_analysis_columns(chunk, year):
    chunk = chunk.copy()
    chunk["YEAR"] = int(year)

    available_cols = [col for col in SELECTED_COLUMNS if col in chunk.columns]
    return chunk[available_cols]


def split_csv_files(data_dir, chunk_size, max_chunks):
    csv_files = find_year_csvs(data_dir)
    if not csv_files:
        print("분할할 연도별 원본 CSV가 없습니다. 이미 part_*.csv가 있으면 이 단계는 건너뜁니다.")
        return

    chunk_limit = None if max_chunks == 0 else max_chunks

    for csv_file in csv_files:
        year = re.match(r"^(\d{4})", csv_file.name).group(1)
        print(f"[{year}년 데이터 분할 중]")

        chunk_reader = pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False)
        try:
            for idx, chunk in enumerate(chunk_reader, start=1):
                if chunk_limit is not None and idx > chunk_limit:
                    break

                output_file = data_dir / f"part_{year}_{idx}.csv"
                keep_analysis_columns(chunk, year).to_csv(output_file, index=False)
                print("저장 완료:", output_file.name)
        finally:
            chunk_reader.close()

        csv_file.unlink()

    print("데이터 분할 완료")


def upload_to_hdfs(data_dir, hdfs_raw_dir):
    part_files = sorted(data_dir.glob("part_*.csv"))
    if not part_files:
        print("HDFS에 올릴 part_*.csv 파일이 없습니다.")
        return

    print("파일 업로드 시작")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_raw_dir], check=True)

    for part_file in part_files:
        print("업로드 중:", part_file.name)
        subprocess.run(["hdfs", "dfs", "-put", "-f", str(part_file), hdfs_raw_dir], check=True)

    print("HDFS 업로드 완료:", hdfs_raw_dir)


def main():
    args = parse_args()
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)

    print("1. 데이터 수집 시작")
    if args.skip_download:
        print("다운로드 스킵")
    else:
        download_dataset(data_dir)

    print("\n다운받은 데이터 분할 시작")
    split_csv_files(data_dir, args.chunk_size, args.max_chunks)

    if args.upload_hdfs:
        upload_to_hdfs(data_dir, args.hdfs_raw_dir)
    else:
        print("\n로컬에서 실행되어 HDFS 업로드는 스킵")


if __name__ == "__main__":
    main()
