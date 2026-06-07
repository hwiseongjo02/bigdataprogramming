import argparse
import os
import subprocess
import zipfile
import pandas as pd
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi


DATASET = "cdc/behavioral-risk-factor-surveillance-system"
SAVE_PATH = "./data"
USER = os.environ.get("USER", "maria_dev")
HDFS_RAW_DIR = "/user/"+ USER + "/brfss/raw"

def main():
    parser = argparse.ArgumentParser(description="Kaggle 데이터 다운로드하고 분할, HDFS 업로드 스크립트")
    parser.add_argument("--skip-download", action="store_true", help="이미 파일이 존재하면 다운로드 생략")
    parser.add_argument("--upload-hdfs", action="store_true", help="HDFS에 결과물 업로드")
    args = parser.parse_args()

    save_path = Path(SAVE_PATH)
    save_path.mkdir(parents=True, exist_ok=True)

    print("1. 데이터 수집 시작")

    # 1. Kaggle API로 데이터 다운로드하고 다운로드 받은 파일압축 해제
    if args.skip_download:
        print("다운로드 스킵")
    else:
        print("Kaggle에서 데이터셋 다운로드받는 중")
        api = KaggleApi()
        api.authenticate() 
        api.dataset_download_files(DATASET, path=str(save_path), unzip=False)

        zip_file = save_path / (DATASET.split('/')[-1] + ".zip")
        print("압축 푸는 중")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(save_path)
        zip_file.unlink()  # 로컬 환경 용량 확보용
        print("데이터 다운로드 및 압축 해제 완료")


    # 2. 다운로드 받은 대용량 CSV 파일을 연도별, 청크 단위로 쪼개기
    print("\n다운받은 데이터 분할 시작")
    
    # 다운받은 csv 파일 찾기 
    csv_files = [f for f in save_path.glob("*.csv") if f.name[0].isdigit()]
    
    if not csv_files:
        print("CSV 파일을 찾지 못했음")
        return

    for csv_file in csv_files:
        year_str = csv_file.stem 
        print("[연도별 데이터 분할 중]")
        
        # 청크 사이즈만큼씩 잘라서 읽기
        chunk_size = 50000 
        chunk_iterator = pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False)
        
        # 너무 많이 만들지는 말고 적당하게 만들기
        max_chunks = 5 
        
        for i, chunk in enumerate(chunk_iterator):
            if i >= max_chunks:
                break
                
            chunk["YEAR"] = int(year_str)
            
            output_file = save_path / "part_{}_{}.csv".format(year_str, i+1)
            chunk.to_csv(output_file, index=False)
            print("저장 완료")
            
        # 원본 파일은 분할 다 했으니깐 용량 확보를 위해 삭제
        csv_file.unlink()

    print("데이터 분할 완료")

    # 3. HDFS 업로드
    if args.upload_hdfs:
        print("파일 업로드 시작")
        
        # 에러 방지용으로 미리 파일 생성
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_RAW_DIR], check=False, stderr=subprocess.DEVNULL)
        
        for part_file in save_path.glob("part_*.csv"):
            cmd = ["hdfs", "dfs", "-put", "-f", str(part_file), HDFS_RAW_DIR]
            print("업로드 중")
            subprocess.run(cmd, check=True)
            
        print("HDFS 업로드 완료")
    else:
        print("\n로컬에서 실행되어 HDFS 업로드는 스킵")

if __name__ == "__main__":
    main()