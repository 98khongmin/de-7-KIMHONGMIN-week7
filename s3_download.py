import sys
import os
import csv
import boto3

# 버킷 이름: 커맨드라인 인자, 없으면 환경 변수 참조
if len(sys.argv) > 1:
    bucket_name = sys.argv[1]
else:
    bucket_name = os.environ.get("S3_BUCKET_NAME")

if not bucket_name:
    print("Error: 버킷 이름이 지정되지 않았습니다. 인자 또는 S3_BUCKET_NAME 환경 변수를 설정하세요.")
    sys.exit(1)

s3 = boto3.client("s3")
prefix = "bronze/"
target_key = "bronze/netflix_titles.csv"
download_dir = "data"
download_path = os.path.join(download_dir, "netflix_titles.csv")


print("1. bronze/ 아래 객체 목록 및 크기:")
print(f"\ts3://{bucket_name}/{prefix}")
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        size = obj["Size"]
        print(f"\t{key:<35} {size:>12,} bytes")


print("2. netflix_titles.csv 다운로드:")
os.makedirs(download_dir, exist_ok=True)
print(f"\ts3://{bucket_name}/{target_key} -> {download_path}")
s3.download_file(bucket_name, target_key, download_path)
file_size = os.path.getsize(download_path)
print(f"\tdownloaded ({file_size:,} bytes)")


print("3. CSV 레코드 행 수:")
with open(download_path, mode="r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader, None)  # 헤더 라인 스킵
    row_count = sum(1 for _ in reader)

print(f"\trows: {row_count}")
