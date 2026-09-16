import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, count

# 기준 연도 인자 주입 (기본값: 2015)
min_release_year = int(sys.argv[1]) if len(sys.argv) > 1 else 2015
input_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/data/netflix_titles.csv"
output_path = sys.argv[3] if len(sys.argv) > 3 else "/tmp/output/silver"

spark = SparkSession.builder \
    .appName("NetflixTransform") \
    .getOrCreate()

df = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(input_path)

# 예시 로그 형식 대응 (입력 행 수)
print(f"input rows = {df.count()}")

# release_year 필터링 (기준 연도 이상)
filtered_df = df.filter(col("release_year").cast("int") >= min_release_year)

# listed_in 쉼표 컬럼 분해 및 공백 제거
exploded_df = filtered_df.withColumn("genre", explode(split(col("listed_in"), ","))) \
                         .withColumn("genre", trim(col("genre")))

# type x 장르 별 작품 집계
agg_df = exploded_df.groupBy("type", "genre") \
                    .agg(count("*").alias("title_count"))

# 행 수 및 전체 테이블 출력
row_count = agg_df.count()
print(f"집계 행 수: {row_count}")
agg_df.show(100, truncate=False)

# Snappy 압축 Parquet 저장
agg_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

spark.stop()
