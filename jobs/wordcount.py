from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, split

spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# 파일 읽기
df = spark.read.text("/opt/spark/data/wordcount.txt")


# 단어 분리
words_df = df.select(explode(split(col("value"), " ")).alias("word"))
filtered_df = words_df.filter(col("word") != "")

# 집계
result_df = filtered_df.groupBy("word").count().orderBy(desc("count"))
result_df.show(20, truncate=False)

spark.stop()

