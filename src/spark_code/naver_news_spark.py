from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import isnull
from pyspark.sql.functions import col, to_date, to_timestamp,regexp_replace, concat, sha2
from pyspark.sql import Row
from pyspark.sql.functions import lit
from datetime import timedelta
from datetime import datetime as dt

spark = SparkSession.builder.appName("NavernewsToPostgresql").getOrCreate()

#S3에서 데이터 read
parquet_file_path = "s3a://hyunwoo-toy-project-bucket/naver_news/"
day = (dt.today()-timedelta(days=1)).strftime("%Y%m%d")

# parquet파일 dataframe형태로 변경 
naver_news = spark.read.parquet(parquet_file_path + f'{day}_news.parquet')

# 2. 데이터 전처리 (datetime, content/c_content null값 제거)
naver_news = naver_news.dropna('any')#null값 제거

# 3. title+content+c_content 합쳐서 중복 방지 해쉬값 생성
#title + content + c_content 컬럼의 데이터 합치기 
naver_news = naver_news.withColumn("concatenated", concat(col("title"),col("content"),col("c_content")))\
    .withColumn("dup_hash",sha2("concatenated",256))

# 4. sql 테이블에 hash 테이블을 생성하여 새롭게 insert될 row들과 비교
jdbc_url = "jdbc:postgresql://postgresqlIp/databases"
properties = {"user": userid, "password": password, "driver": "org.postgresql.Driver"}

# 5. 중복 제거된 row들을 postgreSQL에 insert
# PostgreSQL 테이블의 데이터를 DataFrame으로 읽기
sql_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "naver_news").option("user", "root").option("password", "1234").load()

# 중복 제거를 위한 DataFrame
unique_df = naver_news.join(sql_df, "dup_hash", "left_anti")

# 중복 데이터가 제거된 DataFrame을 삽입
# unique_df.write.mode("append").saveAsTable("naver_news")
unique_df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "naver_news").option("user", "root").option("password", "1234").mode("append").save()

# 추가된 DataFrame 개수 
unique_df.count()


spark.stop()
