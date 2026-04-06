from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col,trim
spark = SparkSession.builder.appName("practise").getOrCreate()

business = spark.read.parquet("../data/bronze/business")
checkin = spark.read.parquet("../data/bronze/checkin")
reviews = spark.read.parquet("../data/bronze/reviews")
tip = spark.read.parquet("../data/bronze/tip")
user = spark.read.parquet("../data/bronze/user")

#business
business_silver = (
    business
    .dropDuplicates(["business_id"])
    .filter(col("business_id").isNotNull())
    .withColumn("name", trim(col("name")))
    .withColumn("postal_code", trim(col("postal_code")))
    .withColumn("review_count", col("review_count").cast("int"))
    .withColumn("stars",col("stars").cast("double"))
    .filter((col("stars")>=0)&(col("stars")<=5))
    .filter(col("review_count")>0)
)
# business.printSchema()
# print(business.count())
# print(business_silver.count())
# business_silver.printSchema()

business_silver.write.mode("overwrite").parquet("../data/silver/business")

#checkin
checkin = spark.read.parquet("../data/bronze/checkin")

checkin.printSchema()