from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col,trim,to_timestamp,when,split,size
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
checkin_silver = (
    checkin
    .dropDuplicates(["business_id"])
    .filter(col("business_id").isNotNull())
    .withColumn("date", to_timestamp("date"))
    )
# checkin_silver.printSchema() 

#reviews

reviews = spark.read.parquet("../data/bronze/reviews")

reviews_silver = (
    reviews
    .dropDuplicates(["review_id"])
    .filter(col("review_id").isNotNull())
    .filter(col("business_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .withColumn("cool", col("cool").cast("int"))
    .withColumn("funny",col("funny").cast("int"))
    .withColumn("stars",col("stars").cast("double"))
    .withColumn("useful",col("useful").cast("int"))
    .filter(((col("stars")>=1)&(col("stars")<=5)))   
)

# reviews.show()
# reviews.printSchema()

reviews_silver.write.mode("overwrite").parquet("../data/silver/reviews")

#tip
tip = spark.read.parquet("../data/bronze/tip")



tip_silver = (
    tip
    .drop_duplicates(["business_id"])
    .filter(col("business_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .withColumn("compliment_count",col("compliment_count").cast("int"))
)
# tip.printSchema()
# tip.show()

tip_silver.write.mode("overwrite").parquet("../data/siver/tip")

user = spark.read.parquet("../data/bronze/user")

user_silver = (
    user
    .dropDuplicates(["user_id"])
    .filter(col("user_id").isNotNull())
    .withColumn(
        "elite_list",
        when(col("elite").isNull() | (col("elite") == ""), None).otherwise(split(col("elite"), ","))
    )
    .withColumn("elite_years_count", size(col("elite_list")))

# )
# user.printSchema()
# user.show()