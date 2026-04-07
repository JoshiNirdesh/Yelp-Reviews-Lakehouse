from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("practise").getOrCreate()

business = spark.read.parquet("../data/silver/business")

# business.printSchema()

dim_business = business.select(
    "business_id",
    "name",
    "city",
    "is_open",
    "postal_code",
    "review_count",
    "stars",
    "state"
)

dim_business.write.mode("overwrite").parquet("../data/gold/dim_business")

user = spark.read.parquet("../data/silver/user")

dim_user = user.select(
    "user_id",
    "name",
    "review_count",
    "average_stars",
    "elite_years_count",
    "fans"
    )
dim_user.write.mode("overwrite").parquet("../data/gold/dim_user")


spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

review = spark.read.parquet("../data/silver/reviews")

review = review.withColumn(
    "review_date",
    coalesce(
        expr("try_to_date(date, 'yyyy-MM-dd')"),
        expr("try_to_date(date, 'yyyy/MM/dd')"),
        to_date(expr("try_to_timestamp(date, 'yyyy-MM-dd HH:mm:ss')"))
    )
)

review = review.filter(col("review_date").isNotNull())

date_bounds = review.select(
    min("review_date").alias("min_date"),
    max("review_date").alias("max_date")
).collect()[0]

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]

date_df = spark.sql(f"""
SELECT explode(
    sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)
) AS date_key
""")

dim_date = (
    date_df
    .withColumn("date_id", date_format("date_key", "yyyyMMdd").cast("int"))
    .withColumn("year", year("date_key"))
    .withColumn("month", month("date_key"))
    .withColumn("quarter", quarter("date_key"))
    .withColumn("day", dayofmonth("date_key"))
    .withColumn("year_month", date_format("date_key", "yyyy-MM"))
    .withColumn("month_name", date_format("date_key", "MMMM"))
    .withColumn("day_name", date_format("date_key", "EEEE"))
    .withColumn(
        "is_weekend",
        when(dayofweek("date_key").isin(1, 7), lit(True)).otherwise(lit(False))
    )
)

dim_date.write.mode("overwrite").parquet("../data/gold/dim_date")


review = spark.read.parquet("../data/silver/reviews")

review = review.withColumn(
    "review_date",
    coalesce(
        expr("try_to_date(date, 'yyyy-MM-dd')"),
        expr("try_to_date(date, 'yyyy/MM/dd')"),
        to_date(expr("try_to_timestamp(date, 'yyyy-MM-dd HH:mm:ss')"))
    )
)

review = review.filter(col("review_date").isNotNull())

review = review.withColumn("text_length", length(col("text")))

fact_reviews = review.select(
    "review_id",
    "business_id",
    "user_id",
    col("review_date").alias("date_key"),   # FIXED
    col("stars").alias("review_stars"),
    "useful",
    "funny",
    "cool",
    "text_length"
)

fact_reviews.write.mode("overwrite").parquet("../data/gold/fact_reviews")