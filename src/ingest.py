from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("practise").getOrCreate()

RAW_PATH = "../data/raw"
BRONZE_PATH = "../data/bronze"

files = {
    "business":"yelp_academic_dataset_business.json",
    "checkin":"yelp_academic_dataset_checkin.json",
    "reviews":"yelp_academic_dataset_review.json",
    "tip":"yelp_academic_dataset_tip.json",
    "user":"yelp_academic_dataset_user.json"
}

for file_name,file_path in files.items():
    source_path = os.path.join(RAW_PATH,file_path)
    target_path = os.path.join(BRONZE_PATH,file_name)
    df = spark.read.json(source_path)
    df.write.mode("overwrite").parquet(target_path)