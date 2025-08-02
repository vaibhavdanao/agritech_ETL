import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = "krishikendra-data-lake"
root_prefix = "bronze/raw/"
entities = [
    "bills", "bill_items", "categories", "customers", "krishikendra",
    "license", "messages", "news", "products", "receipt", "stock",
    "suppliers", "udhari_records", "user_sessions"
]

s3 = boto3.client("s3")

# Step 1: List all folders under bronze/raw/
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket, Prefix=root_prefix, Delimiter='/')

for page in pages:
    for prefix_obj in page.get('CommonPrefixes', []):
        full_prefix = prefix_obj['Prefix']
        if "mobile_number=" not in full_prefix:
            continue

        retailer_id = full_prefix.split("mobile_number=")[1].rstrip('/')
        print(f"\n📦 Found Retailer: {retailer_id}")

        entry_prefix = full_prefix  # mobile_number=XXXX/
        response = s3.list_objects_v2(Bucket=bucket, Prefix=entry_prefix)

        all_keys = [obj['Key'] for obj in response.get("Contents", [])]
        print(f"📄 Total files in {entry_prefix}: {len(all_keys)}")
        for key in all_keys:
            print(f"🔹 Found file: {key}")

        # Find all entry_date folders
        entry_dates = set()
        for key in all_keys:
            parts = key.split('/')
            for part in parts:
                if part.startswith("entry_date="):
                    entry_dates.add(part.split("=")[1])

        for entry_date in sorted(entry_dates):
            base_path = f"s3://{bucket}/{root_prefix}mobile_number={retailer_id}/entry_date={entry_date}/"
            print(f"📅 Processing entry_date: {entry_date}")

            for entity in entities:
                file_path = base_path + f"{entity}.csv"
                print(f"🔍 Trying to read: {file_path}")
                try:
                    df = spark.read.option("header", "true").csv(file_path)

                    if df.rdd.isEmpty():
                        print(f"⚠️ File is empty: {file_path}")
                        continue

                    df = df.withColumn("retailer_id", lit(retailer_id)) \
                           .withColumn("entry_date", lit(entry_date))

                    output_path = f"s3://{bucket}/silver/{entity}/mobile_number={retailer_id}/entry_date={entry_date}/"
                    df.write.mode("overwrite").parquet(output_path)

                    print(f"✅ Wrote to: {output_path}")
                except Exception as e:
                    print(f"❌ Could not read {file_path}: {str(e)}")

job.commit()
