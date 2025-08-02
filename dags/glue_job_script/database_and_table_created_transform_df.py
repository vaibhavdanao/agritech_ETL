import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from pyspark.sql.functions import col, concat_ws
from pyspark.sql import SparkSession

# --- Glue setup ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Config ---
raw_s3_base = "s3://krishikendra-data-lake/silver"
output_s3_base = "s3://krishikendra-data-lake/gold"
glue_database = "silver_cleaned_db"

# --- 1. Create Database if not exists ---
spark.sql(f"CREATE DATABASE IF NOT EXISTS {glue_database}")

# --- 2. Define tables to process ---
tables = ["customers", "bill_items", "bills", "products"]

# --- 3. Load, process, write, and catalog each table ---
for table in tables:
    print(f"\n--- Processing table: {table} ---")

    input_path = f"{raw_s3_base}/{table}/"
    output_path = f"{output_s3_base}/{table}/"

    # Read partitioned parquet files with mobile_number and entry_date
    df = spark.read.option("basePath", input_path).parquet(input_path)

    # Add 'source_system' from partition
    df = df.withColumn("source_system", col("mobile_number"))

    # Namespace key columns
    if "customer_id" in df.columns:
        df = df.withColumn("global_customer_id", concat_ws("_", col("source_system"), col("customer_id")))
    if "bill_id" in df.columns:
        df = df.withColumn("global_bill_id", concat_ws("_", col("source_system"), col("bill_id")))
    if "product_id" in df.columns:
        df = df.withColumn("global_product_id", concat_ws("_", col("source_system"), col("product_id")))

    # Write cleaned data and register table in Glue catalog
    df.write.mode("overwrite") \
        .format("parquet") \
        .partitionBy("source_system", "entry_date") \
        .option("path", output_path) \
        .saveAsTable(f"{glue_database}.{table}")

    print(f"✔️ Finished table: {table}")

# --- Done ---
job.commit()
print("✅ All tables processed and registered in Glue Catalog.")