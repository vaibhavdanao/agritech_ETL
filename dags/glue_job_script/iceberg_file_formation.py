import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import SparkSession

# Get job args
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------
# Iceberg Catalog Configuration
# -------------------------------
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://krishikendra-data-lake/iceberg/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# -------------------------------
# Source and Target Config
# -------------------------------
gold_base_path = "s3://krishikendra-data-lake/gold"
iceberg_database = "retail_iceberg"
tables = ["customers", "bill_items", "bills", "products"]  # You can add more

# -------------------------------
# Conversion Loop: Parquet to Iceberg
# -------------------------------
for table in tables:
    print(f"\n🔄 Converting table: {table}")

    input_path = f"{gold_base_path}/{table}/"

    try:
        df = spark.read.parquet(input_path)
        print(f"✅ Read success: {input_path}")
    except Exception as e:
        print(f"❌ Failed to read {input_path}: {e}")
        continue

    # Write to Iceberg table (will auto-create in Glue Catalog)
    target_table = f"{iceberg_database}.{table}"

    try:
        (
            df.writeTo(target_table)
              .using("iceberg")
              .option("merge-schema", "true")
              .partitionedBy("source_system", "entry_date")
              .createOrReplace()
        )
        print(f"✅ Iceberg table created: {target_table}")
    except Exception as e:
        print(f"❌ Failed writing to Iceberg: {target_table}: {e}")
        
job.commit()