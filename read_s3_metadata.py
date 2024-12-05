from pyspark.sql import SparkSession

# Define variables
WAREHOUSE_PATH = "XXX"

# Initialize SparkSession with required configurations
spark = SparkSession.builder \
    .appName("iceberg_lab") \
    .config("spark.jars.packages","software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.0,"
                                  "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.0") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.client.region", "us-east-1") \
    .config("spark.sql.catalog.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", WAREHOUSE_PATH) \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl",
            "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .getOrCreate()


spark.sql("SHOW TABLES in s3tablesbucket.aws_s3_metadata").show()

spark.sql("SELECT * FROM s3tablesbucket.aws_s3_metadata.s3metadata_soumil_dev_bucket_1995").show()
