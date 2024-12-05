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

# Create namespace if not exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.example_namespace")

# Show available namespaces
spark.sql("SHOW NAMESPACES IN s3tablesbucket").show()

# Show available tables in namespace
spark.sql("SHOW TABLES IN s3tablesbucket.example_namespace").show()

# Create a table
spark.sql("""
    CREATE TABLE IF NOT EXISTS s3tablesbucket.example_namespace.table1
    (id INT, name STRING, value INT)
    USING iceberg
""")

# Insert data into the table
spark.sql("""
    INSERT INTO s3tablesbucket.example_namespace.table1
    VALUES
        (1, 'ABC', 100),
        (2, 'XYZ', 200)
""")

# Query the table
spark.sql("SELECT * FROM s3tablesbucket.example_namespace.table1").show()
