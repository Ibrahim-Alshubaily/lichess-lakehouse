from pyspark.sql import SparkSession
import sys

month = sys.argv[1]

spark = (
    SparkSession.builder.appName("Ingest to Iceberg").master("local[*]").getOrCreate()
)

df = (
    spark.read.format("avro")
    .option("recursiveFileLookup", "true")
    .load(f"s3a://processed-data/{month}")
)


df.writeTo("iceberg.games").using("iceberg").createOrReplace()

spark.stop()
