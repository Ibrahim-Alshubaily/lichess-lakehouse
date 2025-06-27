from pyspark.sql import SparkSession
import sys

input_paths = sys.argv[1:]

spark = (
    SparkSession.builder.appName("Ingest to Iceberg").master("local[*]").getOrCreate()
)

df = (
    spark.read.format("avro").option("recursiveFileLookup", "true").load(input_paths)
)

spark.sql("CREATE SCHEMA IF NOT EXISTS lichess")

if not spark.catalog.tableExists("lichess.games"):
    (
        df.writeTo("lichess.games")
        .using("iceberg")
        .option("primary-key", "game_id")
        .create()
    )
else:
    (
        df.writeTo("lichess.games")
        .using("iceberg")
        .option("upsert-enabled", "true")
        .append()
    )

spark.stop()
