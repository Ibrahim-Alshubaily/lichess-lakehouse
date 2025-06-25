from pyspark.sql import SparkSession
import sys

sourceDataPath = sys.argv[1]

spark = (
    SparkSession.builder.appName("Ingest to Iceberg").master("local[*]").getOrCreate()
)

df = (
    spark.read.format("avro").option("recursiveFileLookup", "true").load(sourceDataPath)
)


spark.sql("SHOW CATALOGS").show()
spark.sql("SELECT current_catalog()").show()


spark.sql(
    """
CREATE SCHEMA IF NOT EXISTS lichess
"""
)

df.writeTo("lichess.games").using("iceberg").createOrReplace()


spark.stop()
