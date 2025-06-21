import sys
import subprocess

import boto3

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin123"
S3_REGION = "us-east-1"

warehouse_bucket = "warehouse"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

try:
    s3.create_bucket(Bucket=warehouse_bucket)
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass


month = sys.argv[1]

submit_cmd = [
    "docker",
    "exec",
    "spark",
    "/opt/spark/bin/spark-submit",
    "--master",
    "local[*]",
    "--packages",
    ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0",
            "org.apache.spark:spark-avro_2.12:3.4.1",
        ]
    ),
]

conf_args = {
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": f"s3a://{warehouse_bucket}/",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.driver.extraJavaOptions": "-Duser.home=/tmp",
}

for k, v in conf_args.items():
    submit_cmd += ["--conf", f"{k}={v}"]

submit_cmd += ["ingest.py", month]

subprocess.run(submit_cmd, check=True)
