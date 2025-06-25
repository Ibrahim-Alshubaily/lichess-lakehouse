import os
import subprocess
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = os.environ["MINIO_USER"]
S3_SECRET_KEY = os.environ["MINIO_PASSWORD"]
WAREHOUSE_BUCKET = os.environ["WAREHOUSE_BUCKET"]

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

try:
    s3.create_bucket(Bucket=WAREHOUSE_BUCKET)
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

# import sys
# month = sys.argv[1]
# input_path = f"s3a://processed-data/{month}/"

cmd = [
    "docker",
    "exec",
    "spark",
    "/opt/spark/bin/spark-submit",
    "--master",
    "local[*]",
    "/app/scripts/ingest.py",
    "s3a://processed-data/",
]

subprocess.run(cmd, check=True)
