import logging
import boto3
import os
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")

S3_BUCKET = os.getenv("S3_BUCKET")
R2_STORAGE = os.getenv("R2_STORAGE")
R2_BUCKET = os.getenv("R2_BUCKET")
ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")


def lambda_handler(event, context):
    """
    Lambda entry point
    """
    files = event["volume_matches"]
    upload_files(files)
    logger.info("PDF upload is complete.")


def create_r2_s3_client():
    """
    Creates s3 client for r2
    Needed as r2 connection expects access credentials
    """
    return boto3.client(
        service_name="s3",
        endpoint_url=R2_STORAGE,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_KEY,
        region_name="auto",
    )


def upload_files(files):
    """
    Gets the files from s3 bucket, uploads to r2 bucket
    """
    r2_s3_client = create_r2_s3_client()

    for file in files:
        file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=file["s3_key"])
        try:
            file_content = file_obj["Body"].read()
            r2_s3_client.upload_fileobj(
                io.BytesIO(file_content), R2_BUCKET, file["r2_key"]
            )
        except ClientError as e:
            logger.error(f"{file['s3_key']} - {file['r2_key']}: {e}")
