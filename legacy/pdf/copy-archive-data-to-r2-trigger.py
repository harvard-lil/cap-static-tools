import json
import logging
import boto3
import os
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")
paginator = s3_client.get_paginator("list_objects_v2")

S3_BUCKET = os.getenv("S3_BUCKET")
R2_STORAGE = os.getenv("R2_STORAGE")
R2_BUCKET = os.getenv("R2_BUCKET")
S3_FOLDER = os.getenv("S3_FOLDER")
ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
BATCH_SIZE = 50


def lambda_handler(event, context):
    """
    Lambda entry point
    """
    pdf_files = get_s3_files(S3_BUCKET, S3_FOLDER)
    volumes_metadata = json.loads(get_volume_metadata())
    volume_matches = get_volume_matches(pdf_files, volumes_metadata)

    # invoke the upload lambda in batches to avoid timeouts
    for match in range(0, len(volume_matches), BATCH_SIZE):
        event_data = {
            "volume_matches": volume_matches[match: match + BATCH_SIZE]}
        lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-west-2:486926067183:function:copy-archive-data-to-r2",
            InvocationType="Event",
            Payload=json.dumps(event_data),
        )

    logger.info("Invocation is complete.")


def get_s3_files(bucket, path):
    """
    Creates a list of dictionaries for each volume pdf that are in the archive bucket
    Pagination is needed as s3.list_objects_v2 can only return max 1000 records at a time
    """
    s3_files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=path, PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            s3_files.append(item["Key"])

    return s3_files


def get_volume_metadata():
    """
    Gets the root level VolumesMetadata.json contents
    """
    r2_s3_client = create_r2_s3_client()
    volumes_metadata = r2_s3_client.get_object(
        Bucket=R2_BUCKET, Key="VolumesMetadata.json"
    )

    return volumes_metadata["Body"].read().decode("utf-8")


def get_volume_matches(s3_files, volumes_metadata):
    """
    Finds volumes for which there are volume pdfs in the archive bucket
    """
    volume_matches = []

    for volume in volumes_metadata:
        file = {}
        unredacted_key = f"pdf/unredacted/{volume['id']}.pdf"
        redacted_key = f"pdf/redacted/{volume['id']}.pdf"

        if not volume["redacted"] and unredacted_key in s3_files:
            file = {
                "r2_key": f"{volume['reporter_slug']}/{volume['volume_folder']}.pdf",
                "s3_key": unredacted_key
            }
        elif volume["redacted"] or unredacted_key not in s3_files:
            if redacted_key in s3_files:
                file = {
                    "r2_key": f"{volume['reporter_slug']}/{volume['volume_folder']}.pdf",
                    "s3_key": redacted_key
                }

        if file:
            volume_matches.append(file)

    return volume_matches


def create_r2_s3_client():
    """
    Creates s3 client for r2
    This is needed as r2 connection expects access credentials
    """
    return boto3.client(
        service_name="s3",
        endpoint_url=R2_STORAGE,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_KEY,
        region_name="auto",
    )