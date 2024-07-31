import os
import re
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

# config
S3_ARCHIVE_BUCKET = os.environ.get("S3_ARCHIVE_BUCKET")
S3_PDF_FOLDER = os.environ.get("S3_PDF_FOLDER")
S3_CAPTAR_FOLDER = os.environ.get("S3_CAPTAR_FOLDER")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
R2_STORAGE = os.environ.get("R2_STORAGE")
R2_STATIC_BUCKET = os.environ.get("R2_STATIC_BUCKET")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_UNREDACTED_BUCKET = os.environ.get("R2_UNREDACTED_BUCKET")
RCLONE_S3_BASE_URL = f"cap_s3:{S3_ARCHIVE_BUCKET}/"
RCLONE_R2_UNREDACTED_BASE_URL = f"cap_r2:{R2_UNREDACTED_BUCKET}/"
RCLONE_R2_CAP_STATIC_BASE_URL = f"cap_r2:{R2_STATIC_BUCKET}/"

# clients
s3_client = boto3.client(
    "s3", aws_access_key_id=S3_ACCESS_KEY_ID, aws_secret_access_key=S3_ACCESS_KEY
)
s3_paginator = s3_client.get_paginator("list_objects_v2")
r2_s3_client = boto3.client(
    service_name="s3",
    endpoint_url=R2_STORAGE,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_ACCESS_KEY,
    region_name="auto",
)
r2_paginator = r2_s3_client.get_paginator("list_objects_v2")


def get_volumes_metadata(r2_bucket=R2_UNREDACTED_BUCKET):
    """
    Gets the root level VolumesMetadata.json contents
    """
    volumes_metadata = r2_s3_client.get_object(Bucket=r2_bucket, Key="VolumesMetadata.json")
    return volumes_metadata["Body"].read().decode("utf-8")


def write_paths_to_file(files, file_name="source_target_paths.txt"):
    """
    Writes the source and destination file paths to a txt file
    """
    with open(file_name, "w") as file:
        for file_pair in files:
            file.write(f"{file_pair['source']} {file_pair['destination']}\n")

    print(f"{len(files)} path pairs were written to txt file.")


def get_reporter_volumes_metadata(bucket, reporter):
    """
    Gets the reporter level VolumesMetadata.json contents
    """
    key = f"{reporter}/VolumesMetadata.json"
    try:
        volumes_metadata = r2_s3_client.get_object(Bucket=bucket, Key=key)
        return volumes_metadata["Body"].read().decode("utf-8")
    except ClientError as e:
        print(f"Reporter volume metadata not found in {bucket} bucket: {key}: {e}")
        return


