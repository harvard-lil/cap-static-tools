import json
import logging
import boto3
import os
import re
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")
paginator = s3_client.get_paginator("list_objects_v2")

S3_BUCKET = os.getenv("S3_BUCKET")
R2_STORAGE = os.getenv("R2_STORAGE")
R2_BUCKET = os.getenv("R2_BUCKET")
S3_REDACTED_FOLDER = os.getenv("S3_REDACTED_FOLDER")
S3_UNREDACTED_FOLDER = os.getenv("S3_UNREDACTED_FOLDER")
ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
BATCH_SIZE = 10


def lambda_handler(event, context):
    """
    Lambda entry point
    """
    volumes_metadata = json.loads(get_volume_metadata())
    s3_items_dict = create_deduped_files_dict()
    tar_files = get_files_for_extension(volumes_metadata, s3_items_dict, '.tar')
    tar_csv_files = get_files_for_extension(volumes_metadata, s3_items_dict, '.tar.csv')
    tar_sha256_files = get_files_for_extension(volumes_metadata, s3_items_dict, '.tar.sha256')
    volume_matches = tar_files + tar_csv_files + tar_sha256_files

    # invoke the upload lambda in batches to avoid timeouts
    for match in range(0, len(volume_matches), BATCH_SIZE):
        event_data = {
            "volume_matches": volume_matches[match: match + BATCH_SIZE]}
        lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-west-2:486926067183:function:copy-tar-files-to-r2",
            InvocationType="Event",
            Payload=json.dumps(event_data),
        )

    logger.info("Invocation is complete.")


def get_s3_files(bucket, path, redact_str):
    """
    Creates a list of dictionaries for each tar file that is in the archive bucket
    Pagination is needed as s3.list_objects_v2 can only return max 1000 records at a time
    """
    s3_files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=path, PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            volume_id = (item["Key"].split('/')[-1]).split(f"_{redact_str}")[0]
            ts_result = re.search(r'\d{4}_\d{2}_\d{2}_\d{2}\.\d{2}\.\d{2}', item["Key"])
            timestamp = '2024' if ts_result is None else ts_result.group(0)
            file = {
                "s3_key": item["Key"],
                "volume_id": volume_id,
                "redacted": redact_str,
                "extension": item["Key"][item["Key"].index('.tar'):],
                "timestamp": timestamp
            }
            s3_files.append(file)

    return s3_files

def create_deduped_files_dict():
    redacted_files = get_s3_files(S3_BUCKET, S3_REDACTED_FOLDER, 'redacted')
    unredacted_files = get_s3_files(S3_BUCKET, S3_UNREDACTED_FOLDER, 'unredacted')
    all_files = redacted_files + unredacted_files
    grouped_data = defaultdict(list)
    
    for item in all_files:
        key = (item["volume_id"], item["extension"], item["redacted"])
        grouped_data[key].append(item)

    # Create a new list of dictionaries with only unique items based on the timestamp column
    unique_items = []
    for key, items in grouped_data.items():
        if len(items) == 1:
            unique_items.append(items[0])
        else:
            newest_item = max(items, key=lambda x: x["timestamp"])
            unique_items.append(newest_item)

    return {f"{file['volume_id']}/{file['redacted']}/{file['extension']}/": file for file in unique_items}


def get_volume_metadata():
    """
    Gets the root level VolumesMetadata.json contents
    """
    r2_s3_client = create_r2_s3_client()
    volumes_metadata = r2_s3_client.get_object(
        Bucket=R2_BUCKET, Key="VolumesMetadata.json"
    )

    return volumes_metadata["Body"].read().decode("utf-8")


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


def get_files_for_extension(volumes, s3_items, extension):
    """
    Finds matching volume files by extension
    """
    matches = []

    for volume in volumes:
        match_file = {}
        redacted_file_lookup = f"{volume['id']}/redacted/{extension}/"
        unredacted_file_lookup = f"{volume['id']}/unredacted/{extension}/"

        if not volume["redacted"] and unredacted_file_lookup in s3_items:
            s3_file = s3_items.get(unredacted_file_lookup)
            match_file = {
                "r2_key": f"{volume['reporter_slug']}/{volume['volume_folder']}{extension}",
                "s3_key": s3_file['s3_key']
            }
        elif volume["redacted"] or unredacted_file_lookup not in s3_items:
            if redacted_file_lookup in s3_items:
                s3_file = s3_items.get(redacted_file_lookup)
                match_file = {
                    "r2_key": f"{volume['reporter_slug']}/{volume['volume_folder']}{extension}",
                    "s3_key": s3_file['s3_key']
                }

        if match_file:
            matches.append(match_file)

    return matches
