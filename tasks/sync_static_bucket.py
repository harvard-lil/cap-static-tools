import json
import re
from collections import defaultdict
from invoke import task

from .helpers import (
    get_volumes_metadata,
    write_paths_to_file,
    s3_paginator,
    R2_STATIC_BUCKET,
    RCLONE_R2_CAP_STATIC_BASE_URL,
    S3_ARCHIVE_BUCKET,
    RCLONE_S3_BASE_URL,
    S3_CAPTAR_REDACTED_FOLDER,
    S3_CAPTAR_UNREDACTED_FOLDER,
    S3_PDF_FOLDER,
    OBJECT_PATHS_FILE
)


@task
def tar_paths(ctx, file_path=OBJECT_PATHS_FILE):
    """
    Creates file path pairs to copy tar files from s3 to r2 cap-static bucket.
    """
    volumes_metadata = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))
    deduped_s3_tars = filter_for_newest_tars()
    extensions = [".tar", ".tar.csv", ".tar.sha256"]
    volume_matches = []

    for extension in extensions:
        volume_matches += get_volume_matches_for_artifacts(deduped_s3_tars, volumes_metadata, extension)
    write_paths_to_file(volume_matches, file_path)


@task
def pdf_paths(ctx, file_path=OBJECT_PATHS_FILE):
    """
    Creates file path pairs to copy pdf files from s3 to r2 cap-static bucket.
    """
    pdf_files = get_s3_files(S3_ARCHIVE_BUCKET, S3_PDF_FOLDER)
    volumes_metadata = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))
    volume_matches = get_volume_matches_for_pdfs(pdf_files, volumes_metadata)
    write_paths_to_file(volume_matches, file_path)


def get_volume_matches_for_artifacts(s3_files, volumes_metadata, file_type):
    """
    Finds volume - s3 file matches for s3 r2 sync
    """
    volume_matches = []

    for volume in volumes_metadata:
        redacted_file_lookup = f"{volume['id']}/redacted/{file_type}/"
        unredacted_file_lookup = f"{volume['id']}/unredacted/{file_type}/"

        if not volume["redacted"] and unredacted_file_lookup in s3_files:
            s3_file = s3_files.get(unredacted_file_lookup)
            volume_matches.append(
                {
                    "source": f"{RCLONE_S3_BASE_URL}{s3_file['s3_key']}",
                    "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}{volume['reporter_slug']}/"
                                   f"{volume['volume_folder']}{file_type}"
                }
            )
        elif volume["redacted"] or unredacted_file_lookup not in s3_files:
            if redacted_file_lookup in s3_files:
                s3_file = s3_files.get(redacted_file_lookup)
                volume_matches.append(
                    {
                        "source": f"{RCLONE_S3_BASE_URL}{s3_file['s3_key']}",
                        "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}{volume['reporter_slug']}/"
                                       f"{volume['volume_folder']}{file_type}"
                    }
                )

    return volume_matches


def filter_for_newest_tars():
    """
    There can be multiple versions of tar files for the same volume in archive bucket
    Gets all tar files from both redacted and unredacted s3 folders
    Removes duplicate volume files by selecting the most recent one for each extension and folder
    """
    grouped_data = defaultdict(list)

    for page in s3_paginator.paginate(Bucket=S3_ARCHIVE_BUCKET, Prefix=S3_CAPTAR_REDACTED_FOLDER,
                                      PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            volume_id = (item["Key"].split("/")[-1]).split("_redacted")[0]
            ts_result = re.search(r"\d{4}_\d{2}_\d{2}_\d{2}\.\d{2}\.\d{2}", item["Key"])
            timestamp = "1600" if ts_result is None else ts_result.group(0)
            redacted = "redacted"
            extension = item["Key"][item["Key"].index(".tar"):]
            grouped_data[(volume_id, extension, redacted)].append({
                "s3_key": item["Key"],
                "volume_id": volume_id,
                "redacted": redacted,
                "extension": extension,
                "timestamp": timestamp,
            })

    for page in s3_paginator.paginate(Bucket=S3_ARCHIVE_BUCKET, Prefix=S3_CAPTAR_UNREDACTED_FOLDER,
                                      PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            volume_id = (item["Key"].split("/")[-1]).split("_unredacted")[0]
            ts_result = re.search(r"\d{4}_\d{2}_\d{2}_\d{2}\.\d{2}\.\d{2}", item["Key"])
            timestamp = "1600" if ts_result is None else ts_result.group(0)
            redacted = "unredacted"
            extension = item["Key"][item["Key"].index(".tar"):]
            grouped_data[(volume_id, extension, redacted)].append({
                "s3_key": item["Key"],
                "volume_id": volume_id,
                "redacted": redacted,
                "extension": extension,
                "timestamp": timestamp,
            })

    unique_items = []

    for key, items in grouped_data.items():
        if len(items) == 1:
            unique_items.append(items[0])
        else:
            newest_item = max(items, key=lambda x: x["timestamp"])
            unique_items.append(newest_item)

    return {f"{file['volume_id']}/{file['redacted']}/{file['extension']}/": file for file in unique_items}


def get_s3_files(bucket, path):
    """
    Creates a list of dictionaries for each volume pdf that are in the archive bucket
    Pagination is needed as s3.list_objects_v2 can only return max 1000 records at a time
    """
    s3_files = []

    for page in s3_paginator.paginate(Bucket=bucket, Prefix=path, PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            s3_files.append(item["Key"])

    return s3_files


def get_volume_matches_for_pdfs(s3_files, volumes_metadata):
    """
    Finds volumes for which there are volume pdfs in the archive bucket
    """
    volume_matches = []

    for volume in volumes_metadata:
        unredacted_key = f"pdf/unredacted/{volume['id']}.pdf"
        redacted_key = f"pdf/redacted/{volume['id']}.pdf"

        if not volume["redacted"] and unredacted_key in s3_files:
            volume_matches.append(
                {
                    "source": f"{RCLONE_S3_BASE_URL}{unredacted_key}",
                    "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}"
                                   f"{volume['reporter_slug']}/{volume['volume_folder']}.pdf",
                }
            )
        elif volume["redacted"] or unredacted_key not in s3_files:
            if redacted_key in s3_files:
                volume_matches.append(
                    {
                        "source": f"{RCLONE_S3_BASE_URL}{redacted_key}",
                        "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}"
                                       f"{volume['reporter_slug']}/{volume['volume_folder']}.pdf",
                    }
                )

    return volume_matches
