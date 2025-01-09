import json
import re
from datetime import datetime, timezone
from invoke import task
from collections import defaultdict
import pandas as pd

from .helpers import (get_volumes_metadata, get_reporter_volumes_metadata, write_paths_to_file, write_volumes_to_file,
                      R2_STATIC_BUCKET, R2_UNREDACTED_BUCKET, S3_ARCHIVE_BUCKET, S3_PDF_FOLDER, S3_CAPTAR_UNREDACTED_FOLDER,
                      RCLONE_R2_UNREDACTED_BASE_URL, RCLONE_R2_CAP_STATIC_BASE_URL, RCLONE_S3_BASE_URL,
                      s3_paginator, r2_paginator, r2_s3_client,
                      OBJECT_PATHS_FILE, VOLUMES_TO_UNREDACT_FILE)


@task
def pdf_paths(ctx, file_path=OBJECT_PATHS_FILE):
    """ Creates file path pairs to copy unredacted pdfs from S3 to r2 unredacted bucket. """
    volumes_metadata = json.loads(get_volumes_metadata())
    s3_files = {}
    for page in s3_paginator.paginate(Bucket=S3_ARCHIVE_BUCKET, Prefix=S3_PDF_FOLDER, PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            s3_files[f"{item['volume_id']}/{item['extension']}/"] = {
                "s3_key": item["Key"],
                "volume_id": (item["Key"].split("/")[-1]).split(".")[0],
                "extension": ".pdf",
            }
    volume_matches = get_volume_matches_for_artifacts(s3_files, volumes_metadata, ".pdf")
    write_paths_to_file(volume_matches, file_path)


@task
def tar_paths(ctx, file_path=OBJECT_PATHS_FILE):
    """ Creates file path pairs to copy unredacted tars to r2 unredacted bucket. """
    volumes_metadata = json.loads(get_volumes_metadata())
    deduped_s3_tars = filter_for_newest_tars()
    extensions = [".tar", ".tar.csv", ".tar.sha256"]
    volume_matches = []

    for extension in extensions:
        volume_matches += get_volume_matches_for_artifacts(deduped_s3_tars, volumes_metadata, extension)
    write_paths_to_file(volume_matches, file_path)


@task
def unredact_volumes(ctx, volume=None, reporter=None, publication_year=None):
    """
    Invoked with
    `invoke unredact.unredact-volumes --volume=32044109578716` or
    `invoke unredact.unredact-volumes --reporter=bta` or
    `invoke unredact.unredact-volumes --publication-year=1930`
    Creates a txt file with source and target path pairs which later will be used for rclone sync
    Creates a txt file with reporter and volume folder data which later will be used for metadata json file updates
    """
    passed_params = [param for param in [volume, reporter, publication_year] if param is not None]
    assert len(passed_params) == 1, "Exactly one parameter has to be passed."

    if volume:
        process_unredaction(volume, None, None)
    elif reporter:
        process_unredaction(None, reporter, None)
    elif publication_year:
        process_unredaction(None, None, publication_year)


@task
def update_volume_fields(ctx, dry_run=False):
    """
    Invoked with `invoke unredact.update-volume-fields`
    The output of the unredact-volumes task is used to decide which volumes need updating.
    Updates the `redacted` fields in top level and reporter level VolumesMetadata.json files.
    Updates the `last_updated` fields in top level and reporter level VolumesMetadata.json files.
    If dry-run is passed, won't update the files.
    """
    with open(VOLUMES_TO_UNREDACT_FILE, 'r') as volumes_file:
        if not bool(volumes_file.readlines()):
            raise Exception(f"Couldn't find any volumes in file.")

        volumes_file.seek(0)
        volumes_metadata = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))

        # make a backup of top level VolumesMetadata.json file in case we need to restore it quickly in the event of a bug
        with open("VolumesMetadata_backup.json", 'w') as backup_file:
            json.dump(volumes_metadata, backup_file, indent=4)

        ### update the top level volumes metadata fields ###

        volumes_to_unredact = volumes_file.readlines()
        # Format example: 2024-01-01T00:00:00+00:00
        current_time = datetime.now(timezone.utc).isoformat()

        for vol in volumes_to_unredact:
            reporter_slug, volume_folder = map(str.strip, vol.split('/', 1))
            for volume in volumes_metadata:
                if reporter_slug == volume["reporter_slug"] and volume_folder == volume["volume_folder"]:
                    volume["redacted"] = False
                    volume["last_updated"] = current_time

        # upload the new top level VolumesMetadata.json
        if not dry_run:
            print("Updating top level VolumesMetadata.json")
            r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(volumes_metadata),
                                    Key="VolumesMetadata.json", ContentType="application/json")

        ### update the reporter level volumes metadata fields ###

        df = pd.read_csv(VOLUMES_TO_UNREDACT_FILE, header=None, names=['volume_string'])
        df[['reporter', 'volume_folder']] = df['volume_string'].str.split('/', expand=True)
        grouped_volume_data = df.groupby('reporter')['volume_folder'].apply(list).to_dict()

        for reporter, volumes in grouped_volume_data.items():
            reporter_volumes_metadata = json.loads(get_reporter_volumes_metadata(R2_STATIC_BUCKET, reporter))
            for volume_folder in volumes:
                for volume in reporter_volumes_metadata:
                    if volume_folder == volume["volume_folder"]:
                        volume["redacted"] = False
                        volume["last_updated"] = current_time

            # upload the new reporter level VolumesMetadata.json
            if not dry_run:
                print(f"Updating reporter level VolumesMetadata.json for reporter {reporter}")
                r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(reporter_volumes_metadata),
                                        Key=f"{reporter}/VolumesMetadata.json", ContentType="application/json")



@task
def add_last_updated_field(ctx, dry_run=False):
    """
    Adds last_updated field to all volumes in VolumesMetadata.json files.
    If dry-run is passed, only prints what would be updated.
    """
    current_time = datetime.now(timezone.utc).isoformat()

    # Update main VolumesMetadata.json
    volumes_metadata = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))
    updated_count = 0

    for volume in volumes_metadata:
        if "last_updated" not in volume:
            volume["last_updated"] = current_time
            updated_count += 1

    print(f"Would update {updated_count} volumes in main VolumesMetadata.json")

    if not dry_run:
        r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(volumes_metadata), Key="VolumesMetadata.json",
                                ContentType="application/json")
        print("Updated main VolumesMetadata.json")

    # Update reporter-specific metadata files
    reporters = set(vol["reporter_slug"] for vol in volumes_metadata)

    for reporter in reporters:
        try:
            reporter_metadata = json.loads(get_reporter_volumes_metadata(R2_STATIC_BUCKET, reporter))
            reporter_updated_count = 0

            for volume in reporter_metadata:
                if "last_updated" not in volume:
                    volume["last_updated"] = current_time
                    reporter_updated_count += 1

            print(f"Would update {reporter_updated_count} volumes in {reporter}/VolumesMetadata.json")

            if not dry_run:
                r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(reporter_metadata),
                                        Key=f"{reporter}/VolumesMetadata.json", ContentType="application/json")
                print(f"Updated {reporter}/VolumesMetadata.json")

        except Exception as e:
            print(f"Error processing {reporter}: {e}")


def create_file_mappings_for_unredaction(volume=None, reporter=None, publication_year=None):
    """
    Creates a list of volumes that need unredaction
    Creates a list of files that need to be copied to static bucket
    """
    if volume:
        unredacted_bucket_volumes = get_volumes_metadata(R2_UNREDACTED_BUCKET)
        static_bucket_volumes = get_volumes_metadata(R2_STATIC_BUCKET)
        unredacted_bucket_volume = [item for item in json.loads(unredacted_bucket_volumes) if item.get("id") == volume]
        static_bucket_volume = [item for item in json.loads(static_bucket_volumes) if item.get("id") == volume]

        if not unredacted_bucket_volume:
            raise Exception(f"Did not find the volume in {R2_UNREDACTED_BUCKET} bucket")

        if not static_bucket_volume:
            raise Exception(f"Did not find the volume in {R2_STATIC_BUCKET} bucket")

        return map_files_for_unredaction(static_bucket_volume, unredacted_bucket_volume)

    if reporter:
        unredacted_bucket_volumes = get_reporter_volumes_metadata(R2_UNREDACTED_BUCKET, reporter)
        static_bucket_volumes = get_reporter_volumes_metadata(R2_STATIC_BUCKET, reporter)

        if not unredacted_bucket_volumes:
            raise Exception(f"Did not find any reporter volumes in {R2_UNREDACTED_BUCKET} bucket")

        if not static_bucket_volumes:
            raise Exception(f"Did not find any reporter volumes in {R2_STATIC_BUCKET} bucket")

        return map_files_for_unredaction(json.loads(static_bucket_volumes), json.loads(unredacted_bucket_volumes))

    if publication_year:
        static_bucket_volumes = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))
        unredacted_bucket_volumes = json.loads(get_volumes_metadata(R2_UNREDACTED_BUCKET))

        vols_published_before = list(filter(
            lambda item: item.get('publication_year') is not None and item['publication_year'] < int(publication_year),
            static_bucket_volumes)
        )

        return map_files_for_unredaction(vols_published_before, unredacted_bucket_volumes)


def map_files_for_unredaction(static_volumes, unredacted_volumes):
    """
    Skips volumes that are already flagged as `unredacted`
    Returns the volumes that need to be unredacted
    Returns a list of files that need replacing in static bucket
    """
    volumes_to_unredact = []
    files = []

    for volume in static_volumes:
        if not volume["redacted"]:
            continue

        if volume["id"] in [unredacted_vol["id"] for unredacted_vol in unredacted_volumes]:
            volumes_to_unredact.append({
                "reporter": volume["reporter_slug"],
                "volume_folder": volume["volume_folder"]
            })
            files.extend(get_unredacted_volume_files(volume))

    return volumes_to_unredact, files


def get_unredacted_volume_files(volume):
    """
    Returns a list of dictionaries with volume file source and destination paths
    """
    key_prefix = f"{volume['reporter_slug']}/{volume['volume_folder']}"
    extensions = ["pdf", "zip", "tar", "tar.csv", "tar.sha256"]
    volume_files = []

    # grab the volume artifacts
    for page in r2_paginator.paginate(Bucket=R2_UNREDACTED_BUCKET, Prefix=f"{key_prefix}.",
                                      PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            if any(ext in item["Key"] for ext in extensions):
                volume_files.append(
                    {
                        "source": f"{RCLONE_R2_UNREDACTED_BASE_URL}{item['Key']}",
                        "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}{item['Key']}",
                    }
                )

    # grab the volume case and metadata files
    for page in r2_paginator.paginate(Bucket=R2_UNREDACTED_BUCKET, Prefix=f"{key_prefix}/",
                                      PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            volume_files.append(
                {
                    "source": f"{RCLONE_R2_UNREDACTED_BASE_URL}{item['Key']}",
                    "destination": f"{RCLONE_R2_CAP_STATIC_BASE_URL}{item['Key']}",
                }
            )

    return volume_files


def get_volume_matches_for_artifacts(s3_files, volumes_metadata, file_type):
    """
    Finds volume - s3 file matches for s3 r2 sync
    """
    volume_matches = []

    for volume in volumes_metadata:
        volume_key = f"{volume['id']}/{file_type}/"

        if volume["redacted"]:
            print("Skipping redacted volume.")
            continue

        if volume_key in s3_files:
            s3_file = s3_files.get(volume_key)
            volume_matches.append(
                {
                    "source": f"{RCLONE_S3_BASE_URL}{s3_file['s3_key']}",
                    "destination": f"{RCLONE_R2_UNREDACTED_BASE_URL}{volume['reporter_slug']}/{volume['volume_folder']}{file_type}"
                }
            )

    return volume_matches


def filter_for_newest_tars():
    """
    There can be multiple versions of tar files for the same volume in archive bucket
    Removes duplicate files by selecting the most recent one for each extension
    """
    grouped_data = defaultdict(list)

    for page in s3_paginator.paginate(Bucket=S3_ARCHIVE_BUCKET, Prefix=S3_CAPTAR_UNREDACTED_FOLDER, PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            volume_id = (item["Key"].split("/")[-1]).split("_unredacted")[0]
            ts_result = re.search(r"\d{4}_\d{2}_\d{2}_\d{2}\.\d{2}\.\d{2}", item["Key"])
            timestamp = "1600" if ts_result is None else ts_result.group(0)
            grouped_data[(item["volume_id"], item["extension"])].append({
                "s3_key": item["Key"],
                "volume_id": volume_id,
                "extension": item["Key"][item["Key"].index(".tar"):],
                "timestamp": timestamp,
            })

    unique_items = []

    for key, items in grouped_data.items():
        if len(items) == 1:
            unique_items.append(items[0])
        else:
            newest_item = max(items, key=lambda x: x["timestamp"])
            unique_items.append(newest_item)

    return {f"{file['volume_id']}/{file['extension']}/": file for file in unique_items}


def process_unredaction(volume, reporter, publication_year):
    """
    Helper function for the unredaction process
    Creates source and target paths for unredaction, and writes them to file
    Writes the volumes that need to be unredacted to a file
    """
    volumes_to_unredact, volume_matches = create_file_mappings_for_unredaction(volume, reporter, publication_year)
    print(f"{len(volumes_to_unredact)} volumes need to be unredacted.")
    if volume_matches:
        write_paths_to_file(volume_matches)
        write_volumes_to_file(volumes_to_unredact)
