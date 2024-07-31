import json

from invoke import task

from .helpers import get_volumes_metadata, get_reporter_volumes_metadata, R2_STATIC_BUCKET, R2_UNREDACTED_BUCKET, \
    RCLONE_R2_UNREDACTED_BASE_URL, RCLONE_R2_CAP_STATIC_BASE_URL, r2_paginator, r2_s3_client, write_paths_to_file, \
    s3_paginator, S3_ARCHIVE_BUCKET, S3_PDF_FOLDER, RCLONE_S3_BASE_URL


@task
def pdf_paths(ctx, file_path="source_target_paths.txt"):
    """ Create file path pairs to copy unredacted pdfs from S3 to unredacted r2 bucket. """
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
def tar_paths(ctx, file_path="source_target_paths.txt"):
    """ Create file path pairs to copy unredacted tars to unredacted r2 bucket. """
    volumes_metadata = json.loads(get_volumes_metadata())
    deduped_s3_tars = filter_for_newest_tars()
    extensions = [".tar", ".tar.csv", ".tar.sha256"]
    volume_matches = []

    for extension in extensions:
        volume_matches += get_volume_matches_for_artifacts(deduped_s3_tars, volumes_metadata, extension)
    write_paths_to_file(volume_matches, file_path)


@task
def volume_paths(ctx, reporter=None, publication_year=None, file_path="source_target_paths.txt"):
    """
    Create file path pairs to copy unredacted volume files from r2 unredacted bucket to static bucket.
    Must specify either reporter or publication_year.
    """
    if reporter and publication_year:
        raise ValueError("Cannot pass reporter and publication_year at the same time.")

    if reporter:
        volumes_to_unredact, volume_matches = create_file_mappings_for_unredaction(reporter, None)
        print(f"{len(volumes_to_unredact)} volumes to unredact.")
        if volume_matches:
            write_paths_to_file(volume_matches, file_path)

    elif publication_year:
        volumes_to_unredact, volume_matches = create_file_mappings_for_unredaction(None, publication_year)
        print(f"{len(volumes_to_unredact)} volumes to unredact.")
        if volume_matches:
            write_paths_to_file(volume_matches, file_path)

    else:
        raise ValueError("Must pass reporter or publication_year.")


@task
def update_redacted_field_of_volume(ctx, reporter=None, publication_year=None, dry_run=False):
    """
    Update the redacted flags in top level and reporter level metadata files
    If dry_run is passed, won't proceed with the actual json file update
    """
    if reporter and publication_year:
        raise ValueError("Cannot pass reporter and publication_year at the same time.")

    volumes_to_unredact = []
    if reporter:
        volumes_to_unredact = create_file_mappings_for_unredaction(reporter, None)[0]
    if publication_year:
        volumes_to_unredact = create_file_mappings_for_unredaction(None, publication_year)[0]

    print(f"{len(volumes_to_unredact)} volumes need 'redacted' field update.")

    if volumes_to_unredact and not dry_run:
        volumes_metadata = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))

        for item in volumes_to_unredact:
            for volume in volumes_metadata:
                if item["id"] == volume["id"]:
                    volume["redacted"] = False

        r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(volumes_metadata), Key="VolumesMetadata.json",
                                ContentType="application/json")

        if reporter:
            reporter_volumes_metadata = json.loads(get_reporter_volumes_metadata(R2_STATIC_BUCKET, reporter))
            for item in volumes_to_unredact:
                for volume in reporter_volumes_metadata:
                    if item["id"] == volume["id"]:
                        volume["redacted"] = False

            r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(reporter_volumes_metadata),
                                    Key=f"{reporter}/VolumesMetadata.json", ContentType="application/json")

        if publication_year:
            for item in volumes_to_unredact:
                reporter_volumes_metadata = json.loads(
                    get_reporter_volumes_metadata(R2_STATIC_BUCKET, item["reporter"]))
                for volume in reporter_volumes_metadata:
                    if item["id"] == volume["id"]:
                        volume["redacted"] = False

                r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Body=json.dumps(reporter_volumes_metadata),
                                        Key=f"{item["reporter"]}/VolumesMetadata.json", ContentType="application/json")


def create_file_mappings_for_unredaction(reporter=None, publication_year=None):
    """
    Creates a list of volumes that need unredaction
    Creates a list of files that need to be copied to static bucket
    """
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
    Filters out non qualifying volumes
    Returns the ids of volumes that need to be unredacted
    Returns a list of files that need replacing in static bucket
    """
    volumes_to_unredact = []
    files = []

    for volume in static_volumes:
        if not volume["redacted"]:
            continue

        if volume["id"] in [unredacted_vol["id"] for unredacted_vol in unredacted_volumes]:
            volumes_to_unredact.append({
                "id": volume["id"],
                "reporter": volume["reporter_slug"]
            })
            files.extend(get_unredacted_volume_files(volume))

    return volumes_to_unredact, files


def get_unredacted_volume_files(volume):
    """
    Returns a list of volumes files from unredacted bucket
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

    for page in s3_paginator.paginate(Bucket=S3_ARCHIVE_BUCKET, Prefix=S3_CAPTAR_FOLDER, PaginationConfig={"PageSize": 1000}):
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
