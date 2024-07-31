import concurrent.futures
import io
import json
import threading
import zipfile

from botocore.exceptions import ClientError
from invoke import task

from .helpers import get_volumes_metadata, r2_s3_client, r2_paginator

zip_lock = threading.Lock()


@task(default=True)
def zip_volumes(r2_bucket):
    """ Download data for each volume from R2, zip, and upload. """
    volumes = json.loads(get_volumes_metadata(r2_bucket))
    volume_counter = 0

    for volume in volumes:
        # fetch files for volume
        reporter = volume["reporter_slug"]
        volume = volume["volume_folder"]
        json_files = get_case_files_of_volume(reporter, volume, "json", r2_bucket)
        html_files = get_case_files_of_volume(reporter, volume, "html", r2_bucket)
        metadata_files = [
            f"{reporter}/{volume}/VolumeMetadata.json",
            f"{reporter}/{volume}/CasesMetadata.json",
        ]
        files = json_files + html_files + metadata_files

        # write files to zip buffer
        bytes_io = io.BytesIO()
        with zipfile.ZipFile(bytes_io, "a", zipfile.ZIP_DEFLATED) as zip_file:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                file_folder_pairs = [(file, get_folder(file)) for file in files]
                futures = [
                    executor.submit(fetch_and_write_to_zip, zip_file, file, folder, r2_bucket)
                    for file, folder in file_folder_pairs
                ]
                concurrent.futures.wait(futures)
        bytes_io.seek(0)

        # upload zip
        file_name = f"{reporter}/{volume}.zip"
        try:
            r2_s3_client.upload_fileobj(bytes_io, file_name)
        except ClientError as e:
            print(f"File upload error for: {file_name}: {e}")

        volume_counter += 1
        print(f"{volume_counter}/{len(volumes)} were processed")



def get_case_files_of_volume(reporter, volume, file_type, bucket):
    """
    Gets json and html files of a volume
    """
    prefix = create_prefix(reporter, volume, file_type)
    files_for_volumes = []

    for page in r2_paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            files_for_volumes.append(item["Key"])

    return files_for_volumes


def fetch_and_write_to_zip(zip_file, file, folder, bucket):
    """
    Fetches file content from R2 and writes to zip file
    """
    content = r2_s3_client.get_object(Bucket=bucket, Key=file)["Body"].read()
    file_name = file.split("/")[-1]

    with zip_lock:
        zip_file.writestr(f"{folder}/{file_name}", content)


def get_folder(file):
    """
    Returns folder name that will be used in zip file folder tree
    """
    if file.endswith("Metadata.json"):
        return "metadata"
    elif file.endswith(".html"):
        return "html"
    else:
        return "json"


def create_prefix(reporter, volume, file_type):
    """
    Creates the object prefix from file type
    """
    return (
        f"{reporter}/{volume}/cases"
        if file_type == "json"
        else f"{reporter}/{volume}/html"
    )
