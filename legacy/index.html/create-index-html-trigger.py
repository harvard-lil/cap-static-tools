import json
import logging
import boto3
import os
import pandas as pd
from natsort import natsorted

logger = logging.getLogger()
logger.setLevel(logging.INFO)
lambda_client = boto3.client("lambda")

R2_STORAGE = os.getenv("R2_STORAGE")
R2_BUCKET = os.getenv("R2_BUCKET")
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
BASE_PATH = os.getenv("BASE_PATH")
BATCH_SIZE = 50


def lambda_handler(event, context):
    """
    Lambda entry point
    """
    volumes = json.loads(get_metadata("VolumesMetadata.json"))
    reporters = json.loads(get_metadata("ReportersMetadata.json"))
    first_level_html = create_first_level_html(reporters)
    second_level_df = create_second_level_df(volumes)
    upload_first_level_file(first_level_html)
    upload_second_level_files(second_level_df)

    # invoke the upload lambda that will create the third and fourth level index.html files
    # invoke in batches to avoid timeouts
    for volume in range(0, len(volumes), BATCH_SIZE):
        event_data = {"volumes": volumes[volume: volume + BATCH_SIZE]}
        lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-west-2:486926067183:function:create-index-html",
            InvocationType="Event",
            Payload=json.dumps(event_data),
        )

    logger.info("Invocation is complete.")


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


def get_metadata(metadata_type):
    """
    Gets metadata.json file contents
    """
    s3_client = create_r2_s3_client()
    response = s3_client.get_object(Bucket=R2_BUCKET, Key=metadata_type)
    return response["Body"].read().decode("utf-8")


def create_second_level_html(item):
    """
    Creates html for second level items
    """
    r2_reporter_files = get_reporter_files(item['reporter_slug'])

    volume_folders = [x for x in item["volume_folder"]]
    volume_folders = natsorted(volume_folders)
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += f"<ul><li><a href='{BASE_PATH}'>Home</a><li> / </li></li><li>{item['reporter_slug']}</li></ul>"
    html += "<table><tr><th>Volume</th><th>Zip</th><th>PDF</th><th>Tar</th><th>Tar.csv</th><th>Tar.sha256</th></tr>"

    for volume in volume_folders:
        html += f"<tr><td><a href='{BASE_PATH}{item['reporter_slug']}/{volume}/'>{volume}</a></td>"
        html += f"<td><a href='{BASE_PATH}{item['reporter_slug']}/{volume}.zip'>{volume}.zip</a></td>"
        html += create_artifacts_html(r2_reporter_files, item['reporter_slug'], volume)
        html += "</tr>"
    html += f"<tr><td><a href='{BASE_PATH}{item['reporter_slug']}/ReporterMetadata.json'>ReporterMetadata.json</a></td></tr>"
    html += f"<tr><td><a href='{BASE_PATH}{item['reporter_slug']}/VolumesMetadata.json'>VolumesMetadata.json</a></td></tr>"
    html += "</table>"

    return html


def create_second_level_df(data):
    """
    Creates dataframe for second level items
    """
    df = pd.DataFrame.from_dict(data)
    second_level_df = df.groupby(["reporter_slug"], as_index=False).agg(list)
    second_level_df['html'] = second_level_df.apply(lambda row: create_second_level_html(row), axis=1)
    return second_level_df


def create_first_level_html(reporters):
    """
    Creates html for first level items
    """
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += "<table><tr><th>Contents</th></tr>"

    for reporter in reporters:
        html += f"<tr><td><a href='{BASE_PATH}{reporter['slug']}/'>{reporter['slug']}</a></td>"
    html += f"<tr><td><a href='{BASE_PATH}ReportersMetadata.json'>ReportersMetadata.json</a></td>"
    html += f"<tr><td><a href='{BASE_PATH}VolumesMetadata.json'>VolumesMetadata.json</a></td>"
    html += f"<tr><td><a href='{BASE_PATH}JurisdictionsMetadata.json'>JurisdictionsMetadata.json</a></td>"
    html += "</table>"

    return html


def upload_first_level_file(html):
    """
    Uploads index.html file to R2
    """
    r2_s3_client = create_r2_s3_client()
    try:
        r2_s3_client.put_object(Bucket=R2_BUCKET, Key="index.html", Body=html, ContentType='text/html')
    except Exception as error:
        logging.error(error)


def upload_second_level_files(dataframe):
    """
    Uploads index.html files to R2
    """
    r2_s3_client = create_r2_s3_client()
    for index, row in dataframe.iterrows():
        try:
            r2_s3_client.put_object(Bucket=R2_BUCKET, Key=f"{row['reporter_slug']}/index.html", Body=row["html"],
                                    ContentType='text/html')
        except Exception as error:
            logging.error(f"{row['reporter_slug']}: {error}")


def create_artifacts_html(reporter_files, reporter, volume):
    """
    Creates html for the volume artifacts
    """
    html = ""

    if any(f"{reporter}/{volume}.pdf" in file for file in reporter_files):
        html += f"<td><a href='{BASE_PATH}{reporter}/{volume}.pdf'>{volume}.pdf</a></td>"
    else:
        html += "<td></td>"

    if any(f"{reporter}/{volume}.tar" in file for file in reporter_files):
        html += f"<td><a href='{BASE_PATH}{reporter}/{volume}.tar'>{volume}.tar</a></td>"
    else:
        html += "<td></td>"

    if any(f"{reporter}/{volume}.tar.csv" in file for file in reporter_files):
        html += f"<td><a href='{BASE_PATH}{reporter}/{volume}.tar.csv'>{volume}.tar.csv</a></td>"
    else:
        html += "<td></td>"

    if any(f"{reporter}/{volume}.tar.sha256" in file for file in reporter_files):
        html += f"<td><a href='{BASE_PATH}{reporter}/{volume}.tar.sha256'>{volume}.tar.sha256</a></td>"
    else:
        html += "<td></td>"

    return html


def get_reporter_files(reporter):
    """
    Gets all files of a reporter
    """
    r2_s3_client = create_r2_s3_client()
    paginator = r2_s3_client.get_paginator("list_objects_v2")
    files = []

    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{reporter}/", PaginationConfig={"PageSize": 1000}):
        for item in page["Contents"]:
            files.append(item["Key"])

    return files
