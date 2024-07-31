import logging
import boto3
import os
import pandas as pd
import re
from datetime import datetime
import pytz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

R2_STORAGE = os.getenv("R2_STORAGE")
R2_BUCKET = os.getenv("R2_BUCKET")
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
BASE_PATH = os.getenv("BASE_PATH")


def lambda_handler(event, context):
    """
    Lambda entry point
    """
    volumes = event["volumes"]
    volume_files = get_volume_files(volumes)
    third_level_df, fourth_level_df = create_grouped_dataframe(volume_files)
    upload_files(third_level_df, 3)
    upload_files(fourth_level_df, 4)
    logger.info("Index.html upload is complete.")


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


def create_third_level_html(item):
    """
    Creates html for third folder level
    """
    item['file_location'] = list(dict.fromkeys(item['file_location']))
    # reverse the list to display cases and html paths first instead of metadata.json paths
    item['file_location'].reverse()
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += f"<ul><li><a href='{BASE_PATH}'>Home</a><li> / </li></li><li><a href='{BASE_PATH}{item['reporter']}/'>{item['reporter']}</a></li><li> / </li><li>{item['volume']}</li></ul>"
    html += "<table><tr><th>Contents</th></tr>"

    for location in item['file_location']:
        href = f"{BASE_PATH}{item['reporter']}/{item['volume']}/{location}"
        if location in ['cases', 'html']:
            href = f"{href}/"
        html += f"<tr><td><a href='{href}'>{location}</a></td>"
    html += "</table>"

    return html


def create_fourth_level_html(item):
    """
    Creates html for fourth folder level
    """
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += f"<ul><li><a href='{BASE_PATH}'>Home</a><li> / </li></li><li><a href='{BASE_PATH}{item['reporter']}/'>{item['reporter']}</a></li><li> / </li><li><a href='{BASE_PATH}{item['reporter']}/{item['volume']}/'>{item['volume']}</a></li><li> / </li><li>{item['file_location']}</li></ul>"
    html += "<table><tr><th>File</th><th>Size</th><th>Last Modified</th></tr>"

    for index, key in enumerate(item["key"]):
        file = re.split("/", key)[-1]
        html += f"<tr><td><a href='{BASE_PATH}{key}'>{file}</a></td>"
        html += f"<td>{item['file_size'][index]}</td>"
        html += f"<td>{item['last_modified'][index]}</td></tr>"
    html += "</table>"

    return html


def convert_time(time_obj):
    """
    Converts the s3 datetime object to EST time
    """
    utc_datetime = time_obj.strftime("%m/%d/%Y %H:%M:%S")
    parsed_datetime = datetime.strptime(utc_datetime, "%m/%d/%Y %H:%M:%S")
    localized_datetime = pytz.utc.localize(parsed_datetime)
    est_datetime = localized_datetime.astimezone(pytz.timezone('America/New_York'))

    return est_datetime.strftime("%m/%d/%Y %H:%M:%S")


def get_volume_files(volumes):
    """
    Gets file names from R2
    """
    r2_s3_client = create_r2_s3_client()
    paginator = r2_s3_client.get_paginator("list_objects_v2")
    files = []

    for volume in volumes:
        prefix = f"{volume["reporter_slug"]}/{volume["volume_folder"]}/"
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
            for item in page["Contents"]:
                # exclude /index.html as we don't want to display it among the volume files
                if "/index.html" not in item["Key"]:
                    files.append({"key": item["Key"], "file_size": f"{round(item["Size"] / 1024, 2)} KB",
                                  "last_modified": convert_time(item["LastModified"])})

    return files


def create_grouped_dataframe(data):
    """
    Creates new columns from the file paths
    Creates an html column from the grouped rows
    """
    df = pd.DataFrame.from_dict(data)
    df.insert(0, "file_location", df.key.str.split("/").str[2])
    df.insert(0, "volume", df.key.str.split("/").str[1])
    df.insert(0, "reporter", df.key.str.split("/").str[0])

    third_level_df = df.groupby(["reporter", "volume"], as_index=False).agg(list)
    fourth_level_df = df.groupby(["reporter", "volume", "file_location"], as_index=False).agg(list)
    fourth_level_df = fourth_level_df[
        (fourth_level_df['file_location'] == "cases") | (fourth_level_df['file_location'] == "html")]

    third_level_df['html'] = third_level_df.apply(lambda row: create_third_level_html(row), axis=1)
    fourth_level_df['html'] = fourth_level_df.apply(lambda row: create_fourth_level_html(row), axis=1)

    return third_level_df, fourth_level_df


def upload_files(dataframe, level):
    """
    Uploads index.html files to R2
    """
    r2_s3_client = create_r2_s3_client()
    for index, row in dataframe.iterrows():
        key = f"{row['reporter']}/{row['volume']}/index.html"
        if level == 4:
            key = f"{row['reporter']}/{row['volume']}/{row['file_location']}/index.html"

        try:
            r2_s3_client.put_object(Bucket=R2_BUCKET, Key=key, Body=row["html"], ContentType='text/html')
        except Exception as error:
            logging.error(f"{key}: {error}")
