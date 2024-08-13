import json
import pandas as pd
from natsort import natsorted
from invoke import task
from datetime import datetime
import re
import pytz

from .helpers import (
    get_volumes_metadata,
    get_reporters_metadata,
    get_reporter_files,
    r2_paginator,
    r2_s3_client,
    R2_STATIC_BUCKET,
    CAP_STATIC_BASE_URL
)


@task
def create_html(ctx, level="root"):
    """
    Creates and uploads index.html pages to the static bucket.
    -- Level options --
    root: Creates ands upload the root level html (https://static.case.law/)
    reporter: Creates and uploads reporter level htmls (e.g.: https://static.case.law/a2d/)
    volume: Creates and uploads volume level htmls (e.g.:
    https://static.case.law/a2d/31/, https://static.case.law/a2d/31/html/ and https://static.case.law/a2d/31/cases/)
    """
    level_options = ["root", "reporter", "volume"]
    assert level in level_options, f"Value '{level}' is not a valid option"

    volumes = json.loads(get_volumes_metadata(R2_STATIC_BUCKET))

    if level == "root":
        reporters = json.loads(get_reporters_metadata(R2_STATIC_BUCKET))
        root_level_html = create_root_level_html(reporters)
        upload_root_level_file(root_level_html)

    if level == "reporter":
        reporter_level_df = create_reporter_level_df(volumes)
        upload_reporter_level_files(reporter_level_df)

    if level == "volume":
        volume_files = get_volume_files(volumes)
        volume_root_level_df, volume_cases_level_df = create_grouped_dataframe(volume_files)
        upload_volume_level_files(volume_root_level_df, 3)
        upload_volume_level_files(volume_cases_level_df, 4)


def create_root_level_html(reporters):
    """
    Creates html for root level items - reporters and metadata files
    """
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += "<table><tr><th>Contents</th></tr>"

    for reporter in reporters:
        html += f"<tr><td><a href='{CAP_STATIC_BASE_URL}{reporter['slug']}/'>{reporter['slug']}</a></td></tr>"
    html += f"<tr><td><a href='{CAP_STATIC_BASE_URL}ReportersMetadata.json'>ReportersMetadata.json</a></td></tr>"
    html += f"<tr><td><a href='{CAP_STATIC_BASE_URL}VolumesMetadata.json'>VolumesMetadata.json</a></td></tr>"
    html += (f"<tr><td><a href='{CAP_STATIC_BASE_URL}JurisdictionsMetadata.json'>"
             f"JurisdictionsMetadata.json</a></td></tr>")
    html += "</table>"

    return html


def upload_root_level_file(html):
    """
    Uploads index.html file to R2
    """
    try:
        r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Key="index.html", Body=html, ContentType='text/html')
    except Exception as error:
        print(error)


def create_reporter_level_html(item):
    """
    Creates html for reporter level items
    """
    r2_reporter_files = get_reporter_files(item['reporter_slug'])

    volume_folders = [x for x in item["volume_folder"]]
    volume_folders = natsorted(volume_folders)
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += f"<ul><li><a href='{CAP_STATIC_BASE_URL}'>Home</a></li><li> / </li><li>{item['reporter_slug']}</li></ul>"
    html += "<table><tr><th>Volume</th><th>Zip</th><th>PDF</th><th>Tar</th><th>Tar.csv</th><th>Tar.sha256</th></tr>"

    for volume in volume_folders:
        html += f"<tr><td><a href='{CAP_STATIC_BASE_URL}{item['reporter_slug']}/{volume}/'>{volume}</a></td>"
        html += f"<td><a href='{CAP_STATIC_BASE_URL}{item['reporter_slug']}/{volume}.zip'>{volume}.zip</a></td>"
        html += create_artifacts_html(r2_reporter_files, item['reporter_slug'], volume)
        html += "</tr>"
    html += (f"<tr><td><a href='{CAP_STATIC_BASE_URL}{item['reporter_slug']}/"
             f"ReporterMetadata.json'>ReporterMetadata.json</a></td></tr>")
    html += (f"<tr><td><a href='{CAP_STATIC_BASE_URL}{item['reporter_slug']}/"
             f"VolumesMetadata.json'>VolumesMetadata.json</a></td></tr>")
    html += "</table>"

    return html


def create_reporter_level_df(data):
    """
    Creates dataframe for reporter level items
    """
    df = pd.DataFrame.from_dict(data)
    reporter_level_df = df.groupby(["reporter_slug"], as_index=False).agg(list)
    reporter_level_df['html'] = reporter_level_df.apply(lambda row: create_reporter_level_html(row), axis=1)
    return reporter_level_df


def upload_reporter_level_files(dataframe):
    """
    Uploads index.html files to R2
    """
    for index, row in dataframe.iterrows():
        try:
            r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Key=f"{row['reporter_slug']}/index.html", Body=row["html"],
                                    ContentType='text/html')
        except Exception as error:
            print(f"{row['reporter_slug']}: {error}")


def create_artifacts_html(reporter_files, reporter, volume):
    """
    Creates html for the volume artifacts such as pdf and tar files
    """
    html = ""
    extensions = ["pdf", "tar", "tar.csv", "tar.sha256"]

    for ext in extensions:
        file_name = f"{reporter}/{volume}.{ext}"
        if any(file_name in file for file in reporter_files):
            html += f"<td><a href='{CAP_STATIC_BASE_URL}{file_name}'>{volume}.{ext}</a></td>"
        else:
            html += "<td></td>"

    return html


def create_volume_root_level_html(item):
    """
    Creates html for volume root level
    """
    item['file_location'] = list(dict.fromkeys(item['file_location']))
    # reverse the list to display cases and html paths first instead of metadata.json paths
    item['file_location'].reverse()
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += (f"<ul><li><a href='{CAP_STATIC_BASE_URL}'>Home</a></li><li> / </li><li><a href='{CAP_STATIC_BASE_URL}"
             f"{item['reporter']}/'>{item['reporter']}</a></li><li> / </li><li>{item['volume']}</li></ul>")
    html += "<table><tr><th>Contents</th></tr>"

    for location in item['file_location']:
        href = f"{CAP_STATIC_BASE_URL}{item['reporter']}/{item['volume']}/{location}"
        if location in ['cases', 'html']:
            href = f"{href}/"
        html += f"<tr><td><a href='{href}'>{location}</a></td></tr>"
    html += "</table>"

    return html


def create_volume_cases_level_html(item):
    """
    Creates html for volume cases level - cases and html folders
    """
    html = ("<style>table {width: 100%;}td, th {text-align: left;} th {padding-top: 5px; padding-bottom: 14px} ul {"
            "font-size: 1.50em; font-weight: bold; padding: 0} li {display: inline;}</style>")
    html += (f"<ul><li><a href='{CAP_STATIC_BASE_URL}'>Home</a></li><li> / </li><li><a href='{CAP_STATIC_BASE_URL}"
             f"{item['reporter']}/'>{item['reporter']}</a></li><li> / </li><li><a href='{CAP_STATIC_BASE_URL}"
             f"{item['reporter']}/{item['volume']}/'>{item['volume']}</a></li><li> / </li><li>{item['file_location']}"
             f"</li></ul>")
    html += "<table><tr><th>File</th><th>Size</th><th>Last Modified</th></tr>"

    for index, key in enumerate(item["key"]):
        file = re.split("/", key)[-1]
        html += f"<tr><td><a href='{CAP_STATIC_BASE_URL}{key}'>{file}</a></td>"
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
    files = []

    for volume in volumes:
        prefix = f"{volume["reporter_slug"]}/{volume["volume_folder"]}/"
        for page in r2_paginator.paginate(Bucket=R2_STATIC_BUCKET, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
            for item in page["Contents"]:
                # exclude /index.html as we don't want to display it among the volume files
                if "/index.html" not in item["Key"]:
                    files.append({"key": item["Key"], "file_size": f"{round(item["Size"] / 1024, 2)} KB",
                                  "last_modified": convert_time(item["LastModified"])})

    return files


def create_grouped_dataframe(files):
    """
    Creates new columns from the file paths
    Creates html column from the grouped rows
    """
    df = pd.DataFrame.from_dict(files)
    df.insert(0, "file_location", df.key.str.split("/").str[2])
    df.insert(0, "volume", df.key.str.split("/").str[1])
    df.insert(0, "reporter", df.key.str.split("/").str[0])

    volume_root_level_df = df.groupby(["reporter", "volume"], as_index=False).agg(list)
    volume_cases_level_df = df.groupby(["reporter", "volume", "file_location"], as_index=False).agg(list)
    volume_cases_level_df = volume_cases_level_df[
        (volume_cases_level_df['file_location'] == "cases") | (volume_cases_level_df['file_location'] == "html")]

    volume_root_level_df['html'] = volume_root_level_df.apply(lambda row: create_volume_root_level_html(row), axis=1)
    volume_cases_level_df['html'] = volume_cases_level_df.apply(lambda row: create_volume_cases_level_html(row), axis=1)

    return volume_root_level_df, volume_cases_level_df


def upload_volume_level_files(dataframe, level):
    """
    Uploads index.html files to R2
    """
    for index, row in dataframe.iterrows():
        key = f"{row['reporter']}/{row['volume']}/index.html"
        if level == 4:
            key = f"{row['reporter']}/{row['volume']}/{row['file_location']}/index.html"

        try:
            r2_s3_client.put_object(Bucket=R2_STATIC_BUCKET, Key=key, Body=row["html"], ContentType='text/html')
        except Exception as error:
            print(f"{key}: {error}")
