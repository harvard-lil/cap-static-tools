import os
import zipfile
import io
from invoke import task
from PyPDF2 import PdfReader, PdfWriter
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json
import tempfile

from .helpers import (
    r2_s3_client as production_s3_client,
    get_volumes_metadata,
    R2_STATIC_BUCKET,
    R2_UNREDACTED_BUCKET,
)


@task
def split_pdfs(ctx, reporter=None, publication_year=None, s3_client=None):
    """ Split PDFs into individual case files for all jurisdictions or a specific reporter. """
    print(
        f"Starting split_pdfs task for reporter: {reporter}, year: {publication_year}"
    )
    if s3_client is None:
        s3_client = production_s3_client

    volumes_to_process = get_volumes_to_process(reporter, publication_year, s3_client)
    print(f"Volumes to process: {volumes_to_process}")

    total_volumes = len(volumes_to_process)
    print(f"Total volumes to process: {total_volumes}")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [
            executor.submit(process_volume, volume, s3_client)
            for volume in volumes_to_process
        ]

        for future in tqdm(
            as_completed(futures), total=total_volumes, desc="Processing Volumes"
        ):
            try:
                result = future.result()
                print(f"Processed volume result: {result}")
            except Exception as e:
                print(f"Error processing volume: {e}")

    print(f"Processed {total_volumes} volumes.")


def get_volumes_to_process(
    reporter=None, publication_year=None, s3_client=None, r2_bucket=R2_STATIC_BUCKET
):
    volumes_metadata = json.loads(get_volumes_metadata(r2_bucket))

    if reporter:
        volumes_metadata = [
            v for v in volumes_metadata if v["reporter_slug"] == reporter
        ]
    if publication_year:
        volumes_metadata = [
            v
            for v in volumes_metadata
            if v.get("publication_year") == int(publication_year)
        ]

    return volumes_metadata


def get_cases_metadata(s3_client, bucket, volume):
    zip_key = f"{volume['reporter_slug']}/{volume['volume_number']}.zip"
    unzipped_key = (
        f"{volume['reporter_slug']}/{volume['volume_folder']}/CasesMetadata.json"
    )

    try:
        # Try to get metadata from zip file first
        response = s3_client.get_object(Bucket=bucket, Key=zip_key)
        with zipfile.ZipFile(io.BytesIO(response["Body"].read())) as zip_ref:
            file_list = zip_ref.namelist()
            metadata_file_name = next(
                (name for name in file_list if name.endswith("CasesMetadata.json")),
                None,
            )
            if metadata_file_name:
                with zip_ref.open(metadata_file_name) as metadata_file:
                    return json.load(metadata_file)
            else:
                print(f"CasesMetadata.json not found in zip file {zip_key}")
    except s3_client.exceptions.NoSuchKey:
        # If zip file doesn't exist, try unzipped file
        try:
            response = s3_client.get_object(Bucket=bucket, Key=unzipped_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception as e:
            print(f"Error getting cases metadata for {unzipped_key}: {str(e)}")
            return None
    except Exception as e:
        print(f"Error getting cases metadata from zip {zip_key}: {str(e)}")
        return None


def process_volume(volume, s3_client=production_s3_client):
    cases_metadata = get_cases_metadata(s3_client, R2_STATIC_BUCKET, volume)

    if not cases_metadata:
        print(f"Skipping volume {volume['volume_number']} due to missing metadata")
        return

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
        pdf_path = temp_file.name
        download_pdf(volume, pdf_path, s3_client)

    try:
        case_pdfs = split_pdf(pdf_path, cases_metadata)
        print(f"Split {len(case_pdfs)} case PDFs")
        upload_case_pdfs(case_pdfs, volume, s3_client)
        return f"Processed {len(case_pdfs)} cases for volume {volume['volume_number']}"
    except Exception as e:
        print(
            f"Error processing volume {volume['volume_number']} of {volume['reporter_slug']}: {str(e)}"
        )
        return f"Error processing volume {volume['volume_number']}: {str(e)}"
    finally:
        os.unlink(pdf_path)


def download_pdf(volume, local_path, s3_client=production_s3_client):
    key = f"{volume['reporter_slug']}/{volume['volume_folder']}/{volume['volume_number']}.pdf"
    try:
        s3_client.download_file(R2_STATIC_BUCKET, key, local_path)
    except Exception as e:
        print(
            f"Error downloading PDF for volume {volume['volume_number']} of {volume['reporter_slug']}: {str(e)}"
        )
        raise


def split_pdf(pdf_path, cases_metadata):
    reader = PdfReader(pdf_path)

    case_pdfs = []
    for case in cases_metadata:
        writer = PdfWriter()
        start_page = case["first_page_order"] - 1
        end_page = case["last_page_order"]

        for page_num in range(start_page, end_page):
            writer.add_page(reader.pages[page_num])

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_case_file:
            writer.write(temp_case_file)
            case_pdfs.append((case["file_name"], temp_case_file.name))

    return case_pdfs


def upload_case_pdfs(case_pdfs, volume, s3_client=production_s3_client):
    for case_name, case_path in case_pdfs:
        key = f"{volume['reporter_slug']}/{volume['volume_folder']}/case-pdfs/{case_name}.pdf"
        try:
            s3_client.upload_file(case_path, R2_STATIC_BUCKET, key)
            print(f"Uploaded {key} to {R2_STATIC_BUCKET}")
        except Exception as e:
            print(
                f"Error uploading case PDF {case_name} for volume {volume['volume_number']} of {volume['reporter_slug']}: {str(e)}"
            )
        finally:
            os.unlink(case_path)
