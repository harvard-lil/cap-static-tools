import pytest
import boto3
from moto import mock_aws
from invoke.context import MockContext
import invoke.runners
from unittest.mock import patch
import json
import os
import io
import zipfile

from tasks.split_pdfs import split_pdfs
from tasks.helpers import R2_STATIC_BUCKET


def print_bucket_contents(s3_client, bucket_name):
    print(f"Contents of {bucket_name}:")
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        for obj in response["Contents"]:
            print(f"- {obj['Key']}")
    else:
        print("Bucket is empty")
    print()


def upload_directory(s3_client, bucket, local_path, prefix=""):
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_path)
            s3_key = os.path.join(prefix, relative_path).replace("\\", "/")
            s3_client.upload_file(local_file_path, bucket, s3_key)


@mock_aws
@patch("tasks.helpers.get_volumes_metadata")
@patch("tasks.split_pdfs.get_volumes_to_process")
def test_split_pdfs(mock_get_volumes_to_process, mock_get_volumes_metadata, s3_client):
    # Path to test data
    test_data_path = os.path.join(os.path.dirname(__file__), "test_data")

    with open(os.path.join(test_data_path, "VolumesMetadata.json"), "r") as f:
        volumes_metadata = json.load(f)

    test_volume = next(
        v
        for v in volumes_metadata
        if v["reporter_slug"] == "a2d" and v["volume_number"] == "100"
    )

    mock_get_volumes_metadata.return_value = json.dumps(volumes_metadata)
    mock_get_volumes_to_process.return_value = [test_volume]

    # Create mock bucket
    s3_client.create_bucket(Bucket=R2_STATIC_BUCKET)

    # Test if mock S3 client is working
    s3_client.put_object(
        Bucket=R2_STATIC_BUCKET, Key="test_object", Body="test content"
    )
    test_objects = s3_client.list_objects_v2(Bucket=R2_STATIC_BUCKET)
    assert "Contents" in test_objects, "Mock S3 client is not working as expected"
    s3_client.delete_object(Bucket=R2_STATIC_BUCKET, Key="test_object")

    # Upload the entire test data directory to the mock S3 static bucket
    upload_directory(s3_client, R2_STATIC_BUCKET, test_data_path)

    # Verify the contents of the mock S3 bucket
    print("Initial bucket contents:")
    print_bucket_contents(s3_client, R2_STATIC_BUCKET)

    # Print contents of the zip file
    zip_key = "a2d/100.zip"
    response = s3_client.get_object(Bucket=R2_UNREDACTED_BUCKET, Key=zip_key)
    with zipfile.ZipFile(io.BytesIO(response["Body"].read())) as zip_ref:
        print(f"Contents of {zip_key}:")
        for file in zip_ref.namelist():
            print(f"- {file}")

    # Choose a specific reporter and volume from your test data
    reporter = "a2d"
    volume = "100"

    # Create a MockContext with predetermined run results
    ctx = MockContext(
        run={
            "some_command": invoke.runners.Result(
                stdout="mocked output", stderr="", exited=0
            )
        }
    )

    # Run the split_pdfs function
    try:
        split_pdfs(ctx, reporter=reporter, s3_client=s3_client)
    except Exception as e:
        print(f"Error in split_pdfs: {e}")
        import traceback

        traceback.print_exc()
        raise

    # Print the contents of the bucket after running split_pdfs
    print("\nBucket contents after running split_pdfs:")
    print_bucket_contents(s3_client, R2_STATIC_BUCKET)

    # Check if case PDFs were created in the static bucket
    objects = s3_client.list_objects_v2(Bucket=R2_STATIC_BUCKET)
    print(f"Objects in R2_STATIC_BUCKET: {objects}")

    assert "Contents" in objects, "No objects found in static bucket"

    case_pdf_keys = [
        obj["Key"] for obj in objects["Contents"] if "case-pdfs" in obj["Key"]
    ]

    assert len(case_pdf_keys) > 0, f"No case PDFs found"
    assert all(
        key.startswith(f"{reporter}/{volume}/case-pdfs/") for key in case_pdf_keys
    ), "Unexpected case PDF location"

    # Print some details about the case PDFs
    print("\nCase PDFs created:")
    for key in case_pdf_keys:
        print(f"- {key}")

    if case_pdf_keys:
        sample_key = case_pdf_keys[0]
        response = s3_client.get_object(Bucket=R2_STATIC_BUCKET, Key=sample_key)
        content = response["Body"].read()
        print(f"\nContent of {sample_key}:")
        print(content[:100])

    print("\nTest completed successfully")


if __name__ == "__main__":
    import sys
    import os

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pytest.main([__file__, "-s"])
