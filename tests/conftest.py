import os
import pytest
import boto3
from moto import mock_aws

from tasks.helpers import R2_UNREDACTED_BUCKET, R2_STATIC_BUCKET


def upload_directory(s3_client, bucket, path, prefix=""):
    for root, dirs, files in os.walk(path):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, path)
            s3_path = os.path.join(prefix, relative_path).replace("\\", "/")
            with open(local_path, "rb") as file:
                s3_client.put_object(Bucket=bucket, Key=s3_path, Body=file.read())


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")

        # Create the necessary buckets
        s3.create_bucket(Bucket=R2_UNREDACTED_BUCKET)
        s3.create_bucket(Bucket=R2_STATIC_BUCKET)

        # Upload test data to the unredacted bucket
        test_data_path = os.path.join(os.path.dirname(__file__), "test_data")
        upload_directory(s3, R2_UNREDACTED_BUCKET, test_data_path)

        yield s3
