import json
from datetime import datetime
import pytest
from unittest.mock import Mock, patch, ANY
from invoke import Context
from tasks.unredact import add_last_updated_field


@pytest.fixture
def mock_context():
    return Mock(spec=Context)


@pytest.fixture
def sample_volumes_metadata():
    return [
        {
            "volume_number": "1",
            "id": "32044078646858",
            "redacted": False,
            "reporter_slug": "ad",
        },
        {
            "volume_number": "2",
            "id": "32044078646940",
            "redacted": False,
            "reporter_slug": "ad",
            "last_updated": "2024-01-01T00:00:00+00:00",
        },
    ]


@pytest.fixture
def sample_reporter_metadata():
    return [
        {"volume_number": "1", "id": "32044078646858", "redacted": False},
        {
            "volume_number": "2",
            "id": "32044078646940",
            "redacted": False,
            "last_updated": "2024-01-01T00:00:00+00:00",
        },
    ]


@patch("tasks.unredact.get_volumes_metadata")
@patch("tasks.unredact.get_reporter_volumes_metadata")
@patch("tasks.unredact.r2_s3_client")
def test_add_last_updated_field_dry_run(
    mock_r2_client,
    mock_get_reporter,
    mock_get_volumes,
    mock_context,
    sample_volumes_metadata,
    sample_reporter_metadata,
    capsys,
):
    mock_get_volumes.return_value = json.dumps(sample_volumes_metadata)
    mock_get_reporter.return_value = json.dumps(sample_reporter_metadata)

    add_last_updated_field(mock_context, dry_run=True)

    captured = capsys.readouterr()

    mock_r2_client.put_object.assert_not_called()

    assert "Would update 1 volumes in main VolumesMetadata.json" in captured.out
    assert "Would update 1 volumes in ad/VolumesMetadata.json" in captured.out


@patch("tasks.unredact.get_volumes_metadata")
@patch("tasks.unredact.get_reporter_volumes_metadata")
@patch("tasks.unredact.r2_s3_client")
def test_add_last_updated_field_actual_update(
    mock_r2_client,
    mock_get_reporter,
    mock_get_volumes,
    mock_context,
    sample_volumes_metadata,
    sample_reporter_metadata,
):
    # Setup mocks
    mock_get_volumes.return_value = json.dumps(sample_volumes_metadata)
    mock_get_reporter.return_value = json.dumps(sample_reporter_metadata)

    add_last_updated_field(mock_context, dry_run=False)

    mock_r2_client.put_object.assert_any_call(
        Bucket=ANY,
        Body=ANY,
        Key="VolumesMetadata.json",
        ContentType="application/json",
    )

    mock_r2_client.put_object.assert_any_call(
        Bucket=ANY,
        Body=ANY,
        Key="ad/VolumesMetadata.json",
        ContentType="application/json",
    )

    calls = mock_r2_client.put_object.call_args_list
    for call in calls:
        body = json.loads(call.kwargs["Body"])
        for volume in body:
            # Verify all volumes have last_updated
            assert "last_updated" in volume

            # Verify existing data is preserved
            if volume["id"] == "32044078646858":
                assert volume["volume_number"] == "1"
                assert volume["redacted"] is False
                if call.kwargs["Key"] == "VolumesMetadata.json":
                    assert volume["reporter_slug"] == "ad"
            elif volume["id"] == "32044078646940":
                assert volume["volume_number"] == "2"
                assert volume["last_updated"] == "2024-01-01T00:00:00+00:00"
                assert volume["redacted"] is False
                if call.kwargs["Key"] == "VolumesMetadata.json":
                    assert volume["reporter_slug"] == "ad"


@patch("tasks.unredact.get_volumes_metadata")
@patch("tasks.unredact.get_reporter_volumes_metadata")
@patch("tasks.unredact.r2_s3_client")
def test_add_last_updated_field_handles_errors(
    mock_r2_client,
    mock_get_reporter,
    mock_get_volumes,
    mock_context,
    sample_volumes_metadata,
    capsys,
):
    mock_get_volumes.return_value = json.dumps(sample_volumes_metadata)
    mock_get_reporter.side_effect = Exception("Failed to get reporter metadata")

    add_last_updated_field(mock_context, dry_run=False)

    captured = capsys.readouterr()

    mock_r2_client.put_object.assert_called_once_with(
        Bucket=ANY,
        Body=ANY,
        Key="VolumesMetadata.json",
        ContentType="application/json",
    )

    assert "Error processing ad: Failed to get reporter metadata" in captured.out
