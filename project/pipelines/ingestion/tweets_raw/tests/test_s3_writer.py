from unittest.mock import MagicMock, patch, call
import json
import pytest

from project.pipelines.ingestion.tweets_raw.utils.s3_writer import (
    S3RawWriter,
)


@pytest.fixture
def writer():
    with patch(
        "project.pipelines.ingestion.tweets_raw.utils.s3_writer.boto3.client"
    ) as mock_client:
        w = S3RawWriter(
            endpoint_url="http://localhost:9000",
            bucket="tweets-backup",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        w._client = mock_client.return_value
        yield w


def test_flush_empty_buffer_returns_zero(writer):
    assert writer.flush() == 0
    writer._client.put_object.assert_not_called()


def test_write_batch_then_flush_uploads_ndjson(writer):
    tweets = [
        {"id": "1", "text": "PSG gagne", "lang": "fr"},
        {"id": "2", "text": "OM nul", "lang": "fr"},
    ]

    writer.write_batch(tweets)
    count = writer.flush()

    assert count == 2
    writer._client.put_object.assert_called_once()

    call_kwargs = writer._client.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "tweets-backup"
    assert call_kwargs["ContentType"] == "application/x-ndjson"

    lines = call_kwargs["Body"].decode("utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["id"] == "1"
    assert json.loads(lines[1])["id"] == "2"


def test_flush_clears_buffer(writer):
    writer.write_batch([{"id": "1", "text": "test"}])
    writer.flush()
    assert writer._buffer == []


def test_flush_clears_buffer_on_s3_error(writer):
    writer._client.put_object.side_effect = Exception("S3 unavailable")
    writer.write_batch([{"id": "1", "text": "test"}])

    count = writer.flush()

    assert count == 0
    assert writer._buffer == []


def test_flush_does_not_propagate_s3_error(writer):
    writer._client.put_object.side_effect = Exception("connexion refusée")
    writer.write_batch([{"id": "1", "text": "test"}])

    # Ne doit pas lever d'exception
    writer.flush()


def test_s3_key_has_correct_partition_format(writer):
    writer.write_batch([{"id": "1", "text": "test"}])
    writer.flush()

    key = writer._client.put_object.call_args.kwargs["Key"]
    assert "year=" in key
    assert "month=" in key
    assert "day=" in key
    assert key.endswith(".ndjson")


def test_write_batch_accumulates(writer):
    writer.write_batch([{"id": "1"}])
    writer.write_batch([{"id": "2"}, {"id": "3"}])
    assert len(writer._buffer) == 3
