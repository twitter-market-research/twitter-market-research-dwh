"""Tests for AuditLogProducer: ships log lines to the audit_logs topic.

Encodes the contract:
- a structured line is parsed (ts / level / logger / message);
- an unrecognized line is still shipped (level UNPARSED);
- every message carries the audit_type=app_log header (#10);
- blank lines are ignored.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from project.pipelines.ingestion.audit_logs.utils.log_producer import (
    AuditLogProducer,
)

MODULE = "project.pipelines.ingestion.audit_logs.utils.log_producer"


@pytest.fixture
def producer_and_mock():
    with patch(f"{MODULE}.Producer") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer
        producer = AuditLogProducer(brokers="broker-1:29092")
        yield producer, mock_producer


class TestAuditLogProducer:

    def test_structured_line_is_parsed(self, producer_and_mock):
        producer, mock_producer = producer_and_mock

        producer.ship("2026-08-12 15:13:52 [ERROR] api_extractor: budget exceeded")

        mock_producer.produce.assert_called_once()
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["topic"] == "audit_logs"
        event = json.loads(kwargs["value"].decode("utf-8"))
        assert event["level"] == "ERROR"
        assert event["logger"] == "api_extractor"
        assert event["message"] == "budget exceeded"
        assert event["ts"] == "2026-08-12 15:13:52"

    def test_ships_with_app_log_header(self, producer_and_mock):
        producer, mock_producer = producer_and_mock

        producer.ship("2026-08-12 15:13:52 [INFO] x: y")

        _, kwargs = mock_producer.produce.call_args
        assert ("audit_type", b"app_log") in kwargs["headers"]

    def test_unparsed_line_still_shipped(self, producer_and_mock):
        producer, mock_producer = producer_and_mock

        producer.ship("    at java.base/... (stack trace line)")

        mock_producer.produce.assert_called_once()
        _, kwargs = mock_producer.produce.call_args
        event = json.loads(kwargs["value"].decode("utf-8"))
        assert event["level"] == "UNPARSED"
        assert "stack trace" in event["message"]

    def test_empty_line_is_ignored(self, producer_and_mock):
        producer, mock_producer = producer_and_mock

        producer.ship("   \n")

        mock_producer.produce.assert_not_called()

    def test_value_is_json_bytes(self, producer_and_mock):
        producer, mock_producer = producer_and_mock

        producer.ship("2026-08-12 15:13:52 [INFO] a: b")

        _, kwargs = mock_producer.produce.call_args
        assert isinstance(kwargs["value"], bytes)
        json.loads(kwargs["value"])  # does not raise
