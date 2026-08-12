from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

DELIVERY_TIMEOUT_MS = 120000

# Tag used so consumers can tell application logs apart from the other
# audit_logs producers (validation rejects, connector DLQ) without
# deserializing the value (#10).
AUDIT_TYPE_HEADER = "audit_type"
AUDIT_TYPE_APP_LOG = b"app_log"

# Matches the ingestion pipeline log format:
#   "2026-08-12 15:13:52 [ERROR] api_extractor: some message"
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) "
    r"\[(?P<level>\w+)\] "
    r"(?P<logger>[\w.]+): "
    r"(?P<message>.*)$"
)


class AuditLogProducer:
    """Ship application log lines to the ``audit_logs`` Kafka topic.

    Each line is parsed into a structured event and produced with an
    ``audit_type=app_log`` header so downstream consumers can route it apart
    from validation rejects and the S3 connector DLQ that share the topic.
    """

    def __init__(self, brokers: str, topic: str = "audit_logs") -> None:
        """Create the producer.

        Parameters
        ----------
        brokers : str
            Bootstrap servers (e.g. ``"broker-1:29092,..."``).
        topic : str
            Destination topic (defaults to ``audit_logs``).
        """
        self._topic = topic
        self._producer = Producer({
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": DELIVERY_TIMEOUT_MS,
            "linger.ms": 50,
            "compression.type": "zstd",
        })

    def _parse(self, line: str) -> Dict[str, Any]:
        """Turn a raw log line into a structured event.

        Parameters
        ----------
        line : str
            A single log line (trailing newline tolerated).

        Returns
        -------
        Dict[str, Any]
            Structured event. Lines that do not match the expected format
            are still returned with ``level="UNPARSED"`` and the raw text as
            ``message`` (e.g. stack-trace continuation lines).
        """
        stripped = line.rstrip("\n")
        match = _LINE_RE.match(stripped)
        if match:
            return {"type": "APP_LOG", **match.groupdict()}
        return {
            "type": "APP_LOG",
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "UNPARSED",
            "logger": "unknown",
            "message": stripped,
        }

    def ship(self, line: str) -> None:
        """Parse a log line and produce it to the audit topic.

        Blank lines are ignored. The value is UTF-8 JSON; the message carries
        the ``audit_type=app_log`` header.

        Parameters
        ----------
        line : str
            The log line to ship.
        """
        if not line.strip():
            return
        event = self._parse(line)
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")
        self._producer.produce(
            topic=self._topic,
            value=value,
            headers=[(AUDIT_TYPE_HEADER, AUDIT_TYPE_APP_LOG)],
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages.

        Parameters
        ----------
        timeout : float
            Max seconds to wait.

        Returns
        -------
        int
            Number of messages still queued after the timeout (0 = all sent).
        """
        return self._producer.flush(timeout=timeout)
