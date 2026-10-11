#!/usr/bin/env python3
"""Entry point: tail the ingestion log file and ship each line to audit_logs.

Configuration comes from the environment:
- KAFKA_BROKERS  : bootstrap servers (default localhost:9092)
- TOPIC_NAME     : destination topic (default audit_logs)
- LOG_FILE       : log file to follow (default /app/logs/ingestion.log)
- LOG_FROM_START : "true" to ship existing content, else only new lines
"""
import logging
import os
import signal
import sys
import time
from typing import Iterator

from utils.log_producer import AuditLogProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("audit_logs.extract")

_running = True


def _handle_signal(_signum, _frame) -> None:
    """Flip the shutdown flag so the follow loop can exit cleanly."""
    global _running
    _running = False
    logger.info("Shutdown signal received, stopping...")


def follow(
    path: str,
    from_start: bool,
    poll_interval: float = 0.5,
) -> Iterator[str]:
    """Yield lines from a growing file, ``tail -f`` style.

    Waits for the file to appear, then yields appended lines. Honors the
    module-level shutdown flag so the loop exits on SIGTERM/SIGINT.

    Parameters
    ----------
    path : str
        Path of the log file to follow.
    from_start : bool
        If True, yield existing content first; otherwise start at EOF.
    poll_interval : float
        Seconds to wait when no new line is available.

    Yields
    ------
    str
        The next log line (including its trailing newline).
    """
    while _running and not os.path.exists(path):
        logger.info("Waiting for log file %s ...", path)
        time.sleep(poll_interval)
    if not _running:
        return

    with open(path, "r", encoding="utf-8") as handle:
        if not from_start:
            handle.seek(0, os.SEEK_END)
        while _running:
            line = handle.readline()
            if line:
                yield line
            else:
                time.sleep(poll_interval)


def main() -> int:
    """Run the audit-log shipper until a shutdown signal is received.

    Returns
    -------
    int
        Process exit code (0 on clean shutdown).
    """
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:9092")
    topic = os.environ.get("TOPIC_NAME", "audit_logs")
    log_file = os.environ.get("LOG_FILE", "/app/logs/ingestion.log")
    from_start = os.environ.get("LOG_FROM_START", "false").lower() == "true"

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "Audit log shipper starting: file=%s topic=%s brokers=%s",
        log_file, topic, brokers,
    )
    producer = AuditLogProducer(brokers=brokers, topic=topic)
    shipped = 0
    try:
        for line in follow(log_file, from_start):
            producer.ship(line)
            shipped += 1
    finally:
        remaining = producer.flush(timeout=15)
        logger.info(
            "Shipper stopped: %d lines shipped, %d unflushed",
            shipped, remaining,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
