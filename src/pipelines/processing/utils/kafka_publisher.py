"""Publish each enriched micro-batch to the silver Kafka topic.

Batch (not streaming) write inside ``foreachBatch``: the micro-batch is
already a static DataFrame at that point.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from project.pipelines.processing.utils.transform import to_kafka_frame

logger = logging.getLogger(__name__)


class KafkaEnrichedPublisher:
    """Callable sink writing a micro-batch to ``tweets_enriched``."""

    def __init__(self, brokers: str, topic: str) -> None:
        """Configure the publisher.

        Parameters
        ----------
        brokers : str
            Kafka bootstrap servers.
        topic : str
            Destination topic (the silver layer).
        """
        self._brokers = brokers
        self._topic = topic

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        """Write one micro-batch to Kafka.

        Parameters
        ----------
        batch_df : DataFrame
            The enriched micro-batch.
        batch_id : int
            Monotonic batch identifier provided by Spark.
        """
        (
            to_kafka_frame(batch_df)
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", self._brokers)
            .option("topic", self._topic)
            .save()
        )
        logger.info("Batch %s: published to %s", batch_id, self._topic)
