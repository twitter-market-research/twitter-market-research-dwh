"""Fan one micro-batch out to two sinks under a single checkpoint."""
from __future__ import annotations

import logging
from typing import Callable

logger = logging.getLogger(__name__)

BatchSink = Callable[[object, int], None]


class DualSink:
    """foreachBatch sink writing to Kafka first, then BigQuery.

    Caches the batch: without it Spark recomputes the whole plan for the
    second write, re-running the tagging UDF and re-reading the source.

    Kafka goes first on purpose: it is the replayable silver log, so if
    BigQuery fails the data is still recoverable from the topic. Neither
    write is transactional -- a retried batch lands twice in both, which
    is why the silver topic is keyed on ``tweet_id``.
    """

    def __init__(self, kafka_sink: BatchSink, bq_sink: BatchSink) -> None:
        """Store both destinations.

        Parameters
        ----------
        kafka_sink : BatchSink
            Called first with ``(batch_df, batch_id)``.
        bq_sink : BatchSink
            Called second with the same arguments.
        """
        self._kafka_sink = kafka_sink
        self._bq_sink = bq_sink

    def __call__(self, batch_df, batch_id: int) -> None:
        """Write the micro-batch to both destinations.

        Parameters
        ----------
        batch_df : DataFrame
            The enriched micro-batch.
        batch_id : int
            Monotonic batch identifier provided by Spark.
        """
        batch_df.persist()
        try:
            self._kafka_sink(batch_df, batch_id)
            self._bq_sink(batch_df, batch_id)
        finally:
            batch_df.unpersist()
