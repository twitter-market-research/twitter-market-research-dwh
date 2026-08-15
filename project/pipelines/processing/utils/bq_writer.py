"""foreachBatch sink: write each micro-batch to BigQuery.

Dev targets the BigQuery *emulator* (goccy) through the Python client with a
custom ``api_endpoint`` and anonymous credentials; prod points at real
BigQuery. Micro-batches are tiny in dev, so we collect the batch on the driver
and stream-insert the rows. For production scale this should move to the native
Spark BigQuery connector / Storage Write API.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class BigQueryBatchSink:
    """Callable sink for ``DataFrame.writeStream.foreachBatch``.

    The BigQuery client is created lazily on first use so this object stays
    lightweight and picklable when Spark ships it around.
    """

    def __init__(
        self,
        table: str,
        project: str,
        api_endpoint: Optional[str] = None,
    ) -> None:
        """Configure the sink.

        Parameters
        ----------
        table : str
            Fully-qualified table id ``project.dataset.table``.
        project : str
            GCP project id (any string works against the emulator).
        api_endpoint : Optional[str]
            Emulator endpoint (e.g. ``http://bq-emulator:9050``). When set the
            client uses anonymous credentials. Leave ``None`` for real BigQuery.
        """
        self._table = table
        self._project = project
        self._api_endpoint = api_endpoint
        self._client = None

    def _get_client(self):
        """Build (once) and return the BigQuery client."""
        if self._client is not None:
            return self._client
        from google.cloud import bigquery

        if self._api_endpoint:
            from google.auth.credentials import AnonymousCredentials

            self._client = bigquery.Client(
                project=self._project,
                credentials=AnonymousCredentials(),
                client_options={"api_endpoint": self._api_endpoint},
            )
        else:
            self._client = bigquery.Client(project=self._project)
        return self._client

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        """Insert one micro-batch into BigQuery.

        Parameters
        ----------
        batch_df : DataFrame
            The micro-batch produced by Structured Streaming.
        batch_id : int
            Monotonic batch identifier provided by Spark.
        """
        rows = [json.loads(row) for row in batch_df.toJSON().collect()]
        if not rows:
            return
        client = self._get_client()
        errors = client.insert_rows_json(self._table, rows)
        if errors:
            logger.error(
                "BigQuery insert errors (batch %s): %s", batch_id, errors
            )
            raise RuntimeError(f"BigQuery insert failed: {errors}")
        logger.info(
            "Batch %s: inserted %d rows into %s",
            batch_id, len(rows), self._table,
        )
