from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


class S3RawWriter:
    """
    Écrit les tweets enrichis dans un bucket S3/MinIO en NDJSON.

    Stratégie : accumule les tweets en mémoire pendant le run,
    puis flush un seul fichier NDJSON daté à la fin.
    Les erreurs S3 sont loguées mais ne propagent jamais —
    la collecte Kafka n'est jamais bloquée par un problème de stockage.

    Chemin S3 :
        {prefix}/year=YYYY/month=MM/day=dd/batch_TIMESTAMP_UUID.ndjson
    """

    def __init__(
        self,
        endpoint_url: str,
        bucket: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        prefix: str = "tweets_raw",
        region: str = "us-east-1",
    ) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._buffer: List[Dict[str, Any]] = []

        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
            config=Config(signature_version="s3v4"),
        )

    def write_batch(self, tweets: List[Dict[str, Any]]) -> None:
        """Ajoute une liste de tweets au buffer en mémoire."""
        self._buffer.extend(tweets)

    def flush(self) -> int:
        """
        Écrit le buffer dans S3 en NDJSON et vide le buffer.

        Returns le nombre de tweets écrits, 0 en cas d'erreur ou buffer vide.
        Ne propage jamais d'exception.
        """
        if not self._buffer:
            return 0

        count = len(self._buffer)
        try:
            now = datetime.now(tz=timezone.utc)
            key = (
                f"{self._prefix}/"
                f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
                f"batch_{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}.ndjson"
            )

            body = "\n".join(
                json.dumps(t, ensure_ascii=False) for t in self._buffer
            ).encode("utf-8")

            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType="application/x-ndjson",
            )

            logger.info(f"S3 flush OK: {count} tweets → s3://{self._bucket}/{key}")
            return count

        except Exception as e:
            logger.error(
                f"S3 flush échoué — Kafka non affecté "
                f"({type(e).__name__}: {e})"
            )
            return 0

        finally:
            self._buffer.clear()
