from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Producer

from .tweet_validator import (
    TweetValidator,
)
from .tweet_serializer import (
    TweetSerializer,
)
from .metrics import (
    tweets_sent,
    tweets_rejected,
    send_latency,
    delivery_failed,
)

from .key_builder import KeyBuilder

logger = logging.getLogger(__name__)

# Fenêtre totale de livraison. Doit rester large : une valeur trop faible
# (ex: 10s) transforme la moindre indisponibilité broker en perte silencieuse.
DELIVERY_TIMEOUT_MS = 120000


class TweetsRawProducer:
    """
    Kafka producer for raw tweets. It validates and serializes
     tweets before sending.

    Garanties de livraison (at-least-once) :
    - ``enable.idempotence`` active la déduplication broker et force
      ``acks=all`` + ``max.in.flight<=5`` + retries → pas de doublon ni de
      réordonnancement sur retry.
    - ``delivery.timeout.ms`` large pour ne pas dropper sur indispo passagère.
    """

    def __init__(
        self,
        brokers: str,
        tweets_topic: str,
        audit_topic: str,
    ) -> None:
        self._tweets_topic = tweets_topic
        self._audit_topic = audit_topic

        self._validator = TweetValidator()
        self._serializer = TweetSerializer()
        self._key_builder = KeyBuilder()

        self._producer = Producer({
            "bootstrap.servers": brokers,
            # --- durabilité / at-least-once ---
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": DELIVERY_TIMEOUT_MS,
            # --- débit ---
            "linger.ms": 20,
            "compression.type": "zstd",
        })

    def _on_delivery(self, err, msg) -> None:
        if err:
            logger.error(
                "ÉCHEC LIVRAISON: %s — topic=%s key=%s",
                err, msg.topic(), msg.key(),
            )
            delivery_failed.labels(topic=msg.topic()).inc()
        else:
            logger.debug(
                "OK livré → %s [%s] offset=%s",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def _produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes],
    ) -> None:
        """Produit un message avec gestion de la back-pressure (#15).

        Si la file locale de librdkafka est pleine, ``produce()`` lève
        ``BufferError``. On draine la file via ``poll()`` puis on réessaie une
        fois, au lieu de perdre le message.
        """
        try:
            self._producer.produce(
                topic=topic,
                value=value,
                key=key,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            logger.warning(
                "File producer pleine (topic=%s) — poll() puis retry", topic,
            )
            self._producer.poll(1)
            self._producer.produce(
                topic=topic,
                value=value,
                key=key,
                on_delivery=self._on_delivery,
            )

    def send(self, tweet: Dict[str, Any]) -> None:
        """
        This function sends a tweet to the appropriate
        Kafka topic based on its validity.
        - if valid → topic tweets_raw
        - else → topic audit_logs with validation errors

        Une exception sur un tweet est isolée (loggée) et n'est PAS propagée,
        pour ne pas avorter le batch complet en amont (#14).

        Args:
            tweet (Dict[str, Any]): The tweet to be sent.
        """
        try:
            validation = self._validator.validate(tweet)

            if not validation.is_valid:
                self._send_to_audit(tweet, validation.errors)
                for reason in validation.errors:
                    tweets_rejected.labels(reason=reason).inc()
                return

            # Tweet valide → tweets_raw
            key = self._key_builder.build(tweet)
            value = self._serializer.serialize(tweet)

            if not value:
                logger.error(
                    "Serialize returned None for tweet %s", tweet.get("id"),
                )
                return
            if key is None:
                key = str(tweet.get("id", "")).encode("utf-8")  # fallback

            with send_latency.time():
                self._produce(self._tweets_topic, value, key)

            tweets_sent.labels(club=tweet.get("club", "unknown")).inc()
        except Exception as e:
            # Isolation par record : on log et on continue (#14).
            logger.error(
                "Exception dans send() pour tweet %s: %s: %s",
                tweet.get("id"), type(e).__name__, e, exc_info=True,
            )

    def _send_to_audit(
        self,
        tweet: Dict[str, Any],
        errors: list,
    ) -> None:
        """Route un tweet invalide vers le topic d'audit avec ses erreurs."""
        audit_event = {
            "type": "VALIDATION_ERROR",
            "source": "tweets_raw_producer",
            "errors": errors,
            "raw_tweet": tweet,
        }
        # L'événement d'audit n'est pas un tweet : sérialisation JSON directe,
        # pas via le TweetSerializer (qui exige un schéma tweet).
        audit_value = json.dumps(
            audit_event, ensure_ascii=False, default=str,
        ).encode("utf-8")
        self._produce(
            self._audit_topic,
            audit_value,
            key=b"validation_error",
        )

    def flush(self, timeout: float = 10.0) -> int:
        """
        This function flushes waiting messages.

        Returns
        -------
        int
            Nombre de messages encore en file après le timeout (0 = tout livré).
        """
        return self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """
        This function flushes pending messages before shutdown.

        ``confluent_kafka.Producer`` n'expose pas de ``.close()`` : le flush
        (bloquant jusqu'à livraison) suffit à ne rien perdre.
        """
        self._producer.flush()
