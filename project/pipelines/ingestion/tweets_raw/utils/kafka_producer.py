from __future__ import annotations

import logging
from typing import Any, Dict

from confluent_kafka import Producer

from .tweet_validator import (
    TweetValidator,
)
from .tweet_serializer import (
    TweetSerializer,
)
from .metrics import (
    tweets_sent, tweets_rejected, send_latency
)

from .key_builder import KeyBuilder

logger = logging.getLogger(__name__)

class TweetsRawProducer:
    """
    Kafka producer for raw tweets. It validates and serializes
     tweets before sending
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
            "acks": "1",
            "socket.timeout.ms": 10000,
            "message.timeout.ms": 10000
        })

    def _on_delivery(self, err, msg):
        if err:
            logger.error(f"ÉCHEC LIVRAISON: {err} — topic={msg.topic()} key={msg.key()}")
        else:
            logger.info(f"OK livré → {msg.topic()} [{msg.partition()}] offset={msg.offset()}")

    def send(self, tweet: Dict[str, Any]) -> None:
        """
        This function sends a tweet to the appropriate
        Kafka topic based on its validity.
        - if valid → topic tweets_raw
        - else → topic audit_logs with validation errors
        Args:
            tweet (Dict[str, Any]): The tweet to be sent.
        """
        try:
            validation = self._validator.validate(tweet)

            if not validation.is_valid:
                audit_event = {
                    "type": "VALIDATION_ERROR",
                    "source": "tweets_raw_producer",
                    "errors": validation.errors,
                    "raw_tweet": tweet,
                }
                audit_value = self._serializer.serialize(audit_event)
                if audit_value:
                    self._producer.produce(
                        topic=self._audit_topic,
                        value=audit_value,
                        key=b"validation_error"
                    )
                return

            # Tweet valide → tweets_raw
            key = self._key_builder.build(tweet)
            value = self._serializer.serialize(tweet)

            # DEBUG: log ce qu'on envoie
            logger.debug(
                f"Sending tweet {tweet.get('id')} → "
                f"topic={self._tweets_topic}, "
                f"key={key!r} (type: {type(key).__name__}), "
                f"value_len={len(value)} bytes"
            )

            # Vérifie que key et value sont valides
            if not value:
                logger.error(f"Serialize returned None for tweet {tweet.get('id')}")
                return
            if key is None:
                key = tweet.get("id", "").encode("utf-8")  # fallback
                logger.debug(f"Using fallback key: {key!r}")

            with send_latency.time():
                self._producer.produce(
                    topic=self._tweets_topic,
                    value=value,
                    key=key,
                    on_delivery=self._on_delivery
                )

            # Update metrics
            tweets_sent.labels(club=tweet.get("club", "unknown")).inc()
            for error_name in validation.errors:
                tweets_rejected.labels(reason=error_name).inc()
        except Exception as e:
            logger.error(f"Exception dans send(): {type(e).__name__}: {e}", exc_info=True)
            raise

    def flush(self, timeout: float = 10.0) -> int:
        """
        This function flushes waiting messages.
        """
        return self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """
        This function flushes and closes the producer.
        """
        self._producer.flush()
        self._producer.close()
