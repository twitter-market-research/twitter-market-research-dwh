"""Tests du TweetsRawProducer (API confluent_kafka).

Ces tests encodent les garanties de livraison attendues :
- #1/#2/#3 : configuration at-least-once (idempotence, acks=all, delivery.timeout sain)
- #4       : close() flush sans appeler un .close() inexistant
- #5       : métriques sent/rejected incrémentées sur le bon chemin
- #14      : un tweet en erreur n'avorte pas le batch (isolation par record)
- #15      : file producer pleine → poll() puis retry, pas de perte
"""
from unittest.mock import MagicMock, patch

import pytest

from project.pipelines.ingestion.tweets_raw.utils.kafka_producer import (
    TweetsRawProducer,
)

MODULE = "project.pipelines.ingestion.tweets_raw.utils.kafka_producer"


@pytest.fixture
def producer_and_mock():
    """Instancie un TweetsRawProducer avec un Producer confluent_kafka mocké."""
    with patch(f"{MODULE}.Producer") as mock_cls:
        mock_producer = MagicMock()
        mock_cls.return_value = mock_producer
        producer = TweetsRawProducer(
            brokers="broker-1:29092",
            tweets_topic="tweets_raw",
            audit_topic="audit_logs",
        )
        yield producer, mock_producer, mock_cls


class TestProducerConfiguration:

    def test_configured_for_at_least_once(self, producer_and_mock):
        """#1/#2/#3 : idempotence + acks=all + delivery.timeout sain, pas de
        message.timeout.ms à 10s qui provoquait des pertes silencieuses."""
        _, _, mock_cls = producer_and_mock

        config = mock_cls.call_args.args[0]

        assert config["enable.idempotence"] is True
        assert str(config["acks"]) == "all"
        assert config["delivery.timeout.ms"] >= 60000
        # Le piège d'origine (10s) ne doit plus être présent tel quel.
        assert config.get("message.timeout.ms", 0) != 10000


class TestSendRouting:

    def test_valid_tweet_produced_to_tweets_raw(
        self, producer_and_mock, valid_tweet
    ):
        producer, mock_producer, _ = producer_and_mock

        producer.send(valid_tweet)

        mock_producer.produce.assert_called_once()
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["topic"] == "tweets_raw"
        assert isinstance(kwargs["value"], bytes)
        assert isinstance(kwargs["key"], bytes)
        assert callable(kwargs["on_delivery"])

    def test_invalid_tweet_produced_to_audit_only(
        self, producer_and_mock, invalid_tweet_missing_fields
    ):
        producer, mock_producer, _ = producer_and_mock

        producer.send(invalid_tweet_missing_fields)

        mock_producer.produce.assert_called_once()
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["topic"] == "audit_logs"


class TestMetrics:

    def test_valid_tweet_increments_sent(self, producer_and_mock, valid_tweet):
        """#5 : le compteur sent est incrémenté sur le chemin valide."""
        producer, _, _ = producer_and_mock

        with patch(f"{MODULE}.tweets_sent") as mock_sent:
            producer.send(valid_tweet)

        mock_sent.labels.assert_called_once_with(club="unknown")
        mock_sent.labels.return_value.inc.assert_called_once()

    def test_invalid_tweet_increments_rejected(
        self, producer_and_mock, invalid_tweet_missing_fields
    ):
        """#5 (bug corrigé) : le compteur rejected est bien incrémenté sur rejet,
        une fois par erreur de validation."""
        producer, _, _ = producer_and_mock

        with patch(f"{MODULE}.tweets_rejected") as mock_rejected:
            producer.send(invalid_tweet_missing_fields)

        assert mock_rejected.labels.call_count >= 1
        mock_rejected.labels.return_value.inc.assert_called()


class TestLifecycle:

    def test_close_flushes_without_calling_underlying_close(
        self, producer_and_mock, valid_tweet
    ):
        """#4 : confluent_kafka.Producer n'a pas de .close() ; close() ne doit
        appeler que flush()."""
        producer, mock_producer, _ = producer_and_mock
        producer.send(valid_tweet)

        producer.close()

        mock_producer.flush.assert_called()
        mock_producer.close.assert_not_called()


class TestResilience:

    def test_serialization_error_does_not_abort_batch(
        self, producer_and_mock, valid_tweet
    ):
        """#14 : une exception sur un tweet est isolée (loggée), pas propagée,
        pour ne pas tuer tout le batch en amont."""
        producer, mock_producer, _ = producer_and_mock
        producer._serializer = MagicMock()
        producer._serializer.serialize.side_effect = RuntimeError("boom")

        # Ne doit pas lever.
        producer.send(valid_tweet)

        mock_producer.produce.assert_not_called()

    def test_buffer_full_triggers_poll_and_retry(
        self, producer_and_mock, valid_tweet
    ):
        """#15 : BufferError (file locale pleine) → poll() pour drainer puis
        retry, au lieu de perdre le message."""
        producer, mock_producer, _ = producer_and_mock
        mock_producer.produce.side_effect = [BufferError("queue full"), None]

        producer.send(valid_tweet)

        assert mock_producer.produce.call_count == 2
        mock_producer.poll.assert_called()
