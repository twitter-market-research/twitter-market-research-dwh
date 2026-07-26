from unittest.mock import MagicMock, patch
import pytest

from project.pipelines.ingestion.tweets_raw.utils.kafka_producer import (
    TweetsRawProducer,
)


class TestTweetsRawProducer:

    @patch("project.pipelines.ingestion.tweets_raw.utils.kafka_producer.KafkaProducer")
    def test_valid_tweet_is_sent_to_tweets_raw(
        self,
        mock_kafka_class: MagicMock,
        valid_tweet: dict
    ) -> None:
        """This test checks that a valid tweet is sent to the
         'tweets_raw' topic.
        Args:
            - mock_kafka_class (MagicMock): A mock of the KafkaProducer class.
            - valid_tweet (dict): A fixture that provides a valid tweet
             dictionary.
        """
        mock_producer = MagicMock()
        mock_kafka_class.return_value = mock_producer

        producer = TweetsRawProducer(
            brokers="broker-1:29092",
            tweets_topic="tweets_raw",
            audit_topic="audit_logs",
        )

        producer.send(valid_tweet)

        mock_producer.send.assert_called_once()
        topic, kwargs = mock_producer.send.call_args
        assert topic[0] == "tweets_raw"
        assert "value" in kwargs
        assert isinstance(kwargs["value"], bytes)
        assert "key" in kwargs
        assert isinstance(kwargs["key"], bytes)

    @patch("project.pipelines.ingestion.tweets_raw.utils.kafka_producer.KafkaProducer")
    def test_invalid_tweet_not_sent_to_tweets_raw_but_to_audit(
        self,
        mock_kafka_class: MagicMock,
        invalid_tweet_missing_fields: dict
    ) -> None:
        """This test checks that an invalid tweet is not sent to the
         'tweets_raw' topic but is sent to the 'audit_logs' topic.
         Args:
            - mock_kafka_class (MagicMock): A mock of the KafkaProducer class.
            - invalid_tweet_missing_fields (dict): A fixture that provides an
             invalid tweet dictionary.
        """
        mock_producer = MagicMock()
        mock_kafka_class.return_value = mock_producer

        producer = TweetsRawProducer(
            brokers="broker-1:29092",
            tweets_topic="tweets_raw",
            audit_topic="audit_logs",
        )

        producer.send(invalid_tweet_missing_fields)

        # 1 seul send : sur audit_logs
        mock_producer.send.assert_called_once()
        topic, kwargs = mock_producer.send.call_args
        assert topic[0] == "audit_logs"

    @patch("project.pipelines.ingestion.tweets_raw.utils.kafka_producer.KafkaProducer")
    def test_flush_is_called_on_close(
        self,
        mock_kafka_class: MagicMock,
        valid_tweet: dict
    ) -> None:
        """This test checks that the flush method is called on the Kafka
         producer
         when the close method of TweetsRawProducer is called.
        Args:
            - mock_kafka_class (MagicMock): A mock of the KafkaProducer class.
            - valid_tweet (dict): A fixture that provides a valid tweet
             dictionary.
        """
        mock_producer = MagicMock()
        mock_kafka_class.return_value = mock_producer

        producer = TweetsRawProducer(
            brokers="broker-1:29092",
            tweets_topic="tweets_raw",
            audit_topic="audit_logs",
        )
        producer.send(valid_tweet)
        producer.close()

        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()
