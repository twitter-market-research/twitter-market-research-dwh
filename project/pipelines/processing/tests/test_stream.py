
from project.pipelines.processing.stream import config_from_env


class TestConfigFromEnv:
    """
    Default must safe, and every value overridable.
    """

    def test_defaults_when_env_is_empty(self) -> None:
        config = config_from_env({})
        assert config.brokers == "localhost:9092"
        assert config["input_topic"] == "tweets_raw"
        assert config.starting_offsets == "latest"

    def test_reads_overrides_from_the_mapping(self) -> None:
        config = config_from_env({
            "KAFKA_BROKERS": "broker-1:29092,broker-2:29092",
            "TOPIC_NAME": "tweets_test",
            "STARTING_OFFSETS": "earliest",
        })
        assert config.brokers == "broker-1:29092,broker-2:29092"
        assert config.starting_offsets == "earliest"

    def test_table_default_to_the_staging_dataset(self) -> None:
        """
        Without BQ_TABLE the project id drives a conventional path.
        """
        config = config_from_env({"GCP_PROJECT_ID": "my-project"})
        assert config.staging_table == "my-project.twitter_staging.tweets"

    def test_explicit_table_wins(self) -> None:
        config = config_from_env({
            "GCP_PROJECT_ID": "my-project",
            "BQ_TABLE": "other.dataset.table",
        })
        assert config.staging_table == "other.dataset.table"

    def test_max_offsets_per_trigger_is_an_int(self) -> None:
        """
        It bounds the micro-batch the sink collects on the driver.
        """
        config = config_from_env({"MAX_OFFSETS_PER_TRIGGER": "500"})
        assert config.max_offsets_per_trigger == 500

    def test_api_endpoint_is_none_for_real_bigquery(self) -> None:
        """
        No endpoint means the sink uses real credentials, not anonymous
        """
        assert config_from_env({}).api_endpoint is None
