"""
Governance tests for data contracts.
"""

from pathlib import Path

from src.pipelines.processing.utils.data_contract import (
    CLASSES,
    DENYLIST,
    RETENTION_DAYS,
    STAGING_CONTRACT,
    staging_columns
)

IAC = Path(__file__).resolve().parents[4] / "iac"
SCHEMA_YAML = IAC / "dev" / "storage" / "bigquery" / "staging_schema.yaml"
KAFKA_COMPOSE = IAC / "dev" / "kafka" / "docker-compose.yml"


class TestDataContract:
    """
    The contract governs what may reach the slver layer.
    """

    def test_every_field_is_classified(self):
        """
        No column may carry an unknown data class.
        """
        for field in STAGING_CONTRACT:
            assert field.data_class in CLASSES, field.name

    def test_every_field_states_a_purpose(self) -> None:
        """
        A field with no stated purpose is a field to
        drop.
        """
        for field in STAGING_CONTRACT:
            assert field.purpose.strip(), field.name

    def test_no_denylisted_field_reaches_silver(self) -> None:
        """
        Directly identifying fields
            must never be projected
        """
        leaked = DENYLIST.intersection(staging_columns())
        assert not leaked, f"personal data in silver: {sorted(leaked)}"


class TestRetentionIsEnforced:
    """
    A retention decision that infra ignores is not a decision at all.
    """

    def test_enriched_topic_carries_the_contract_retention(self):
        """
        A retention decision that infra ignores is not a decision at all.
        """
        expected_ms = RETENTION_DAYS * 24 * 60 * 60 * 1000
        compose = KAFKA_COMPOSE.read_text(encoding="utf-8")

        assert f"retention.ms={expected_ms}" in compose

    def test_contract_matches_the_bigquery_schema(self) -> None:
        """
        The DDL and the contract cannot drift apart silently.
        """
        declared = [
            line.split("name:", 1)[1].strip()
            for line in SCHEMA_YAML.read_text(encoding="utf-8").splitlines()
            if "- name" in line
        ]
        assert declared == staging_columns()

    def test_every_topic_carries_its_retention(self) -> None:
        """
        A retention decision that infra ignores is not a decision at all.
        """
        expected = {
            "tweets_raw": 7 * 24 * 60 * 60 * 1000,
            "tweets_enriched": RETENTION_DAYS * 24 * 60 * 60 * 1000,
            "audit_logs": 14 * 24 * 60 * 60 * 1000,
        }

        compose = KAFKA_COMPOSE.read_text(encoding="utf-8")

        for topic, ms in expected.items():
            block = compose.split(f"--topic {topic}", 1)[1][:400]
            assert f"retention.ms={ms}" in block, topic
