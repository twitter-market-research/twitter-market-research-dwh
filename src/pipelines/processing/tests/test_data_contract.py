"""
Governance tests for data contracts.
"""

from pathlib.import Path

from project.pipelines.processing.utils.data_contract import (
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
        for field in staging_columns():
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
        leaked = DENYLIST.intersection({f.name for f in staging_columns()})
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


