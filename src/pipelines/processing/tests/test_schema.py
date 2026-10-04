"""Nullability contract of the ``from_json`` read schema.

``from_json`` is PERMISSIVE by design: a truncated payload, a missing key
or a type mismatch yields ``null`` instead of raising, so a single bad
message cannot kill a stream meant to run forever. Declaring a field
``nullable=False`` therefore promises Catalyst something the parser
cannot honour -- and Catalyst may act on that promise, for instance by
treating the ``isNotNull`` guard in ``transform.enrich_stream`` as
always-true and pruning it. Rejecting bad rows is the filter's job, not
the schema's.

Pure schema introspection: needs PySpark installed, but no JVM.
"""
import pytest

pytest.importorskip("pyspark")

from project.pipelines.processing.utils.schema import (  # noqa: E402
    TWEET_SCHEMA,
)


class TestTweetSchemaNullability:
    """Every field must be nullable, because from_json produces nulls."""

    def test_every_field_is_nullable(self) -> None:
        """No field may promise non-nullability to the optimizer."""
        non_nullable = [
            field.name for field in TWEET_SCHEMA.fields if not field.nullable
        ]
        assert non_nullable == []

    def test_tweet_id_is_nullable(self) -> None:
        """The guarded column above all: transform filters it explicitly."""
        assert TWEET_SCHEMA["tweet_id"].nullable is True
