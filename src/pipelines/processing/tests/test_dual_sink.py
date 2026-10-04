"""Ordering and caching contract of the dual foreachBatch sink.

No Spark needed: the batch DataFrame and both sinks are fakes that just
record the calls they receive.
"""
from project.pipelines.processing.utils.dual_sink import DualSink


class FakeBatch:
    """Minimal stand-in for a micro-batch DataFrame."""

    def __init__(self) -> None:
        self.events = []

    def persist(self):
        self.events.append("persist")
        return self

    def unpersist(self):
        self.events.append("unpersist")
        return self


class RecordingSink:
    def __init__(self, name, events, fail=False):
        self._name = name
        self._events = events
        self._fail = fail

    def __call__(self, batch_df, batch_id):
        self._events.append(self._name)
        if self._fail:
            raise RuntimeError(f"{self._name} exploded")


class TestDualSink:
    """Kafka first, BigQuery second, always unpersist."""

    def test_writes_kafka_then_bigquery(self) -> None:
        events = []
        batch = FakeBatch()
        sink = DualSink(
            kafka_sink=RecordingSink("kafka", events),
            bq_sink=RecordingSink("bigquery", events),
        )
        sink(batch, 0)
        assert events == ["kafka", "bigquery"]

    def test_caches_the_batch_around_both_writes(self) -> None:
        batch = FakeBatch()
        events = []
        sink = DualSink(
            kafka_sink=RecordingSink("kafka", events),
            bq_sink=RecordingSink("bigquery", events),
        )
        sink(batch, 0)
        assert batch.events == ["persist", "unpersist"]

    def test_unpersists_even_when_a_sink_fails(self) -> None:
        """A leaked cached batch would eat executor memory batch after batch."""
        batch = FakeBatch()
        events = []
        sink = DualSink(
            kafka_sink=RecordingSink("kafka", events, fail=True),
            bq_sink=RecordingSink("bigquery", events),
        )
        try:
            sink(batch, 0)
        except RuntimeError:
            pass
        assert batch.events == ["persist", "unpersist"]
        assert "bigquery" not in events
