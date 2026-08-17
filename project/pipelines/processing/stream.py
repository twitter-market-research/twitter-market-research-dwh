#!/usr/bin/env python3
"""
Entry point: Kafka(tweets_raw) -> enrich -> BigQuery staging.

It reads the raw tweets topic as a Structured Streaming source, applies the
``enrich_stream`` transformation and writes every micro-batch to
BigQuery through ``BigQueryBatchSink`` in ``foreachBatch``.

Configuration comes from the environment:
- KAFKA_BROKERS           : bootstrap servers (default localhost:9092)
- TOPIC_NAME              : source topic (default tweets_raw)
- STARTING_OFFSETS        : earliest | latest (default latest)
- MAX_OFFSETS_PER_TRIGGER : micro-batch ceiling (default 1000)
- CHECKPOINT_LOCATION     : streaming state dir (must be persistent)
- GCP_PROJECT_ID          : project id (any string against the emulator)
- BQ_TABLE                : project.dataset.table (default: staging)
- BQ_API_ENDPOINT         : emulator URL; unset for real BigQuery
- TRIGGER_INTERVAL        : micro-batch pace (default "30 seconds")
- OUTPUT_TOPIC            : silver topic (default tweets_enriched)

"""

from __future__ import annotations

import logging
import sys
import os
from dataclasses import dataclass
from typing import Mapping, Optional

from project.pipelines.processing.utils.dual_sink import DualSink
from pyspark.sql import DataFrame, SparkSession

from project.pipelines.processing.utils.bq_writer import BigQueryBatchSink
from project.pipelines.processing.utils.transform import enrich_stream
from project.pipelines.processing.utils.kafka_publisher import KafkaEnrichedPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("processing.stream")


@dataclass(frozen=True)
class StreamConfig:
    """
    Resolved runtime settings for the streaming job.
    """

    brokers: str
    topic: str
    starting_offsets: str
    max_offsets_per_trigger: int
    checkpoint_location: str
    trigger_interval: str
    project: str
    staging_table: str
    api_endpoint: Optional[str]
    output_topic: str


def config_from_env(env: Mapping[str, str]) -> StreamConfig:
    """
    Read the environment variables and return a ``StreamConfig`` object.

    Parameters
    ----------
    env : Mapping[str, str]
        Environment variables (``os.environ`` or a test dict).

    Returns
    -------
    StreamConfig
        The resolved configuration.
    """
    project = env.get("GCP_PROJECT_ID", "local-project")
    bq_table = env.get("BQ_TABLE", f"{project}.twitter_staging.tweets")
    output_topic=env.get("OUTPUT_TOPIC", "tweets_enriched"),

    return StreamConfig(
        brokers=env.get("KAFKA_BROKERS", "localhost:9092"),
        topic=env.get("TOPIC_NAME", "tweets_raw"),
        starting_offsets=env.get("STARTING_OFFSETS", "latest"),
        max_offsets_per_trigger=int(
            env.get("MAX_OFFSETS_PER_TRIGGER", "1000")
        ),
        checkpoint_location=env.get(
            "CHECKPOINT_LOCATION",
            "/tmp/spark-checkpoints/tweets_enriched",
        ),
        trigger_interval=env.get("TRIGGER_INTERVAL", "30 seconds"),
        project=project,
        staging_table=bq_table,
        # An undefined compose variable arrives as "" rather than absent:
        # `or None` keeps the sink on real BigQuery credentials instead of
        # anonymous ones.
        api_endpoint=env.get("BQ_API_ENDPOINT") or None,
        output_topic=output_topic
    )


def build_session(config: StreamConfig) -> SparkSession:
    """
    Create the SparkSession, pinned to UTC.

    Parameters
    ----------
    config : StreamConfig
        The resolved configuration.

    Returns
    -------
    SparkSession
        The active SparkSession .
    """
    # No spark.jars.packages here: the Kafka connector jars are baked into
    # the image (see the processing Dockerfile). Declaring the package
    # would make a direct `python3 stream.py` run contact Maven Central on
    # every start -- dependency provisioning belongs to the image.
    return (
        SparkSession.builder
        .appName("twitter-enrichment")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_tweets(
    spark: SparkSession,
    config: StreamConfig
) -> DataFrame:
    """
    Read the raw tweets from Kafka as a streaming DataFrame.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession.
    config : StreamConfig
        The resolved configuration.

    Returns
    -------
    DataFrame
        A streaming DataFrame whose ``value`` column holds the JSON
        payload, alongside the Kafka metadata columns.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.brokers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .load()
    )


def main() -> int:
    """
    Run the streaming job until termination

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure).
    """
    config = config_from_env(os.environ)
    logger.info(
        "Streaming %s from %s -> %s (offsets=%s)",
        config.topic,
        config.brokers,
        config.staging_table,
        config.starting_offsets,
    )

    spark = build_session(config)
    spark.sparkContext.setLogLevel("WARN")

    sink = DualSink(
        kafka_sink=KafkaEnrichedPublisher(
            brokers=config.brokers,
            topic=config.output_topic,
        ),
        bq_sink=BigQueryBatchSink(
            table=config.staging_table,
            project=config.project,
            api_endpoint=config.api_endpoint,
        ),
    )

    query = (
        enrich_stream(read_tweets(spark, config))
        .writeStream
        .foreachBatch(sink)
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime=config.trigger_interval)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
