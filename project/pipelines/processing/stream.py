#!/usr/bin/env python3


from __future__ import annotations

import logging
import sys
import os
from dataclasses import dataclass
from typing import Mapping, Optional

from pyspark.sql import Dataframe, SparkSession

from project.pipelines.processing.utils.bq_writer import BigQueryBatchSink
from project.pipelines.processing.utils.transform import enrich_stream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger("processing.stream")

# Must match both the Spark version and its Scala build (2.12 here).
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"


@dataclass(frozen=True)
class StreamConfig:
    """
    Resolved runtime settings for the streaming job.
    """

    brokers: str
    input_topic: str
    starting_offsets: str
    staging_table: str
    max_offsets_per_trigger: Optional[int]
    api_endpoint: Optional[str]
    trigger_interval: str
    project: str
    bq_table: str
    bq_api_endpoint: Optional[str]


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
    return StreamConfig(
        brokers=env.get("KAFKA_BROKERS", "localhost:9092"),
        topic=env.get("TOPIC_NAME", "tweets_raw"),
        starting_offsets=env.get("STARTING_OFFSETS", "latest"),
        max_offsets_per_trigger=int(env.get("MAX_OFFSETS_PER_TRIGGER", "1000")),
        checkpoint_location=env.get(
            "CHECKPOINT_LOCATION", 
            "/tmp/spark-checkpoints/tweets_enriched"
        ),
        trigger_interval=env.get("TRIGGER_INTERVAL", "30 seconds"),
        project=project,
        bq_table=bq_table,
        bq_api_endpoint=env.get("BQ_API_ENDPOINT"),
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
    return (
        SparkSession.builder
        .appName("twitter-enrichment")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .getOrCreate()
    )


def read_tweets(
    spark: SparkSession,
    config: StreamConfig
) -> Dataframe:
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
        A streaming DataFrame with a ``value`` column holding the JSON payload.
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
        config.bq_table,
        config.starting_offsets,
    )

    spark = build_session(config)
    spark.sparkContext.setLogLevel("WARN")

    sink = BigQueryBatchSink(
        table=config.bq_table,
        project=config.project,
        api_endpoint=config.bq_api_endpoint
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