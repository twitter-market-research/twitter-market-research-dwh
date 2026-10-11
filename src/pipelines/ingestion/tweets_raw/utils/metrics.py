# Metrics for monitoring the tweets ingestion process
from prometheus_client import Counter, Histogram, start_http_server


# Counter to track the total number of tweets sent to Kafka topics,
# labeled by club
tweets_sent = Counter(
    "tweets_raw_producer_sent_total",
    "Total number of tweets sent to Kafka topics",
    ["club"]
)

# Counter to track the total number of tweets rejected due to
# validation errors, labeled by reason
tweets_rejected = Counter(
    "tweets_raw_producer_rejected_total",
    "Total number of tweets rejected due to validation errors",
    ["reason"]
)

send_latency = Histogram(
    "tweets_raw_producer_send_latency_seconds",
    "Latency of sending tweets to Kafka topics in seconds",
)

# Counter to track messages the broker never acknowledged (async delivery
# failures reported via the delivery callback), labeled by topic.
delivery_failed = Counter(
    "tweets_raw_producer_delivery_failed_total",
    "Total number of messages that failed to be delivered to Kafka",
    ["topic"]
)


def start_metrics_server(port: int = 8000) -> None:
    """
    This function starts the Prometheus metrics server on the specified port.
    Args:
        port (int): The port on which to expose the metrics endpoint.
         Default is 8000.
    """
    start_http_server(port)
