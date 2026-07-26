FROM confluentinc/cp-kafka-connect:7.5.0

# Pre-install S3 Sink plugin to avoid downloading at each container start
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.7
