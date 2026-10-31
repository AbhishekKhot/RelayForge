#!/usr/bin/env bash
set -euo pipefail

for topic in \
    relayforge.config.changed.v1 \
    relayforge.event.accepted.v1 \
    relayforge.delivery.ready.v1 \
    relayforge.analytics.delivery-facts.v1; do
    for suffix in '' '.quarantine'; do
        /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --create --if-not-exists --topic "$topic$suffix" \
            --partitions 1 --replication-factor 1 \
            --config retention.ms=86400000 \
            --config cleanup.policy=delete
    done
done
