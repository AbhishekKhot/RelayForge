"""Verify malformed messages are acknowledged and quarantined by each live consumer."""

import hashlib
import json
import subprocess
import time
from uuid import uuid4

from verify_support import settings


TOPICS = {
    "relayforge.config.changed.v1": "relayforge-router-config-",
    "relayforge.event.accepted.v1": "relayforge-router-events-",
    "relayforge.delivery.ready.v1": "relayforge-delivery-worker",
    "relayforge.analytics.delivery-facts.v1": "relayforge-analytics-consumer",
}


def kafka(*arguments, input_text=None, timeout=30):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka", *arguments],
        input=input_text, text=True, capture_output=True, timeout=timeout)
    assert result.returncode == 0, result.stderr.strip()
    return result.stdout.strip()


def end_offset(topic):
    output = kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
                   "kafka:9092", "--topic", topic)
    offsets = [int(line.rsplit(":", 1)[1]) for line in output.splitlines()
               if line.startswith(topic + ":0:")]
    assert len(offsets) == 1, (topic, output)
    return offsets[0]


def caught_up(group, topic):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka",
         "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092",
         "--describe", "--group", group],
        text=True, capture_output=True, timeout=20)
    if result.returncode != 0:
        return False
    rows = [parts for parts in (line.split() for line in result.stdout.splitlines())
            if len(parts) >= 6 and parts[1] == topic]
    return bool(rows) and all(parts[5] == "0" for parts in rows)


def main():
    shard = settings()["DELIVERY_SHARD_ID"]
    for topic, group_prefix in TOPICS.items():
        group = group_prefix + shard if group_prefix.endswith("-") else group_prefix
        quarantine_topic = topic + ".quarantine"
        quarantine_offset = end_offset(quarantine_topic)
        secret = "private-secret-" + uuid4().hex
        key = "invalid-" + uuid4().hex
        kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
              "kafka:9092", "--topic", topic, "--property", "parse.key=true",
              "--property", "key.separator=|", input_text=key + "|" + secret + "\n")
        output = kafka("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server",
                       "kafka:9092", "--topic", quarantine_topic, "--partition", "0",
                       "--offset", str(quarantine_offset), "--max-messages", "1",
                       "--timeout-ms", "30000", "--property", "print.key=true",
                       "--property", "key.separator=|", timeout=40)
        record_key, body = output.split("|", 1)
        metadata = json.loads(body)
        assert metadata["source_topic"] == topic
        assert metadata["source_partition"] == 0
        assert metadata["source_offset"] >= 0
        assert metadata["quarantine_id"] == record_key
        identity = f"{topic}:0:{metadata['source_offset']}"
        assert record_key == hashlib.sha256(identity.encode()).hexdigest()
        assert metadata["value_sha256"] == hashlib.sha256(secret.encode()).hexdigest()
        assert metadata["reason"] in ("MALFORMED_JSON", "INVALID_FACT")
        assert secret not in body
        for _ in range(60):
            if caught_up(group, topic):
                break
            time.sleep(1)
        else:
            raise AssertionError(f"{group} did not commit malformed {topic} message")
        print(f"D2 {topic} malformed message quarantined and source offset committed")


if __name__ == "__main__":
    main()
