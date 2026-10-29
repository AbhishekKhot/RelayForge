from base64 import urlsafe_b64encode
from pathlib import Path
from secrets import token_hex
from uuid import uuid4
import os


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / ".env.example"
TARGET = ROOT / ".env"

generated = {
    key: token_hex(24)
    for key in (
        "POSTGRES_SUPERUSER_PASSWORD",
        "CONTROL_DB_PASSWORD",
        "INGESTION_DB_PASSWORD",
        "DELIVERY_DB_PASSWORD",
        "REDIS_PASSWORD",
        "CLICKHOUSE_PASSWORD",
        "TRUSTED_PROVISIONING_CREDENTIAL",
    )
}
generated["KAFKA_CLUSTER_ID"] = urlsafe_b64encode(uuid4().bytes).decode().rstrip("=")

lines = []
for line in EXAMPLE.read_text().splitlines():
    key, separator, value = line.partition("=")
    if separator and value == "GENERATE_ME":
        line = key + "=" + generated[key]
    lines.append(line)

descriptor = os.open(TARGET, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(descriptor, "w") as environment_file:
    environment_file.write("\n".join(lines) + "\n")

print("Created .env with local-only credentials")
