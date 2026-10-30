#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

show_startup_failure() {
    docker compose ps >&2
    docker compose logs --tail=120 kafka >&2
}
trap show_startup_failure ERR

if [[ ! -f .env ]]; then
    echo "Create .env with python3 scripts/create-env.py before verification" >&2
    exit 1
fi

wait_healthy() {
    local service="$1"
    local container
    local status
    for ((attempt = 0; attempt < 120; attempt++)); do
        container="$(docker compose ps -q "$service")"
        if [[ -n "$container" ]]; then
            status="$(docker inspect --format '{{.State.Health.Status}}' "$container")"
            if [[ "$status" == healthy ]]; then
                return 0
            fi
        fi
        sleep 2
    done
    echo "$service did not become healthy" >&2
    docker compose ps >&2
    exit 1
}

verify_migrations() {
    local database
    local count
    for database in relayforge_control relayforge_ingestion relayforge_delivery; do
        local expected=1
        if [[ "$database" == relayforge_control || "$database" == relayforge_ingestion ]]; then
            expected=2
        fi
        count="$(docker compose exec -T postgres sh -c \
            'psql -U "$POSTGRES_USER" -d "$1" -At -c "SELECT count(*) FROM flyway_schema_history WHERE version IS NOT NULL AND success"' \
            sh "$database")"
        if [[ "$count" != "$expected" ]]; then
            echo "$database has $count successful versioned migrations; expected $expected" >&2
            exit 1
        fi
    done
}

services=(
    postgres kafka redis clickhouse webhook-simulator
    control-plane control-publisher ingestion ingestion-publisher router
    delivery-api delivery-worker delivery-publisher retry-scheduler
    analytics-consumer analytics-api
)

docker compose config -q
COMPOSE_PARALLEL_LIMIT=2 docker compose up -d --build
for service in "${services[@]}"; do
    wait_healthy "$service"
done
verify_migrations
python3 scripts/verify-b1.py

docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
    -c "CREATE TABLE IF NOT EXISTS d0_persistence_probe (id integer PRIMARY KEY, value text NOT NULL)" \
    -c "INSERT INTO d0_persistence_probe (id, value) VALUES (1, '\''committed'\'') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value"' >/dev/null

topics="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)"
for topic in relayforge.config.changed.v1 relayforge.event.accepted.v1 \
        relayforge.delivery.ready.v1 relayforge.analytics.delivery-facts.v1; do
    if ! grep -qx "$topic" <<< "$topics"; then
        echo "Missing Kafka topic $topic" >&2
        exit 1
    fi
done

table="$(docker compose exec -T clickhouse sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "EXISTS TABLE relayforge_analytics.delivery_facts"')"
if [[ "$table" != 1 ]]; then
    echo "ClickHouse delivery_facts table is missing" >&2
    exit 1
fi

for service in control-plane control-publisher ingestion ingestion-publisher router \
        delivery-api delivery-worker delivery-publisher retry-scheduler \
        analytics-consumer analytics-api; do
    docker compose run --rm "test-$service" >/dev/null
done

docker compose stop postgres
docker compose exec -T control-plane java -cp /app HealthProbe http://127.0.0.1:8080/health/live
if docker compose exec -T control-plane java -cp /app HealthProbe http://127.0.0.1:8080/health/ready; then
    echo "Control-plane stayed ready after PostgreSQL stopped" >&2
    exit 1
fi
docker compose start postgres
wait_healthy postgres
wait_healthy control-plane

docker compose stop kafka
if docker compose exec -T control-publisher java -cp /app HealthProbe http://127.0.0.1:8080/health/ready; then
    echo "Control publisher stayed ready after Kafka stopped" >&2
    exit 1
fi
docker compose start kafka
wait_healthy kafka
wait_healthy control-publisher

docker compose stop clickhouse
if docker compose exec -T analytics-api java -cp /app HealthProbe http://127.0.0.1:8080/health/ready; then
    echo "Analytics API stayed ready after ClickHouse stopped" >&2
    exit 1
fi
docker compose start clickhouse
wait_healthy clickhouse
wait_healthy analytics-api

docker compose down
COMPOSE_PARALLEL_LIMIT=2 docker compose up -d --build
for service in "${services[@]}"; do
    wait_healthy "$service"
done
verify_migrations
persisted="$(docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "SELECT value FROM d0_persistence_probe WHERE id = 1"')"
if [[ "$persisted" != committed ]]; then
    echo "Committed PostgreSQL data was lost after restart" >&2
    exit 1
fi

echo "D0 clean boot, dependency readiness, restart, and B1 endpoint checks passed"
