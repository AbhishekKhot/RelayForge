CREATE UNIQUE INDEX delivery_outbox_fact_version_idx ON delivery_outbox
    ((payload ->> 'fact_id'), (payload ->> 'version'))
    WHERE message_type = 'analytics.delivery-facts.v1';

CREATE FUNCTION emit_delivery_analytics_fact(
    delivery deliveries, fact_type text, fact_time timestamptz, fact_status text,
    fact_attempt integer, fact_http integer, fact_error text, fact_duration integer,
    identity_suffix text DEFAULT '') RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    fact uuid := md5(fact_type || ':' || delivery.accepted_day::text || ':'
        || delivery.id::text || ':' || identity_suffix)::uuid;
BEGIN
    INSERT INTO delivery_outbox (organization_id, accepted_day, event_id, delivery_id,
        message_type, topic, partition_key, payload)
    VALUES (delivery.organization_id, delivery.accepted_day, delivery.event_id, delivery.id,
        'analytics.delivery-facts.v1', 'relayforge.analytics.delivery-facts.v1', fact::text,
        jsonb_strip_nulls(jsonb_build_object(
            'organization_id', delivery.organization_id,
            'event_id', delivery.event_id,
            'delivery_id', delivery.id,
            'endpoint_id', delivery.endpoint_id,
            'replay_generation', delivery.generation,
            'fact_id', fact,
            'version', 1,
            'fact_type', fact_type,
            'status', fact_status,
            'attempt_number', CASE WHEN fact_attempt BETWEEN 1 AND 65535
                THEN fact_attempt END,
            'http_status', CASE WHEN fact_http BETWEEN 100 AND 599 THEN fact_http END,
            'duration_ms', CASE WHEN fact_duration BETWEEN 0 AND 3600000
                THEN fact_duration END,
            'error_code', CASE WHEN fact_error ~ '^[A-Z][A-Z0-9_]{0,79}$'
                THEN fact_error END,
            'occurred_at', fact_time,
            'event_date', (fact_time AT TIME ZONE 'UTC')::date)));
END;
$$;

CREATE FUNCTION emit_delivery_state_facts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM emit_delivery_analytics_fact(NEW, 'delivery.created', NEW.created_at,
            NEW.status, NULL, NULL, NULL, NULL);
        IF NEW.generation > 1 THEN
            PERFORM emit_delivery_analytics_fact(NEW, 'delivery.replayed', NEW.created_at,
                NEW.status, NULL, NULL, NULL, NULL);
        END IF;
    ELSIF OLD.status IS DISTINCT FROM NEW.status THEN
        IF NEW.status = 'SUCCEEDED' THEN
            PERFORM emit_delivery_analytics_fact(NEW, 'delivery.succeeded',
                coalesce(NEW.completed_at, NEW.updated_at), NEW.status,
                NEW.attempt_count, NEW.last_http_status, NULL, NULL);
        ELSIF NEW.status = 'RETRY_SCHEDULED' THEN
            PERFORM emit_delivery_analytics_fact(NEW, 'delivery.retry_scheduled',
                NEW.updated_at, NEW.status, NEW.attempt_count, NEW.last_http_status,
                NEW.last_error_code, NULL, NEW.version::text);
        ELSIF NEW.status = 'DEAD_LETTERED' THEN
            PERFORM emit_delivery_analytics_fact(NEW, 'delivery.dead_lettered',
                coalesce(NEW.completed_at, NEW.updated_at), NEW.status,
                NEW.attempt_count, NEW.last_http_status, NEW.last_error_code, NULL);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_state_analytics_fact
    AFTER INSERT OR UPDATE OF status ON deliveries
    FOR EACH ROW EXECUTE FUNCTION emit_delivery_state_facts();

CREATE FUNCTION emit_finished_attempt_fact() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fact uuid := md5('delivery.attempt.finished:' || NEW.accepted_day::text || ':'
        || NEW.id::text)::uuid;
    fact_version bigint;
    first_occurred_at timestamptz;
    fact_time timestamptz;
    delivery_row deliveries%ROWTYPE;
BEGIN
    IF NEW.finished_at IS NULL OR NEW.outcome = 'IN_PROGRESS'
            OR TG_OP = 'UPDATE' AND OLD.finished_at IS NOT NULL
                AND (OLD.outcome, OLD.http_status, OLD.duration_ms, OLD.error_code)
                    IS NOT DISTINCT FROM
                    (NEW.outcome, NEW.http_status, NEW.duration_ms, NEW.error_code) THEN
        RETURN NEW;
    END IF;
    SELECT * INTO STRICT delivery_row FROM deliveries
    WHERE accepted_day = NEW.accepted_day AND id = NEW.delivery_id
        AND organization_id = NEW.organization_id;
    SELECT coalesce(max((payload ->> 'version')::bigint), 0) + 1 INTO fact_version
    FROM delivery_outbox WHERE message_type = 'analytics.delivery-facts.v1'
        AND payload ->> 'fact_id' = fact::text;
    SELECT (payload ->> 'occurred_at')::timestamptz INTO first_occurred_at
    FROM delivery_outbox WHERE message_type = 'analytics.delivery-facts.v1'
        AND payload ->> 'fact_id' = fact::text
    ORDER BY (payload ->> 'version')::bigint LIMIT 1;
    fact_time := coalesce(first_occurred_at, NEW.finished_at);
    INSERT INTO delivery_outbox (organization_id, accepted_day, event_id, delivery_id,
        message_type, topic, partition_key, payload)
    VALUES (NEW.organization_id, NEW.accepted_day, delivery_row.event_id, NEW.delivery_id,
        'analytics.delivery-facts.v1', 'relayforge.analytics.delivery-facts.v1', fact::text,
        jsonb_strip_nulls(jsonb_build_object(
            'organization_id', NEW.organization_id,
            'event_id', delivery_row.event_id,
            'delivery_id', NEW.delivery_id,
            'endpoint_id', delivery_row.endpoint_id,
            'replay_generation', delivery_row.generation,
            'fact_id', fact,
            'version', fact_version,
            'fact_type', 'delivery.attempt.finished',
            'status', NEW.outcome,
            'attempt_number', CASE WHEN NEW.attempt_number BETWEEN 1 AND 65535
                THEN NEW.attempt_number END,
            'http_status', CASE WHEN NEW.http_status BETWEEN 100 AND 599
                THEN NEW.http_status END,
            'duration_ms', CASE WHEN NEW.duration_ms BETWEEN 0 AND 3600000
                THEN NEW.duration_ms END,
            'error_code', CASE WHEN NEW.error_code ~ '^[A-Z][A-Z0-9_]{0,79}$'
                THEN NEW.error_code END,
            'occurred_at', fact_time,
            'event_date', (fact_time AT TIME ZONE 'UTC')::date)));
    RETURN NEW;
END;
$$;

CREATE TRIGGER delivery_attempt_analytics_fact
    AFTER INSERT OR UPDATE OF finished_at, outcome, http_status, duration_ms, error_code
    ON delivery_attempts FOR EACH ROW EXECUTE FUNCTION emit_finished_attempt_fact();
