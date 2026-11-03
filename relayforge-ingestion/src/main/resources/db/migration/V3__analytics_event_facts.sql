ALTER TABLE ingestion_outbox DROP CONSTRAINT ingestion_outbox_event_id_message_type_key;
ALTER TABLE ingestion_outbox ADD CONSTRAINT ingestion_outbox_day_event_type_key
    UNIQUE (accepted_day, event_id, message_type);

CREATE FUNCTION emit_accepted_event_fact() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fact uuid := md5('event.accepted:' || NEW.accepted_day::text || ':' || NEW.id::text)::uuid;
BEGIN
    INSERT INTO ingestion_outbox (organization_id, accepted_day, event_id, message_type,
        topic, partition_key, payload)
    VALUES (NEW.organization_id, NEW.accepted_day, NEW.id, 'analytics.delivery-facts.v1',
        'relayforge.analytics.delivery-facts.v1', fact::text,
        jsonb_build_object(
            'organization_id', NEW.organization_id,
            'event_id', NEW.id,
            'fact_id', fact,
            'version', 1,
            'fact_type', 'event.accepted',
            'status', 'ACCEPTED',
            'occurred_at', NEW.accepted_at,
            'event_date', (NEW.accepted_at AT TIME ZONE 'UTC')::date));
    RETURN NEW;
END;
$$;

CREATE TRIGGER event_accepted_analytics_fact AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION emit_accepted_event_fact();
