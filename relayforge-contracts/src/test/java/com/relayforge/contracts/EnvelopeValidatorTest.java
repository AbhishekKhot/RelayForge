package com.relayforge.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EnvelopeValidatorTest {
    private static final String ORGANIZATION = "de568400-3487-45de-a986-c02816a60011";
    private static final String MESSAGE = "8d78be41-a5d2-4faa-ae8d-bbc68c035f13";
    private static final String CONFIG_TOPIC = "relayforge.config.changed.v1";
    private static final String EVENT_TOPIC = "relayforge.event.accepted.v1";
    private final EnvelopeValidator validator = new EnvelopeValidator(new ObjectMapper());

    @Test
    void acceptsPublishedConfigurationEnvelope() {
        String payload = "{\"organization_id\":\"" + ORGANIZATION + "\",\"source_revision\":4,"
                + "\"endpoint\":{\"endpoint_id\":\"d7725823-f57d-4880-9ee0-eb1d04d61502\","
                + "\"status\":\"ACTIVE\",\"version\":2},\"subscriptions\":[]}";
        ValidatedEnvelope parsed = validator.validate(CONFIG_TOPIC, ORGANIZATION,
                envelope("configuration.changed.v1", "d7725823-f57d-4880-9ee0-eb1d04d61502", payload));
        assertThat(parsed.organizationId()).isEqualTo(UUID.fromString(ORGANIZATION));
        assertThat(parsed.payload().path("source_revision").asLong()).isEqualTo(4);
        assertThat(parsed.messageId()).isEqualTo("msg_" + MESSAGE);
    }

    @Test
    void acceptsPublishedEventEnvelope() {
        String locator = "evt_opaqueLocator";
        String payload = "{\"organization_id\":\"" + ORGANIZATION + "\",\"event_id\":\""
                + locator + "\",\"ingestion_locator_version\":1,\"accepted_day\":\"2026-10-31\","
                + "\"accepted_at\":\"2026-10-31T10:00:00Z\",\"bucket_id\":3410,"
                + "\"map_epoch\":1,\"ingestion_shard_id\":\"ing-a\","
                + "\"delivery_shard_id\":\"dlv-a\",\"event_type\":\"order.created\","
                + "\"payload_hash\":\"abc\"}";
        ValidatedEnvelope parsed = validator.validate(EVENT_TOPIC, locator,
                envelope("event.accepted.v1", "c203b9c0-a399-4946-90c8-8cd6a82f62df", payload));
        assertThat(parsed.payload().path("event_id").asText()).isEqualTo(locator);
    }

    @Test
    void acceptsMinimalDeliveryReadyPayload() {
        String deliveryId = "dlv_123";
        ValidatedEnvelope parsed = validator.validate("relayforge.delivery.ready.v1", deliveryId,
                envelope("delivery.ready.v1", deliveryId,
                        "{\"delivery_id\":\"" + deliveryId + "\"}"));
        assertThat(parsed.aggregateId()).isEqualTo(deliveryId);
    }

    @Test
    void rejectsConflictingOptionalDeliveryTenant() {
        String deliveryId = "dlv_123";
        assertThatThrownBy(() -> validator.validate("relayforge.delivery.ready.v1", deliveryId,
                envelope("delivery.ready.v1", deliveryId,
                        "{\"delivery_id\":\"" + deliveryId + "\",\"organization_id\":\""
                                + UUID.randomUUID() + "\"}")))
                .isInstanceOf(InvalidEnvelopeException.class)
                .satisfies(failure -> assertThat(((InvalidEnvelopeException) failure).reasonCode())
                        .isEqualTo("INVALID_PAYLOAD"));
    }

    @Test
    void acceptsAnalyticsFactIdentity() {
        String factId = "fact_123";
        ValidatedEnvelope parsed = validator.validate("relayforge.analytics.delivery-facts.v1", factId,
                envelope("analytics.delivery-facts.v1", factId,
                        "{\"fact_id\":\"" + factId + "\",\"organization_id\":\""
                                + ORGANIZATION + "\"}"));
        assertThat(parsed.aggregateId()).isEqualTo(factId);
    }

    @Test
    void rejectsUnknownVersionWithSafeReason() {
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION,
                envelope("configuration.changed.v2", "endpoint", "{\"organization_id\":\""
                        + ORGANIZATION + "\"}")))
                .isInstanceOf(InvalidEnvelopeException.class)
                .satisfies(failure -> assertThat(((InvalidEnvelopeException) failure).reasonCode())
                        .isEqualTo("UNSUPPORTED_VERSION"))
                .hasMessageNotContaining(ORGANIZATION);
    }

    @Test
    void rejectsTopicMismatchAndTenantMismatch() {
        assertThatThrownBy(() -> validator.validate(EVENT_TOPIC, ORGANIZATION,
                envelope("configuration.changed.v1", "endpoint", "{}")))
                .isInstanceOf(InvalidEnvelopeException.class);
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION,
                envelope("configuration.changed.v1", "endpoint", "{\"organization_id\":\""
                        + UUID.randomUUID() + "\",\"source_revision\":1,\"endpoint\":{}}")))
                .isInstanceOf(InvalidEnvelopeException.class)
                .satisfies(failure -> assertThat(((InvalidEnvelopeException) failure).reasonCode())
                        .isEqualTo("INVALID_PAYLOAD"));
    }

    @Test
    void rejectsKeyMismatchAndMissingFields() {
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, UUID.randomUUID().toString(),
                envelope("configuration.changed.v1", "endpoint", "{\"organization_id\":\""
                        + ORGANIZATION + "\",\"source_revision\":1,\"endpoint\":{}}")))
                .isInstanceOf(InvalidEnvelopeException.class);
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION, "{}"))
                .isInstanceOf(InvalidEnvelopeException.class);
    }

    @Test
    void rejectsMalformedOrOversizedInputWithoutEchoingIt() {
        String sensitive = "top-secret-token";
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION, sensitive))
                .isInstanceOf(InvalidEnvelopeException.class)
                .hasMessageNotContaining(sensitive);
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION, "x".repeat(131073)))
                .isInstanceOf(InvalidEnvelopeException.class)
                .satisfies(failure -> assertThat(((InvalidEnvelopeException) failure).reasonCode())
                        .isEqualTo("OVERSIZED_ENVELOPE"));
    }

    @Test
    void rejectsNoncanonicalUuidAndMalformedMetadata() {
        String invalidMessage = envelope("configuration.changed.v1", "endpoint", "{}")
                .replace(MESSAGE, "1-1-1-1-1");
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION, invalidMessage))
                .isInstanceOf(InvalidEnvelopeException.class)
                .satisfies(failure -> assertThat(((InvalidEnvelopeException) failure).reasonCode())
                        .isEqualTo("INVALID_ENVELOPE"));
        assertThatThrownBy(() -> validator.validate(CONFIG_TOPIC, ORGANIZATION,
                envelope("configuration.changed.v1", "endpoint", "{}")
                        .replace("2026-10-31T10:00:00Z", "not-an-instant")))
                .isInstanceOf(InvalidEnvelopeException.class);
    }

    private String envelope(String type, String aggregateId, String payload) {
        return "{\"message_id\":\"msg_" + MESSAGE + "\",\"message_type\":\"" + type
                + "\",\"schema_version\":1,\"occurred_at\":\"2026-10-31T10:00:00Z\","
                + "\"organization_id\":\"" + ORGANIZATION + "\",\"aggregate_id\":\""
                + aggregateId + "\",\"correlation_id\":\"msg_" + MESSAGE + "\",\"payload\":"
                + payload + "}";
    }
}
