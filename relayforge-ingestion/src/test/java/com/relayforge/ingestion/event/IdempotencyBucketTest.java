package com.relayforge.ingestion.event;

import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotencyBucketTest {
    @Test
    void matchesDocumentedStableRoutingVectors() {
        UUID organizationId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        assertThat(IdempotencyBucket.forKey(organizationId, "order-123")).isEqualTo(3410);
        assertThat(IdempotencyBucket.forKey(organizationId, "order-124")).isEqualTo(2152);
        assertThat(IdempotencyBucket.forKey(organizationId, "order-123")).isEqualTo(3410);
    }

    @Test
    void rejectsAmbiguousOrOversizedKeys() {
        UUID organizationId = UUID.randomUUID();
        for (String key : new String[]{"", " order", "order ", "line\nfeed", "a".repeat(256)}) {
            assertThatThrownBy(() -> IdempotencyBucket.forKey(organizationId, key))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
