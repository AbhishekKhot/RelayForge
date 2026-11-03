package com.relayforge.deliveryworker;

import com.fasterxml.jackson.databind.JsonNode;
import com.relayforge.deliveryworker.http.DeliveryReferenceClient;
import com.relayforge.deliveryworker.http.WebhookRequestFactory;
import com.relayforge.deliveryworker.auth.AuthMaterial;
import com.relayforge.deliveryworker.auth.AuthenticationStrategyResolver;
import com.relayforge.deliveryworker.auth.StaticAuthenticationStrategy;
import com.relayforge.deliveryworker.auth.TokenExchangeService;
import com.relayforge.deliveryworker.lease.DeliveryAttemptService;
import com.relayforge.deliveryworker.lease.DeliveryClaimService;
import com.relayforge.deliveryworker.retry.RetryPolicy;
import com.relayforge.outbound.Destination;
import com.relayforge.outbound.DestinationPolicy;
import com.relayforge.outbound.SafeWebhookClient;
import com.relayforge.outbound.ValidatedDestination;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

public final class DeliveryProcessor implements AutoCloseable {
    private final DeliveryClaimService claims;
    private final DeliveryAttemptService attempts;
    private final DeliveryReferenceClient references;
    private final WebhookRequestFactory requests;
    private final DestinationPolicy policy;
    private final SafeWebhookClient client;
    private final RetryPolicy retryPolicy;
    private final AuthenticationStrategyResolver authentication;
    private final Clock clock;
    private final String workerId;
    private final java.util.concurrent.ScheduledExecutorService renewals =
            Executors.newScheduledThreadPool(2);

    public DeliveryProcessor(DeliveryClaimService claims, DeliveryAttemptService attempts,
            DeliveryReferenceClient references, WebhookRequestFactory requests,
            DestinationPolicy policy, SafeWebhookClient client, RetryPolicy retryPolicy,
            Clock clock, String workerId) {
        this(claims, attempts, references, requests, policy, client, retryPolicy,
                new AuthenticationStrategyResolver(new StaticAuthenticationStrategy(),
                        (configuration, lease, leaseValid) -> {
                            throw new IllegalStateException("Token strategy is unavailable");
                        }), clock, workerId);
    }

    public DeliveryProcessor(DeliveryClaimService claims, DeliveryAttemptService attempts,
            DeliveryReferenceClient references, WebhookRequestFactory requests,
            DestinationPolicy policy, SafeWebhookClient client, RetryPolicy retryPolicy,
            AuthenticationStrategyResolver authentication, Clock clock, String workerId) {
        this.claims = Objects.requireNonNull(claims);
        this.attempts = Objects.requireNonNull(attempts);
        this.references = Objects.requireNonNull(references);
        this.requests = Objects.requireNonNull(requests);
        this.policy = Objects.requireNonNull(policy);
        this.client = Objects.requireNonNull(client);
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.authentication = Objects.requireNonNull(authentication);
        this.clock = Objects.requireNonNull(clock);
        this.workerId = Objects.requireNonNull(workerId);
    }

    public void process(UUID deliveryId) {
        DeliveryAttemptService.WorkReference candidate = attempts.candidate(deliveryId);
        if (candidate == null) {
            return;
        }
        JsonNode endpoint;
        try {
            endpoint = references.endpoint(candidate.organizationId(), candidate.endpointId());
        } catch (IOException unavailable) {
            attempts.deferUnclaimed(deliveryId, clock.instant().plusSeconds(30));
            return;
        }
        int concurrency = endpoint.path("max_concurrency").asInt(0);
        if (concurrency < 1 || concurrency > 1000) {
            attempts.rejectUnclaimed(deliveryId, "CONFIGURATION_INVALID");
            return;
        }
        int maxAttempts = endpoint.path("max_attempts").asInt(0);
        if (maxAttempts < 1 || maxAttempts > 20) {
            attempts.rejectUnclaimed(deliveryId, "CONFIGURATION_INVALID");
            return;
        }
        if (candidate.attemptCount() >= maxAttempts) {
            attempts.finishExhausted(deliveryId, maxAttempts);
            return;
        }
        long configurationVersion = endpoint.path("version").asLong(-1);
        if (configurationVersion < 1) {
            attempts.rejectUnclaimed(deliveryId, "CONFIGURATION_INVALID");
            return;
        }
        var claimed = claims.claim(deliveryId, workerId, concurrency, 200);
        if (claimed.isEmpty()) {
            return;
        }
        DeliveryClaimService.Lease lease = claimed.get();
        AtomicReference<DeliveryClaimService.Lease> active = new AtomicReference<>(lease);
        AtomicBoolean lost = new AtomicBoolean();
        ScheduledFuture<?> renewal = renewals.scheduleAtFixedRate(() -> {
            synchronized (active) {
                if (!lost.get()) {
                    try {
                        var renewed = claims.renew(active.get());
                        if (renewed.isPresent()) {
                            active.set(renewed.get());
                        } else {
                            lost.set(true);
                        }
                    } catch (RuntimeException unavailable) {
                        lost.set(true);
                    }
                }
            }
        }, 20, 20, TimeUnit.SECONDS);
        try {
            String locator = attempts.eventLocator(lease);
            endpoint = references.endpoint(lease.organizationId(), lease.endpointId());
            if (endpoint.path("version").asLong(-1) != configurationVersion) {
                return;
            }
            JsonNode payload = references.payload(lease.organizationId(), locator);
            ValidatedDestination destination;
            int timeoutMillis;
            try {
                verify(endpoint, payload, lease, locator);
                destination = policy.validate(new Destination(
                        required(endpoint, "base_url"), required(endpoint, "path"),
                        stringMap(endpoint.path("query_params")),
                        stringMap(endpoint.path("headers"))));
                timeoutMillis = endpoint.path("timeout_ms").asInt(0);
                if (timeoutMillis < 100 || timeoutMillis > 60000) {
                    throw new IllegalStateException("Invalid delivery configuration");
                }
                AuthMaterial preview = authentication.dynamic(endpoint.path("authentication"))
                        ? new AuthMaterial(Map.of())
                        : authentication.resolve(endpoint.path("authentication"), active.get(),
                                () -> !lost.get());
                requests.build(lease.deliveryId(), locator, required(payload, "event_type"),
                        1, payload.path("payload"), preview,
                        required(endpoint, "signing_secret"), clock.instant());
            } catch (RuntimeException invalid) {
                synchronized (active) {
                    if (!lost.get()) {
                        attempts.rejectClaimed(active.get(), "CONFIGURATION_INVALID");
                    }
                }
                return;
            }
            AuthMaterial material;
            try {
                material = authentication.resolve(endpoint.path("authentication"), active.get(),
                        () -> !lost.get());
            } catch (TokenExchangeService.TokenFailure failure) {
                synchronized (active) {
                    if (!lost.get()) {
                        if (failure.retryable()) {
                            attempts.deferClaimed(active.get(), clock.instant().plusSeconds(30),
                                    failure.code());
                        } else {
                            attempts.rejectClaimed(active.get(), failure.code());
                        }
                    }
                }
                return;
            } catch (RuntimeException invalid) {
                synchronized (active) {
                    if (!lost.get()) {
                        attempts.rejectClaimed(active.get(), "CONFIGURATION_INVALID");
                    }
                }
                return;
            }
            DeliveryAttemptService.Attempt attempt;
            synchronized (active) {
                if (lost.get() || !claims.owns(active.get())) {
                    return;
                }
                attempt = attempts.reserve(active.get(), maxAttempts);
            }
            WebhookRequestFactory.Request request = requests.build(lease.deliveryId(), locator,
                    required(payload, "event_type"), attempt.number(), payload.path("payload"),
                    material, required(endpoint, "signing_secret"), clock.instant());
            int httpStatus = 0;
            String excerpt = null;
            String retryAfter = null;
            RetryPolicy.Failure failure = RetryPolicy.Failure.NONE;
            try {
                SafeWebhookClient.Result response = client.send(destination, request.body(),
                        Duration.ofMillis(timeoutMillis), request.headers(), () -> !lost.get());
                httpStatus = response.status();
                excerpt = redact(response.excerpt(), endpoint, request);
                retryAfter = response.retryAfter();
                if (httpStatus == 401 && authentication.dynamic(endpoint.path("authentication"))
                        && !lost.get()) {
                    AuthMaterial refreshed = authentication.resolve(endpoint.path("authentication"),
                            active.get(), () -> !lost.get());
                    request = requests.build(lease.deliveryId(), locator,
                            required(payload, "event_type"), attempt.number(), payload.path("payload"),
                            refreshed, required(endpoint, "signing_secret"), clock.instant());
                    response = client.send(destination, request.body(), Duration.ofMillis(timeoutMillis),
                            request.headers(), () -> !lost.get());
                    httpStatus = response.status();
                    excerpt = redact(response.excerpt(), endpoint, request);
                    retryAfter = response.retryAfter();
                }
            } catch (TokenExchangeService.TokenFailure unavailable) {
                failure = unavailable.retryable() ? RetryPolicy.Failure.NETWORK
                        : RetryPolicy.Failure.SECURITY;
                httpStatus = 0;
            } catch (IOException unavailable) {
                failure = failureFor(unavailable);
            } catch (RuntimeException rejected) {
                failure = RetryPolicy.Failure.SECURITY;
            }
            RetryPolicy.Decision decision = retryPolicy.decide(httpStatus, failure,
                    attempt.number(), maxAttempts, candidate.createdAt(),
                    candidate.eventExpiresAt(), retryAfter);
            synchronized (active) {
                if (!lost.get()) {
                    attempts.finish(active.get(), attempt, httpStatus, decision.errorCode(),
                            excerpt, decision.nextAttemptAt());
                }
            }
        } catch (IOException unavailable) {
            throw new IllegalStateException("Delivery reference is unavailable", unavailable);
        } finally {
            renewal.cancel(true);
        }
    }

    static RetryPolicy.Failure failureFor(IOException unavailable) {
        for (Throwable cause = unavailable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SSLHandshakeException || cause instanceof SSLPeerUnverifiedException) {
                return RetryPolicy.Failure.TLS;
            }
        }
        return RetryPolicy.Failure.NETWORK;
    }

    private void verify(JsonNode endpoint, JsonNode payload, DeliveryClaimService.Lease lease,
            String locator) {
        if (!lease.organizationId().toString().equals(required(endpoint, "organization_id"))
                || !lease.endpointId().toString().equals(required(endpoint, "endpoint_id"))
                || !lease.organizationId().toString().equals(required(payload, "organization_id"))
                || !locator.equals(required(payload, "locator"))
                || !payload.path("payload").isObject()) {
            throw new IllegalStateException("Delivery reference does not match lease");
        }
    }

    private String required(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalStateException("Delivery reference is invalid");
        }
        return value.asText();
    }

    private Map<String, String> stringMap(JsonNode node) {
        if (!node.isObject() || node.size() > 32) {
            throw new IllegalStateException("Endpoint parameters are invalid");
        }
        Map<String, String> values = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> {
            if (!entry.getValue().isTextual()) {
                throw new IllegalStateException("Endpoint parameters are invalid");
            }
            values.put(entry.getKey(), entry.getValue().asText());
        });
        return values;
    }

    private String redact(String excerpt, JsonNode endpoint, WebhookRequestFactory.Request request) {
        if (excerpt == null || excerpt.isEmpty()) {
            return null;
        }
        String result = excerpt;
        String signing = required(endpoint, "signing_secret");
        result = result.replace(signing, "[REDACTED]");
        JsonNode authentication = endpoint.path("authentication");
        for (String name : new String[]{"token", "password", "value"}) {
            String secret = authentication.path(name).asText("");
            if (!secret.isEmpty()) {
                result = result.replace(secret, "[REDACTED]");
            }
        }
        String authorization = request.headers().get("Authorization");
        if (authorization != null) {
            result = result.replace(authorization, "[REDACTED]");
            int separator = authorization.indexOf(' ');
            if (separator >= 0 && separator + 1 < authorization.length()) {
                result = result.replace(authorization.substring(separator + 1), "[REDACTED]");
            }
        }
        return result.length() > 1000 ? result.substring(0, 1000) : result;
    }

    @Override
    public void close() {
        renewals.shutdownNow();
    }
}
