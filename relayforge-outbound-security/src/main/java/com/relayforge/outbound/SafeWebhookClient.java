package com.relayforge.outbound;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.util.Timeout;

public final class SafeWebhookClient implements AutoCloseable {
    private static final Pattern HEADER_NAME = Pattern.compile("[A-Za-z0-9-]{1,80}");
    private static final Set<String> RESERVED = Set.of("x-relayforge-event-id",
            "x-relayforge-delivery-id", "x-relayforge-event-type", "x-relayforge-timestamp",
            "x-relayforge-signature");
    private static final Set<String> BLOCKED = Set.of("host", "connection", "content-length",
            "transfer-encoding", "te", "trailer", "upgrade", "keep-alive", "expect",
            "proxy-authorization", "cookie", "set-cookie", "content-type");
    private final CloseableHttpClient client;
    private final DestinationPolicy policy;
    private final java.util.concurrent.ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();

    public SafeWebhookClient(DestinationPolicy policy) {
        this.policy = policy;
        DnsResolver resolver = new DnsResolver() {
            @Override
            public InetAddress[] resolve(String host) throws UnknownHostException {
                return policy.resolveForConnection(host);
            }

            @Override
            public String resolveCanonicalHostname(String host) {
                return host;
            }
        };
        var connections = PoolingHttpClientConnectionManagerBuilder.create()
                .setDnsResolver(resolver)
                .setMaxConnTotal(200)
                .setMaxConnPerRoute(50)
                .build();
        this.client = HttpClients.custom()
                .setConnectionManager(connections)
                .disableRedirectHandling()
                .disableAutomaticRetries()
                .build();
    }

    public int send(ValidatedDestination destination, byte[] body, Duration timeout) throws IOException {
        return send(destination, body, timeout, Map.of());
    }

    public int send(ValidatedDestination destination, byte[] body, Duration timeout,
            Map<String, String> workerHeaders) throws IOException {
        return send(destination, body, timeout, workerHeaders, () -> true).status();
    }

    public Result send(ValidatedDestination destination, byte[] body, Duration timeout,
            Map<String, String> workerHeaders, BooleanSupplier leaseValid) throws IOException {
        if (destination == null || body == null || timeout == null || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofSeconds(60)) > 0 || workerHeaders == null
                || workerHeaders.size() > 16 || leaseValid == null) {
            throw new IllegalArgumentException("Invalid webhook request");
        }
        workerHeaders.forEach((name, value) -> {
            if (name == null || !HEADER_NAME.matcher(name).matches() || value == null
                    || value.length() > 4096 || value.codePoints().anyMatch(character ->
                    character < 0x20 || character == 0x7f)) {
                throw new IllegalArgumentException("Invalid worker header");
            }
            String normalized = name.toLowerCase(Locale.ROOT);
            if (BLOCKED.contains(normalized) || normalized.startsWith("proxy-")
                    || normalized.startsWith("x-relayforge-") && !RESERVED.contains(normalized)) {
                throw new IllegalArgumentException("Invalid worker header");
            }
        });
        HttpPost request = new HttpPost(destination.uri());
        destination.headers().forEach(request::addHeader);
        workerHeaders.forEach(request::addHeader);
        request.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
        request.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.ofSeconds(2))
                .setConnectTimeout(Timeout.ofSeconds(2))
                .setResponseTimeout(Timeout.ofMilliseconds(timeout.toMillis()))
                .build());
        Future<Result> pending = requests.submit(() -> {
            policy.revalidate(destination);
            return client.execute(request, response -> {
            String excerpt = "";
            if (response.getEntity() != null) {
                try (InputStream stream = response.getEntity().getContent()) {
                    byte[] bytes = stream.readNBytes(1025);
                    excerpt = sanitize(bytes);
                }
            }
            String retryAfter = null;
            var header = response.getFirstHeader("Retry-After");
            if (header != null && header.getValue() != null && header.getValue().length() <= 80) {
                retryAfter = header.getValue();
            }
            return new Result(response.getCode(), excerpt, retryAfter);
            });
        });
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (true) {
                if (!leaseValid.getAsBoolean()) {
                    throw new IOException("Delivery lease was lost");
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("Webhook deadline exceeded");
                }
                try {
                    return pending.get(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(250)),
                            TimeUnit.NANOSECONDS);
                } catch (TimeoutException stillRunning) {
                    // Recheck the overall deadline and lease before waiting again.
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Webhook interrupted", interrupted);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof UnsafeDestinationException unsafe) {
                throw unsafe;
            }
            if (failed.getCause() instanceof InvalidDestinationException invalid) {
                throw invalid;
            }
            if (failed.getCause() instanceof IOException unavailable) {
                throw unavailable;
            }
            throw new IOException("Webhook request failed", failed.getCause());
        } catch (RuntimeException invalid) {
            if (invalid instanceof UnsafeDestinationException
                    || invalid instanceof InvalidDestinationException) {
                throw invalid;
            }
            throw new IOException("Webhook request failed", invalid);
        } finally {
            if (!pending.isDone()) {
                request.cancel();
                pending.cancel(true);
            }
        }
    }

    private String sanitize(byte[] bytes) {
        String text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        StringBuilder result = new StringBuilder(Math.min(text.length(), 1024));
        for (int index = 0; index < text.length() && result.length() < 1024; index++) {
            char value = text.charAt(index);
            result.append(value >= 0x20 && value != 0x7f && value <= 0x7e ? value : ' ');
        }
        return result.toString();
    }

    public record Result(int status, String excerpt, String retryAfter) {
        public Result(int status, String excerpt) {
            this(status, excerpt, null);
        }
    }

    @Override
    public void close() throws IOException {
        requests.shutdownNow();
        client.close();
    }
}
