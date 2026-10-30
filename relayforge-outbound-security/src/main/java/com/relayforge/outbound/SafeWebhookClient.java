package com.relayforge.outbound;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
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
    private final CloseableHttpClient client;
    private final DestinationPolicy policy;

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
        if (destination == null || body == null || timeout == null || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofSeconds(60)) > 0) {
            throw new IllegalArgumentException("Invalid webhook request");
        }
        policy.revalidate(destination);
        HttpPost request = new HttpPost(destination.uri());
        destination.headers().forEach(request::addHeader);
        request.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
        request.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.ofSeconds(2))
                .setConnectTimeout(Timeout.ofSeconds(2))
                .setResponseTimeout(Timeout.ofMilliseconds(timeout.toMillis()))
                .build());
        return client.execute(request, response -> response.getCode());
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
