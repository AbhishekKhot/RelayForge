package com.relayforge.runtime;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "relayforge")
public class RelayForgeSettings {
    @Valid
    private final Database database = new Database();
    @Valid
    private final Kafka kafka = new Kafka();
    @Valid
    private final Redis redis = new Redis();
    @Valid
    private final ClickHouse clickhouse = new ClickHouse();
    @Valid
    private final Api api = new Api();
    @Valid
    private final Webhook webhook = new Webhook();
    @Valid
    private final Retry retry = new Retry();
    @Valid
    private final Encryption encryption = new Encryption();
    @Valid
    private final Provisioning provisioning = new Provisioning();

    public Database getDatabase() { return database; }
    public Kafka getKafka() { return kafka; }
    public Redis getRedis() { return redis; }
    public ClickHouse getClickhouse() { return clickhouse; }
    public Api getApi() { return api; }
    public Webhook getWebhook() { return webhook; }
    public Retry getRetry() { return retry; }
    public Encryption getEncryption() { return encryption; }
    public Provisioning getProvisioning() { return provisioning; }

    public static class Database {
        @NotBlank
        private String url = "jdbc:postgresql://localhost:5432/relayforge";
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
    }

    public static class Kafka {
        @NotBlank
        private String bootstrapServers = "localhost:9092";
        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    }

    public static class Redis {
        @NotBlank
        private String uri = "redis://localhost:6379";
        public String getUri() { return uri; }
        public void setUri(String uri) { this.uri = uri; }
    }

    public static class ClickHouse {
        @NotBlank
        private String url = "http://localhost:8123";
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
    }

    public static class Api {
        @Min(1024)
        @Max(1048576)
        private int maxEventBytes = 262144;
        @Min(1)
        @Max(200)
        private int maxPageSize = 200;
        public int getMaxEventBytes() { return maxEventBytes; }
        public void setMaxEventBytes(int maxEventBytes) { this.maxEventBytes = maxEventBytes; }
        public int getMaxPageSize() { return maxPageSize; }
        public void setMaxPageSize(int maxPageSize) { this.maxPageSize = maxPageSize; }
    }

    public static class Webhook {
        private Duration connectTimeout = Duration.ofSeconds(3);
        private Duration readTimeout = Duration.ofSeconds(10);
        private Duration overallTimeout = Duration.ofSeconds(30);
        public Duration getConnectTimeout() { return connectTimeout; }
        public void setConnectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; }
        public Duration getReadTimeout() { return readTimeout; }
        public void setReadTimeout(Duration readTimeout) { this.readTimeout = readTimeout; }
        public Duration getOverallTimeout() { return overallTimeout; }
        public void setOverallTimeout(Duration overallTimeout) { this.overallTimeout = overallTimeout; }
        @jakarta.validation.constraints.AssertTrue
        public boolean isTimeoutOrderValid() {
            return connectTimeout != null && readTimeout != null && overallTimeout != null
                    && !connectTimeout.isNegative() && !connectTimeout.isZero()
                    && !readTimeout.isNegative() && !readTimeout.isZero()
                    && overallTimeout.compareTo(connectTimeout) >= 0
                    && overallTimeout.compareTo(readTimeout) >= 0;
        }
    }

    public static class Retry {
        @Min(1)
        @Max(20)
        private int maxAttempts = 10;
        private Duration maxHorizon = Duration.ofHours(24);
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        public Duration getMaxHorizon() { return maxHorizon; }
        public void setMaxHorizon(Duration maxHorizon) { this.maxHorizon = maxHorizon; }
        @jakarta.validation.constraints.AssertTrue
        public boolean isHorizonValid() {
            return maxHorizon != null && !maxHorizon.isNegative() && !maxHorizon.isZero();
        }
    }

    public static class Encryption {
        @NotBlank
        @Pattern(regexp = "local|managed")
        private String provider = "local";
        public String getProvider() { return provider; }
        public void setProvider(String provider) { this.provider = provider; }
    }

    public static class Provisioning {
        @Size(min = 32, max = 256)
        private String credential;
        public String getCredential() { return credential; }
        public void setCredential(String credential) { this.credential = credential; }
    }
}
