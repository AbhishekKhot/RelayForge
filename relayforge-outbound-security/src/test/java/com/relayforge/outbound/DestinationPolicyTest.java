package com.relayforge.outbound;

import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DestinationPolicyTest {
    @Test
    void composesPublicHttpsDestinationAndEncodesQueryValues() throws Exception {
        DestinationPolicy policy = policy("public.example", "8.8.8.8");

        var result = policy.validate(new Destination("https://public.example", "/hooks/payment",
                Map.of("source", "relay forge", "id", "a&b"), Map.of("Content-Type", "application/json")));

        assertThat(result.uri().toASCIIString())
                .isEqualTo("https://public.example/hooks/payment?id=a%26b&source=relay+forge");
        assertThat(result.headers()).containsEntry("Content-Type", "application/json");
    }

    @Test
    void rejectsUnsafeSchemesCredentialsAndAmbiguousUrlParts() throws Exception {
        DestinationPolicy policy = policy("public.example", "8.8.8.8");
        for (String base : new String[]{"http://public.example", "file:///etc/passwd",
                "https://user:secret@public.example", "https://public.example/path",
                "https://public.example?q=x", "https://public.example#fragment",
                "https://public.example\\@internal", "https://public.example:0"}) {
            assertThatThrownBy(() -> policy.validate(destination(base)))
                    .isInstanceOf(InvalidDestinationException.class);
        }
    }

    @Test
    void rejectsPrivateMetadataReservedAndMixedDnsAnswers() throws Exception {
        for (String address : new String[]{"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1",
                "169.254.169.254", "100.64.0.1", "198.18.0.1", "192.0.2.1", "0.0.0.0",
                "224.0.0.1", "240.0.0.1", "::1", "fc00::1", "fe80::1", "2001:db8::1",
                "3ffe::1"}) {
            DestinationPolicy policy = policy("public.example", address);
            assertThatThrownBy(() -> policy.validate(destination("https://public.example")))
                    .isInstanceOf(UnsafeDestinationException.class);
        }
        DestinationPolicy mixed = new DestinationPolicy(host -> new InetAddress[]{
                InetAddress.getByName("8.8.8.8"), InetAddress.getByName("127.0.0.1")}, false, null);
        assertThatThrownBy(() -> mixed.validate(destination("https://public.example")))
                .isInstanceOf(UnsafeDestinationException.class);
    }

    @Test
    void allowsOnlyExplicitSimulatorHostAndPortInLocalMode() throws Exception {
        AddressLookup lookup = host -> new InetAddress[]{InetAddress.getByName("172.20.0.8")};
        URI simulator = URI.create("http://webhook-simulator:8080");
        DestinationPolicy local = new DestinationPolicy(lookup, true, simulator);
        DestinationPolicy production = new DestinationPolicy(lookup, false, simulator);

        assertThat(local.validate(destination("http://webhook-simulator:8080")).uri().getHost())
                .isEqualTo("webhook-simulator");
        assertThatThrownBy(() -> local.validate(destination("http://webhook-simulator:8081")))
                .isInstanceOf(InvalidDestinationException.class);
        assertThatThrownBy(() -> local.validate(destination("http://other-internal:8080")))
                .isInstanceOf(InvalidDestinationException.class);
        assertThatThrownBy(() -> production.validate(destination("http://webhook-simulator:8080")))
                .isInstanceOf(InvalidDestinationException.class);
    }

    @Test
    void rejectsUnsafePathHeadersAndQuery() throws Exception {
        DestinationPolicy policy = policy("public.example", "8.8.8.8");
        for (String path : new String[]{"//internal", "/../admin", "/hook?token=x", "/hook#fragment", "/x\\y"}) {
            assertThatThrownBy(() -> policy.validate(new Destination("https://public.example", path,
                    Map.of(), Map.of()))).isInstanceOf(InvalidDestinationException.class);
        }
        for (String header : new String[]{"Host", "Connection", "Content-Length", "Proxy-Authorization",
                "X-RelayForge-Signature", "Authorization", "Cookie"}) {
            assertThatThrownBy(() -> policy.validate(new Destination("https://public.example", "/hook",
                    Map.of(), Map.of(header, "value")))).isInstanceOf(InvalidDestinationException.class);
        }
        assertThatThrownBy(() -> policy.validate(new Destination("https://public.example", "/hook",
                Map.of("bad\nname", "value"), Map.of()))).isInstanceOf(InvalidDestinationException.class);
        assertThatThrownBy(() -> policy.validate(new Destination("https://public.example", "/hook",
                Map.of(), Map.of("X-Test", "bad\r\nvalue")))).isInstanceOf(InvalidDestinationException.class);
        Map<String, String> duplicateHeaders = new LinkedHashMap<>();
        duplicateHeaders.put("X-Trace", "one");
        duplicateHeaders.put("x-trace", "two");
        assertThatThrownBy(() -> policy.validate(new Destination("https://public.example", "/hook",
                Map.of(), duplicateHeaders))).isInstanceOf(InvalidDestinationException.class);
        assertThatThrownBy(() -> policy.validate(destination("https://[::ffff:127.0.0.1]")))
                .isInstanceOf(InvalidDestinationException.class);
    }

    private DestinationPolicy policy(String host, String address) throws Exception {
        return new DestinationPolicy(name -> new InetAddress[]{InetAddress.getByName(address)}, false, null);
    }

    private Destination destination(String baseUrl) {
        return new Destination(baseUrl, "/hook", Map.of(), Map.of());
    }
}
