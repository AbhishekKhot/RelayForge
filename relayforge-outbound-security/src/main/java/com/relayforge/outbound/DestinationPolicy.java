package com.relayforge.outbound;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

public final class DestinationPolicy {
    private static final Pattern DNS_NAME = Pattern.compile("[A-Za-z0-9.-]{1,253}");
    private static final Pattern HEADER_NAME = Pattern.compile("[A-Za-z0-9-]{1,80}");
    private static final Pattern QUERY_NAME = Pattern.compile("[A-Za-z0-9_.~-]{1,128}");
    private static final Set<String> BLOCKED_HEADERS = Set.of(
            "host", "connection", "content-length", "transfer-encoding", "te", "trailer", "upgrade",
            "keep-alive", "authorization", "proxy-authorization", "cookie", "set-cookie", "expect");

    private final AddressLookup lookup;
    private final boolean localMode;
    private final URI localSimulator;

    public DestinationPolicy(AddressLookup lookup, boolean localMode, URI localSimulator) {
        this.lookup = lookup;
        this.localMode = localMode;
        this.localSimulator = localSimulator;
    }

    public ValidatedDestination validate(Destination destination) {
        if (destination == null || destination.baseUrl() == null
                || destination.baseUrl().length() > 2048 || destination.baseUrl().indexOf('\\') >= 0) {
            throw new InvalidDestinationException();
        }
        URI base;
        try {
            base = URI.create(destination.baseUrl());
        } catch (IllegalArgumentException malformed) {
            throw new InvalidDestinationException();
        }
        String scheme = base.getScheme();
        String host = base.getHost();
        if (scheme == null || host == null || base.getRawUserInfo() != null || base.getRawQuery() != null
                || base.getRawFragment() != null || base.isOpaque()
                || base.getRawPath() != null && !base.getRawPath().isEmpty() && !base.getRawPath().equals("/")) {
            throw new InvalidDestinationException();
        }
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);
        String normalizedHost = host.toLowerCase(Locale.ROOT);
        if (!DNS_NAME.matcher(normalizedHost).matches() && !isBracketedIpv6(normalizedHost)) {
            throw new InvalidDestinationException();
        }
        if (normalizedHost.endsWith(".") || normalizedHost.contains("..")
                || normalizedHost.startsWith("-")) {
            throw new InvalidDestinationException();
        }
        int port = base.getPort();
        if (port == 0 || port > 65535) {
            throw new InvalidDestinationException();
        }
        boolean simulator = isLocalSimulator(normalizedScheme, normalizedHost, port);
        if (!normalizedScheme.equals("https") && !(normalizedScheme.equals("http") && simulator)) {
            throw new InvalidDestinationException();
        }
        validatePath(destination.path());
        Map<String, String> headers = validateHeaders(destination.headers());
        String query = encodeQuery(destination.queryParams());
        String lookupHost = unbracket(normalizedHost);
        resolve(lookupHost, simulator);
        try {
            URI result = new URI(normalizedScheme, null, lookupHost, port, destination.path(), null, null);
            if (!query.isEmpty()) {
                result = URI.create(result.toASCIIString() + "?" + query);
            }
            return new ValidatedDestination(result, headers);
        } catch (URISyntaxException | IllegalArgumentException malformed) {
            throw new InvalidDestinationException();
        }
    }

    InetAddress[] resolveForConnection(String host) throws UnknownHostException {
        boolean simulator = localMode && localSimulator != null
                && unbracket(localSimulator.getHost()).equalsIgnoreCase(unbracket(host));
        return resolve(unbracket(host), simulator);
    }

    public void revalidate(ValidatedDestination destination) {
        if (destination == null) {
            throw new InvalidDestinationException();
        }
        URI uri = destination.uri();
        resolve(unbracket(uri.getHost()), isLocalSimulator(uri.getScheme(), uri.getHost(), uri.getPort()));
    }

    private InetAddress[] resolve(String host, boolean simulator) {
        InetAddress[] addresses;
        try {
            addresses = lookup.lookup(host);
        } catch (UnknownHostException unavailable) {
            throw new UnsafeDestinationException();
        }
        if (addresses == null || addresses.length == 0) {
            throw new UnsafeDestinationException();
        }
        for (InetAddress address : addresses) {
            if (address == null || !simulator && !isPublicAddress(address)) {
                throw new UnsafeDestinationException();
            }
        }
        return addresses.clone();
    }

    private boolean isLocalSimulator(String scheme, String host, int port) {
        if (!localMode || localSimulator == null || localSimulator.getHost() == null) {
            return false;
        }
        return scheme.equalsIgnoreCase(localSimulator.getScheme())
                && host.equalsIgnoreCase(localSimulator.getHost())
                && port == localSimulator.getPort();
    }

    private boolean isBracketedIpv6(String host) {
        return host.startsWith("[") && host.endsWith("]") && host.indexOf(':') > 0
                && !host.startsWith("[::ffff:");
    }

    private String unbracket(String host) {
        return host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
    }

    private boolean isPublicAddress(InetAddress address) {
        byte[] bytes = address.getAddress();
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return false;
        }
        if (bytes.length == 4) {
            int first = bytes[0] & 0xff;
            int second = bytes[1] & 0xff;
            int third = bytes[2] & 0xff;
            return first >= 1 && first < 224 && first != 10 && first != 127
                    && !(first == 100 && second >= 64 && second <= 127)
                    && !(first == 169 && second == 254)
                    && !(first == 172 && second >= 16 && second <= 31)
                    && !(first == 192 && second == 0 && third == 0)
                    && !(first == 192 && second == 0 && third == 2)
                    && !(first == 192 && second == 88 && third == 99)
                    && !(first == 192 && second == 168)
                    && !(first == 198 && second == 18 || first == 198 && second == 19)
                    && !(first == 198 && second == 51 && third == 100)
                    && !(first == 203 && second == 0 && third == 113);
        }
        if (bytes.length == 16) {
            int first = bytes[0] & 0xff;
            int second = bytes[1] & 0xff;
            int third = bytes[2] & 0xff;
            int fourth = bytes[3] & 0xff;
            return (first & 0xe0) == 0x20
                    && !(first == 0x20 && second == 0x01 && third < 2)
                    && !(first == 0x20 && second == 0x01 && third == 0x0d && fourth == 0xb8)
                    && !(first == 0x20 && second == 0x02)
                    && !(first == 0x3f && second >= 0xf0);
        }
        return false;
    }

    private void validatePath(String path) {
        if (path == null || path.length() > 2048 || !path.startsWith("/") || path.startsWith("//")
                || path.indexOf('\\') >= 0 || path.indexOf('?') >= 0 || path.indexOf('#') >= 0
                || path.indexOf('%') >= 0 || hasControl(path)
                || Arrays.stream(path.split("/", -1)).anyMatch(segment -> segment.equals(".") || segment.equals(".."))) {
            throw new InvalidDestinationException();
        }
    }

    private Map<String, String> validateHeaders(Map<String, String> headers) {
        if (headers == null || headers.size() > 32) {
            throw new InvalidDestinationException();
        }
        int total = 0;
        Set<String> names = new HashSet<>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (name == null || !HEADER_NAME.matcher(name).matches() || value == null || value.length() > 1024
                    || hasControl(value) || BLOCKED_HEADERS.contains(name.toLowerCase(Locale.ROOT))
                    || name.toLowerCase(Locale.ROOT).startsWith("proxy-")
                    || name.toLowerCase(Locale.ROOT).startsWith("x-relayforge-")
                    || !names.add(name.toLowerCase(Locale.ROOT))) {
                throw new InvalidDestinationException();
            }
            total += name.length() + value.length();
        }
        if (total > 8192) {
            throw new InvalidDestinationException();
        }
        return Map.copyOf(headers);
    }

    private String encodeQuery(Map<String, String> queryParams) {
        if (queryParams == null || queryParams.size() > 32) {
            throw new InvalidDestinationException();
        }
        if (queryParams.keySet().stream().anyMatch(key -> key == null)) {
            throw new InvalidDestinationException();
        }
        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : new TreeMap<>(queryParams).entrySet()) {
            if (entry.getKey() == null || !QUERY_NAME.matcher(entry.getKey()).matches()
                    || entry.getValue() == null || entry.getValue().length() > 1024
                    || hasControl(entry.getValue())) {
                throw new InvalidDestinationException();
            }
            if (!query.isEmpty()) {
                query.append('&');
            }
            query.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
            query.append('=');
            query.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }
        if (query.length() > 4096) {
            throw new InvalidDestinationException();
        }
        return query.toString();
    }

    private boolean hasControl(String value) {
        return value.codePoints().anyMatch(character -> character < 0x20 || character == 0x7f);
    }
}
