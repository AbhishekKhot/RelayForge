# B1 endpoint destination policy

`POST /v1/endpoints` accepts a destination only after URL, DNS, header, event type, and delivery policy validation. It inserts the endpoint, subscriptions, and audit record in one PostgreSQL transaction. B1 accepts `authentication.type=NONE`; credential configuration is B2.

Production destinations require HTTPS. The `local` Spring profile permits HTTP only for the exact `RELAYFORGE_SSRF_LOCAL_SIMULATOR_URL` host and port configured through the ignored `.env`. Registration rejects any DNS answer in private, loopback, link-local, multicast, reserved, or documentation ranges. A mixed public/private answer is rejected. The outbound client resolves and checks again for every send, and its own DNS resolver checks each new connection. It connects to the checked address while retaining the original hostname for TLS. Redirects and automatic retries are disabled.

The policy uses a shared `DestinationPolicy` and `SafeWebhookClient` so later delivery workers can enforce the same rules. B1 does not yet send queued deliveries; the worker integration is part of the delivery milestone. Production deployment should also restrict egress at the network layer.

Implementation follows the [OWASP SSRF prevention guidance](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html), the [IANA IPv4 special-purpose registry](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml), and [Apache HttpClient 5 DNS resolver and redirect controls](https://hc.apache.org/httpcomponents-client-5.5.x/current/httpclient5/apidocs/org/apache/hc/client5/http/impl/classic/HttpClientBuilder.html).
