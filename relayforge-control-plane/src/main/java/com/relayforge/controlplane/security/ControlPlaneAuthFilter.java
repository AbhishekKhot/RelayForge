package com.relayforge.controlplane.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relayforge.apisupport.ApiError;
import com.relayforge.apisupport.RequestIdFilter;
import com.relayforge.controlplane.organization.OrganizationService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(1)
@ConditionalOnProperty(name = "relayforge.database.username")
public final class ControlPlaneAuthFilter extends OncePerRequestFilter {
    public static final String ORGANIZATION_ATTRIBUTE = ControlPlaneAuthFilter.class.getName() + ".organizationId";

    private final OrganizationService organizations;
    private final ObjectMapper json;
    private final byte[] provisioningCredential;

    public ControlPlaneAuthFilter(OrganizationService organizations, ObjectMapper json,
            @Value("${relayforge.provisioning.credential}") String provisioningCredential) {
        this.organizations = organizations;
        this.json = json;
        this.provisioningCredential = provisioningCredential.getBytes(StandardCharsets.UTF_8);
        if (this.provisioningCredential.length < 32) {
            throw new IllegalStateException("A strong provisioning credential is required");
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        if (!path.startsWith("/v1/")) {
            chain.doFilter(request, response);
            return;
        }
        if (isProvisioningPath(path)) {
            String supplied = request.getHeader("X-Provisioning-Credential");
            if (supplied == null || supplied.length() > 256 || !MessageDigest.isEqual(
                    provisioningCredential, supplied.getBytes(StandardCharsets.UTF_8))) {
                reject(request, response, HttpStatus.UNAUTHORIZED);
                return;
            }
            chain.doFilter(request, response);
            return;
        }
        String requiredScope = requiredScope(path, request.getMethod());
        if (requiredScope == null) {
            reject(request, response, HttpStatus.FORBIDDEN);
            return;
        }
        String authorization = request.getHeader("Authorization");
        if (authorization == null || !authorization.startsWith("Bearer ") || authorization.length() > 135) {
            reject(request, response, HttpStatus.UNAUTHORIZED);
            return;
        }
        OrganizationService.Authentication result;
        try {
            result = organizations.authenticate(authorization.substring(7), requiredScope);
        } catch (org.springframework.dao.DataAccessException unavailable) {
            reject(request, response, HttpStatus.SERVICE_UNAVAILABLE);
            return;
        }
        if (result.status() != HttpStatus.OK) {
            reject(request, response, result.status());
            return;
        }
        request.setAttribute(ORGANIZATION_ATTRIBUTE, result.organizationId());
        chain.doFilter(request, response);
    }

    private boolean isProvisioningPath(String path) {
        return path.equals("/v1/organizations") || path.equals("/v1/users")
                || path.equals("/v1/organizations/recover")
                || path.startsWith("/v1/api-keys/");
    }

    private String requiredScope(String path, String method) {
        if (path.equals("/v1/auth/check") && method.equals("GET")) {
            return "events:write";
        }
        if (path.startsWith("/v1/organizations/") && method.equals("GET")) {
            return "organizations:read";
        }
        if (path.startsWith("/v1/events")) {
            return method.equals("GET") ? "events:read" : "events:write";
        }
        if (path.startsWith("/v1/endpoints") || path.startsWith("/v1/subscriptions")) {
            return method.equals("GET") ? "endpoints:read" : "endpoints:write";
        }
        if (path.startsWith("/v1/deliveries")) {
            return method.equals("GET") ? "deliveries:read" : "deliveries:replay";
        }
        if (path.startsWith("/v1/analytics")) {
            return "analytics:read";
        }
        return null;
    }

    private void reject(HttpServletRequest request, HttpServletResponse response, HttpStatus status)
            throws IOException {
        response.setStatus(status.value());
        response.setContentType("application/json");
        String code = status == HttpStatus.FORBIDDEN ? "FORBIDDEN"
                : status == HttpStatus.SERVICE_UNAVAILABLE ? "SERVICE_UNAVAILABLE" : "UNAUTHORIZED";
        json.writeValue(response.getOutputStream(), new ApiError(code, "Request could not be authorized",
                RequestIdFilter.current(request)));
    }
}
