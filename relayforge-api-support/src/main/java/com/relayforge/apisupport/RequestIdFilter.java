package com.relayforge.apisupport;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public final class RequestIdFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Request-Id";
    public static final String ATTRIBUTE = RequestIdFilter.class.getName() + ".id";
    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9_-]{1,100}");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader(HEADER);
        String requestId = supplied != null && SAFE_ID.matcher(supplied).matches()
                ? supplied : UUID.randomUUID().toString();
        request.setAttribute(ATTRIBUTE, requestId);
        response.setHeader(HEADER, requestId);
        MDC.put("requestId", requestId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove("requestId");
        }
    }

    public static String current(HttpServletRequest request) {
        Object value = request.getAttribute(ATTRIBUTE);
        return value instanceof String id ? id : "";
    }
}
