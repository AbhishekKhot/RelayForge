package com.relayforge.apisupport;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class ApiErrorController implements ErrorController {
    @RequestMapping("/error")
    public ResponseEntity<ApiError> error(HttpServletRequest request) {
        Object attribute = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        int statusCode = attribute instanceof Integer code ? code : 500;
        HttpStatus status = HttpStatus.resolve(statusCode);
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        String code = status.is4xxClientError() ? "BAD_REQUEST" : "INTERNAL_ERROR";
        String message = status.is4xxClientError() ? "Request could not be processed" : "An unexpected error occurred";
        return ResponseEntity.status(status).body(new ApiError(code, message, RequestIdFilter.current(request)));
    }
}
