package com.relayforge.apisupport;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ApiSupportTest {
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new ProbeController())
            .setControllerAdvice(new ApiExceptionHandler())
            .addFilters(new RequestIdFilter())
            .build();

    @Test
    void propagatesValidRequestId() throws Exception {
        mvc.perform(get("/probe").header("X-Request-Id", "probe-123"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Request-Id", "probe-123"));
    }

    @Test
    void replacesUnsafeRequestId() throws Exception {
        mvc.perform(get("/probe").header("X-Request-Id", "bad\r\nvalue"))
                .andExpect(status().isOk())
                .andExpect(header().exists("X-Request-Id"));
    }

    @Test
    void returnsStableValidationError() throws Exception {
        mvc.perform(post("/probe").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"\"}")
                        .header("X-Request-Id", "validation-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.requestId").value("validation-1"));
    }

    @Test
    void hidesUnexpectedExceptionDetails() throws Exception {
        mvc.perform(get("/boom").header("X-Request-Id", "failure-1"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred"))
                .andExpect(jsonPath("$.requestId").value("failure-1"));
    }

    @RestController
    static class ProbeController {
        @GetMapping("/probe")
        String probe() {
            return "ok";
        }

        @PostMapping("/probe")
        String validate(@Valid @RequestBody ProbeRequest request) {
            return request.name();
        }

        @GetMapping("/boom")
        String boom() {
            throw new IllegalStateException("private diagnostic");
        }
    }

    record ProbeRequest(@NotBlank String name) {
    }
}
