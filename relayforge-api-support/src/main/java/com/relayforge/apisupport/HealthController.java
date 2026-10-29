package com.relayforge.apisupport;

import java.util.List;
import java.util.Map;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class HealthController {
    private final ApplicationAvailability availability;
    private final List<ReadinessProbe> probes;

    public HealthController(ApplicationAvailability availability, List<ReadinessProbe> probes) {
        this.availability = availability;
        this.probes = List.copyOf(probes);
    }

    @GetMapping("/health/live")
    public Map<String, String> live() {
        return Map.of("status", "UP");
    }

    @GetMapping("/health/ready")
    public ResponseEntity<Map<String, String>> ready() {
        boolean ready = availability.getReadinessState() == ReadinessState.ACCEPTING_TRAFFIC;
        if (ready) {
            for (ReadinessProbe probe : probes) {
                try {
                    if (!probe.available()) {
                        ready = false;
                        break;
                    }
                } catch (Exception unavailable) {
                    ready = false;
                    break;
                }
            }
        }
        return ResponseEntity.status(ready ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", ready ? "UP" : "DOWN"));
    }
}
