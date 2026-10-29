package com.relayforge.apisupport;

@FunctionalInterface
public interface ReadinessProbe {
    boolean available() throws Exception;
}
