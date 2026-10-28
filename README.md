# RelayForge

RelayForge is a webhook delivery platform. Its architecture and implementation sequence are documented in the root design files.

## Current state

This repository contains a Maven reactor scaffold for the planned deployable roles. It has no runnable service or test suite yet. Database schemas, Docker images, and service contracts will follow the capacity architecture gate in the implementation checklist.

Run `mvn validate` to verify the Maven module layout. The target language level is Java 21; Maven currently runs with the JDK selected by the local environment.

## Modules

- `relayforge-control-plane`: accounts, credentials, endpoints, and subscriptions.
- `relayforge-ingestion` and `relayforge-ingestion-publisher`: event acceptance and publication.
- `relayforge-router`, `relayforge-delivery-worker`, `relayforge-delivery-publisher`, and `relayforge-retry-scheduler`: delivery roles.
- `relayforge-analytics-consumer` and `relayforge-analytics-api`: analytical ingestion and queries.
- `relayforge-contracts`: versioned cross-service message contracts.

See `RelayForge_High_Level_Design.md`, `RelayForge_Low_Level_Design.md`, and `RelayForge_Implementation_Checklist.md` before adding implementation code.
