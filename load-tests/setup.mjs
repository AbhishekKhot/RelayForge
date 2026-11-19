import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { identifier, integer, localSettings, options } from './common.mjs';

export async function provision({ tenants, fanout, endpointsPerTenant = fanout,
    receiverUrl, runId, provisioningCredential, api }) {
    integer(tenants, 'tenants', 1, 1000);
    integer(fanout, 'fanout', 1, 1000);
    integer(endpointsPerTenant, 'endpointsPerTenant', fanout, 1000);
    identifier(runId, 'runId');
    if (!provisioningCredential || !['http:', 'https:'].includes(new URL(receiverUrl).protocol)) {
        throw new Error('Provisioning credential and HTTP receiver URL are required');
    }
    const eventType = `load.${runId}`;
    const manifest = { version: 1, run_id: runId, event_type: eventType, fanout,
        endpoints_per_tenant: endpointsPerTenant, tenants: [] };
    for (let tenant = 0; tenant < tenants; tenant += 1) {
        const admin = { 'X-Provisioning-Credential': provisioningCredential,
            'Idempotency-Key': `j3-${runId}-${tenant}` };
        const owner = await api('/v1/organizations', { name: `J3 ${runId} ${tenant}`,
            plan_code: 'STANDARD', quota_overrides: {
                events_per_minute: 60000, burst_events: 2000,
                max_active_endpoints: Math.max(25, endpointsPerTenant),
                max_in_flight_deliveries: 20000,
            } }, admin);
        const organizationId = owner.data.organization_id;
        const keyId = owner.data.api_key.split('_')[2];
        if (!keyId) throw new Error('Provisioning returned an invalid API key');
        const rotated = await api(`/v1/api-keys/${keyId}/rotate`,
            { scopes: ['events:write', 'endpoints:write'] }, admin);
        const bearer = { Authorization: `Bearer ${rotated.data.api_key}` };
        for (let endpoint = 0; endpoint < endpointsPerTenant; endpoint += 1) {
            await api('/v1/endpoints', {
                name: `J3 ${runId} ${tenant} ${endpoint}`,
                destination: { base_url: receiverUrl,
                    path: `/load/${runId}/${tenant}/${endpoint}`,
                    query_params: {}, headers: {}, metadata: {} },
                authentication: { type: 'NONE' },
                event_types: [endpoint < fanout ? eventType : `load.idle.${runId}`],
                delivery_policy: { timeout_ms: 60000, max_attempts: 3, max_concurrency: 1000 },
            }, bearer);
        }
        manifest.tenants.push({ organization_id: organizationId,
            api_key: rotated.data.api_key, event_type: eventType });
    }
    return manifest;
}

export function liveApi(baseUrl) {
    return async (path, body, headers) => {
        const response = await fetch(new URL(path, baseUrl), { method: 'POST',
            headers: { 'Content-Type': 'application/json', ...headers },
            body: JSON.stringify(body), signal: AbortSignal.timeout(15000) });
        if (!response.ok) throw new Error(`${path} returned HTTP ${response.status}`);
        return response.json();
    };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    try {
        const flags = options(process.argv.slice(2));
        const settings = await localSettings();
        const runId = identifier(flags['run-id'] ?? randomUUID().slice(0, 12), 'run-id');
        const output = flags.output ?? 'load-tests/local.credentials.json';
        const manifest = await provision({
            tenants: integer(flags.tenants ?? 1, 'tenants', 1, 1000),
            fanout: integer(flags.fanout ?? 3, 'fanout', 1, 1000),
            endpointsPerTenant: flags['endpoints-per-tenant'] === undefined ? undefined
                : integer(flags['endpoints-per-tenant'], 'endpoints-per-tenant', 1, 1000),
            receiverUrl: settings.WEBHOOK_SIMULATOR_URL,
            provisioningCredential: settings.TRUSTED_PROVISIONING_CREDENTIAL,
            runId,
            api: liveApi(`http://127.0.0.1:${settings.CONTROL_API_HOST_PORT}`),
        });
        await writeFile(output, JSON.stringify(manifest, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
        console.log(JSON.stringify({ run_id: runId, tenants: manifest.tenants.length,
            fanout: manifest.fanout, endpoints_per_tenant: manifest.endpoints_per_tenant,
            active_endpoints: manifest.tenants.length * manifest.endpoints_per_tenant,
            manifest: output }));
    } catch (error) {
        console.error(error.message);
        process.exitCode = 1;
    }
}
