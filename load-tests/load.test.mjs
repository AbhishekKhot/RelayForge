import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, test } from 'node:test';
import { once } from 'node:events';
import { promisify } from 'node:util';
import { buildEventBody, plannedStartDelay, producerBaseUrl, runProducer } from './producer.mjs';
import { createReceiver } from './receiver.mjs';
import { provision } from './setup.mjs';

const servers = [];
const execFileAsync = promisify(execFile);
after(async () => Promise.all(servers.map(server => new Promise(resolve => server.close(resolve)))));

async function listen(server) {
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    servers.push(server);
    return `http://127.0.0.1:${server.address().port}`;
}

test('event size is exact and each event has a distinct idempotency key', () => {
    const first = buildEventBody('load.created', 1024, 'run-a', 1);
    const second = buildEventBody('load.created', 1024, 'run-a', 2);
    assert.equal(Buffer.byteLength(first), 1024);
    assert.equal(JSON.parse(first).data.sequence, 1);
    assert.equal(JSON.parse(second).data.sequence, 2);
    assert.notEqual(first, second);
    assert.throws(() => buildEventBody('load.created', 8, 'run-a', 1), /too small/);
    assert.throws(() => buildEventBody('load.created', 262145, 'run-a', 1), /262144/);
});

test('producer uses the configured remote ingestion URL when provided', () => {
    assert.equal(producerBaseUrl({ INGESTION_API_URL: 'https://ingestion.example.test' }),
        'https://ingestion.example.test');
    assert.equal(producerBaseUrl({ INGESTION_API_HOST_PORT: '18082' }),
        'http://127.0.0.1:18082');
});

test('producer counts only 202 as accepted and bounds in-flight requests', async () => {
    const keys = [];
    const server = createServer((request, response) => {
        keys.push(request.headers['idempotency-key']);
        request.resume();
        request.on('end', () => {
            response.writeHead(keys.length % 2 ? 202 : 429);
            response.end('{}');
        });
    });
    const baseUrl = await listen(server);
    const lines = [];
    const result = await runProducer({ baseUrl, tenants: [{ api_key: 'test', event_type: 'load.created' }],
        rate: 20, durationSeconds: 0.3, payloadBytes: 256, maxInFlight: 2,
        timeoutMs: 1000, runId: 'run-a', instanceId: 'one', record: line => lines.push(line) });
    assert.equal(result.scheduled, 6);
    assert.equal(result.skipped, 0);
    assert.equal(result.completed + result.skipped, 6);
    assert.equal(result.accepted + result.rejected + result.errors, result.completed);
    assert.equal(new Set(keys).size, keys.length);
    assert.equal(lines.length, result.completed);
    assert.ok(lines.every(line => !JSON.stringify(line).includes('test')));
    assert.ok(result.accepted > 0);
    assert.ok(result.rejected > 0);
});

test('producer waits for a shared UTC start and records timing offsets', async () => {
    const server = createServer((request, response) => {
        request.resume();
        request.on('end', () => response.writeHead(202).end());
    });
    const baseUrl = await listen(server);
    const planned = new Date(Date.now() + 120).toISOString();
    const records = [];
    const result = await runProducer({ baseUrl,
        tenants: [{ api_key: 'test', event_type: 'load.created' }], rate: 20,
        durationSeconds: 0.1, payloadBytes: 256, maxInFlight: 2, timeoutMs: 1000,
        runId: 'shared-run', instanceId: 'shard-a', startAtUtc: planned,
        record: line => records.push(line) });
    assert.equal(result.planned_start_at_utc, planned);
    assert.ok(Date.parse(result.started_at_utc) >= Date.parse(planned));
    assert.equal(result.scheduled, 2);
    assert.ok(records.every(line => line.sent_offset_ms >= 0
        && line.completed_offset_ms >= line.sent_offset_ms));
    assert.throws(() => plannedStartDelay('not-a-time', Date.now()), /start-at-utc/);
    assert.throws(() => plannedStartDelay(new Date(Date.now() - 2000).toISOString(),
        Date.now()), /past/);
    assert.throws(() => plannedStartDelay(new Date(Date.now() + 360000).toISOString(),
        Date.now()), /five minutes/);
});

test('producer exits when result recording fails', async () => {
    const server = createServer((request, response) => {
        request.resume();
        request.on('end', () => response.writeHead(202).end());
    });
    const baseUrl = await listen(server);
    await assert.rejects(runProducer({ baseUrl,
        tenants: [{ api_key: 'private-key', event_type: 'load.created' }], rate: 20,
        durationSeconds: 0.1, payloadBytes: 256, maxInFlight: 2, timeoutMs: 1000,
        runId: 'run-c', instanceId: 'one', record: () => { throw new Error('disk full'); } }),
    /disk full/);
});

test('producer records a bounded transport error code without credentials', async () => {
    const server = createServer(request => request.socket.destroy());
    const baseUrl = await listen(server);
    const records = [];
    const result = await runProducer({ baseUrl,
        tenants: [{ api_key: 'private-key', event_type: 'load.created' }], rate: 10,
        durationSeconds: 0.1, payloadBytes: 256, maxInFlight: 1, timeoutMs: 1000,
        runId: 'run-d', instanceId: 'one', record: line => records.push(line) });
    assert.equal(result.errors, 1);
    assert.equal(records[0].error_code, 'ECONNRESET');
    assert.ok(!JSON.stringify(records).includes('private-key'));
});

test('producer records skipped arrivals instead of building an unbounded queue', async () => {
    const server = createServer((request, response) => {
        request.resume();
        request.on('end', () => setTimeout(() => response.writeHead(202).end(), 80));
    });
    const baseUrl = await listen(server);
    const result = await runProducer({ baseUrl, tenants: [{ api_key: 'test', event_type: 'load.created' }],
        rate: 100, durationSeconds: 0.2, payloadBytes: 256, maxInFlight: 1,
        timeoutMs: 1000, runId: 'run-b', instanceId: 'one' });
    assert.equal(result.scheduled, 20);
    assert.ok(result.skipped > 10);
    assert.equal(result.completed + result.skipped, result.scheduled);
    assert.equal(result.max_in_flight_observed, 1);
});

test('receiver applies delay and an exact deterministic failure percentage', async () => {
    const baseUrl = await listen(createReceiver({ latencyMs: 15, failurePercent: 25 }));
    const start = performance.now();
    const statuses = [];
    for (let index = 0; index < 8; index += 1) {
        statuses.push((await fetch(`${baseUrl}/load/test`, { method: 'POST', body: '{}' })).status);
    }
    assert.deepEqual(statuses.filter(status => status === 503).length, 2);
    assert.ok(performance.now() - start >= 100);
    const stats = await (await fetch(`${baseUrl}/__stats`)).json();
    assert.equal(stats.requests, 8);
    assert.equal(stats.failures, 2);
});

test('setup creates requested fan-out for each tenant and keeps keys in the manifest', async () => {
    const calls = [];
    const api = async (path, body, headers) => {
        calls.push({ path, body, headers });
        if (path === '/v1/organizations') {
            return { data: { organization_id: `org-${calls.length}`, api_key: `rf_key_${calls.length}_secret` } };
        }
        if (path.includes('/rotate')) {
            return { data: { api_key: `rotated-${calls.length}` } };
        }
        return { data: { endpoint_id: `endpoint-${calls.length}` } };
    };
    const manifest = await provision({ tenants: 2, fanout: 3, receiverUrl: 'http://load-receiver:8080',
        runId: 'run-a', provisioningCredential: 'private', api });
    assert.equal(manifest.tenants.length, 2);
    assert.equal(calls.filter(call => call.path === '/v1/endpoints').length, 6);
    assert.ok(calls.filter(call => call.path === '/v1/endpoints')
        .every(call => call.body.event_types[0] === manifest.event_type));
    assert.equal(manifest.tenants[0].api_key, 'rotated-2');
    assert.equal(manifest.fanout, 3);
});

test('setup keeps endpoint population separate from matching fan-out', async () => {
    const calls = [];
    const api = async (path, body) => {
        calls.push({ path, body });
        if (path === '/v1/organizations') {
            return { data: { organization_id: 'org-1', api_key: 'rf_key_1_secret' } };
        }
        if (path.includes('/rotate')) {
            return { data: { api_key: 'rotated-key' } };
        }
        return { data: { endpoint_id: `endpoint-${calls.length}` } };
    };
    const manifest = await provision({ tenants: 1, fanout: 3, endpointsPerTenant: 26,
        receiverUrl: 'http://load-receiver:8080', runId: 'run-b',
        provisioningCredential: 'private', api });
    const endpoints = calls.filter(call => call.path === '/v1/endpoints');
    assert.equal(endpoints.length, 26);
    assert.equal(endpoints.filter(call => call.body.event_types.includes(manifest.event_type)).length, 3);
    assert.equal(calls[0].body.quota_overrides.max_active_endpoints, 26);
    assert.equal(manifest.endpoints_per_tenant, 26);
    await assert.rejects(provision({ tenants: 1, fanout: 4, endpointsPerTenant: 3,
        receiverUrl: 'http://load-receiver:8080', runId: 'run-b',
        provisioningCredential: 'private', api }), /endpointsPerTenant/);
});

test('setup provisions through a configured remote control-plane URL', async () => {
    const paths = [];
    const server = createServer((request, response) => {
        paths.push(request.url);
        request.resume();
        request.on('end', () => {
            const data = request.url === '/v1/organizations'
                ? { organization_id: 'org-1', api_key: 'rf_key_test_secret' }
                : request.url.includes('/rotate')
                    ? { api_key: 'rotated-key' } : { endpoint_id: 'endpoint-1' };
            response.writeHead(200, { 'Content-Type': 'application/json' });
            response.end(JSON.stringify({ data }));
        });
    });
    const remoteUrl = await listen(server);
    const directory = await mkdtemp(join(tmpdir(), 'relayforge-remote-control-'));
    try {
        const envFile = join(directory, '.env');
        const output = join(directory, 'credentials.json');
        await writeFile(envFile, `CONTROL_API_URL=${remoteUrl}\nCONTROL_API_HOST_PORT=1\n`
            + 'WEBHOOK_SIMULATOR_URL=http://receiver.test\nTRUSTED_PROVISIONING_CREDENTIAL=test\n');
        await execFileAsync(process.execPath, ['load-tests/setup.mjs', '--tenants', '1',
            '--fanout', '1', '--run-id', 'remote-test', '--output', output],
        { env: { ...process.env, RELAYFORGE_ENV_FILE: envFile }, timeout: 10000 });
        assert.deepEqual(paths, ['/v1/organizations', '/v1/api-keys/test/rotate', '/v1/endpoints']);
        assert.equal(JSON.parse(await readFile(output, 'utf8')).tenants[0].api_key, 'rotated-key');
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
});
