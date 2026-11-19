import { randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir, readFile } from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import { dirname } from 'node:path';
import { performance } from 'node:perf_hooks';
import { finished } from 'node:stream/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import { identifier, integer, localSettings, number, options } from './common.mjs';

export function buildEventBody(eventType, bytes, runId, sequence) {
    integer(bytes, 'payloadBytes', 1, 262144);
    const base = JSON.stringify({ event_type: eventType, schema_version: 1,
        data: { run_id: runId, sequence, padding: '' } });
    const padding = bytes - Buffer.byteLength(base);
    if (padding < 0) throw new Error(`payloadBytes too small; need at least ${Buffer.byteLength(base)}`);
    return JSON.stringify({ event_type: eventType, schema_version: 1,
        data: { run_id: runId, sequence, padding: 'x'.repeat(padding) } });
}

function send(url, body, key, agent, timeoutMs) {
    const transport = url.protocol === 'https:' ? https : http;
    return new Promise((resolve, reject) => {
        const request = transport.request(url, { method: 'POST', agent, headers: {
            Authorization: `Bearer ${key}`, 'Idempotency-Key': body.idempotencyKey,
            'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body.json),
        } }, response => {
            response.resume();
            response.on('end', () => resolve(response.statusCode));
            response.on('error', reject);
        });
        request.setTimeout(timeoutMs, () => request.destroy(new Error('timeout')));
        request.on('error', reject);
        request.end(body.json);
    });
}

function percentile(sorted, fraction) {
    if (!sorted.length) return null;
    return Math.round(sorted[Math.ceil(sorted.length * fraction) - 1] * 1000) / 1000;
}

export function plannedStartDelay(startAtUtc, nowMs) {
    if (startAtUtc == null) return 0;
    if (typeof startAtUtc !== 'string'
        || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(startAtUtc)
        || !Number.isFinite(Date.parse(startAtUtc))) {
        throw new Error('Invalid start-at-utc');
    }
    const delay = Date.parse(startAtUtc) - nowMs;
    if (delay < -1000) throw new Error('start-at-utc is in the past');
    if (delay > 300000) throw new Error('start-at-utc exceeds five minutes');
    return Math.max(0, Math.ceil(delay));
}

export function producerBaseUrl(settings) {
    return settings.INGESTION_API_URL || `http://127.0.0.1:${settings.INGESTION_API_HOST_PORT}`;
}

export async function runProducer(config) {
    const { baseUrl, tenants, rate, durationSeconds, payloadBytes, maxInFlight,
        timeoutMs, runId, instanceId, startAtUtc = null, record = () => {} } = config;
    number(rate, 'rate', 0.001, 1000000);
    number(durationSeconds, 'durationSeconds', 0.001, 86400);
    integer(payloadBytes, 'payloadBytes', 1, 262144);
    integer(maxInFlight, 'maxInFlight', 1, 1000000);
    integer(timeoutMs, 'timeoutMs', 1, 600000);
    identifier(runId, 'runId');
    identifier(instanceId, 'instanceId');
    if (!Array.isArray(tenants) || !tenants.length || tenants.some(tenant =>
        !tenant.api_key || !tenant.event_type)) throw new Error('Manifest needs tenant API keys and event types');
    const startDelay = plannedStartDelay(startAtUtc, Date.now());
    const target = new URL('/v1/events', baseUrl);
    if (!['http:', 'https:'].includes(target.protocol)) throw new Error('baseUrl must use HTTP or HTTPS');
    const agent = target.protocol === 'https:'
        ? new https.Agent({ keepAlive: true, maxSockets: maxInFlight })
        : new http.Agent({ keepAlive: true, maxSockets: maxInFlight });
    const total = Math.floor(rate * durationSeconds);
    const result = { run_id: runId, instance_id: instanceId,
        requested_rate: rate, requested_duration_seconds: durationSeconds, scheduled: total,
        payload_bytes: payloadBytes, max_in_flight: maxInFlight, timeout_ms: timeoutMs,
        planned_start_at_utc: startAtUtc,
        completed: 0, accepted: 0, rejected: 0, errors: 0, skipped: 0,
        max_in_flight_observed: 0 };
    const latencies = [];
    const pending = new Set();
    let recordError;
    let next = 0;
    await sleep(startDelay);
    result.started_at_utc = new Date().toISOString();
    const start = performance.now();
    try {
        while (performance.now() - start < durationSeconds * 1000) {
            if (recordError) break;
            const due = Math.min(total, Math.floor((performance.now() - start) * rate / 1000) + 1);
            while (next < due) {
                const sequence = next++;
                if (pending.size >= maxInFlight) {
                    result.skipped += 1;
                    continue;
                }
                const tenantIndex = sequence % tenants.length;
                const tenant = tenants[tenantIndex];
                const json = buildEventBody(tenant.event_type, payloadBytes, runId, sequence);
                const idempotencyKey = `j3-${runId}-${instanceId}-${sequence}`;
                const sentAt = performance.now();
                const task = send(target, { json, idempotencyKey }, tenant.api_key, agent, timeoutMs)
                    .then(status => ({ status, outcome: status === 202 ? 'accepted' : 'rejected' }))
                    .catch(error => ({ status: null, outcome: 'error',
                        errorCode: error.message === 'timeout' ? 'TIMEOUT'
                            : /^[A-Z0-9_]{1,32}$/.test(error.code ?? '') ? error.code : 'OTHER' }))
                    .then(({ status, outcome, errorCode }) => {
                        const completedAt = performance.now();
                        const latencyMs = completedAt - sentAt;
                        result.completed += 1;
                        result[outcome === 'accepted' ? 'accepted'
                            : outcome === 'rejected' ? 'rejected' : 'errors'] += 1;
                        latencies.push(latencyMs);
                        try {
                            record({ sequence, tenant_index: tenantIndex, status, outcome,
                                ...(errorCode ? { error_code: errorCode } : {}),
                                sent_offset_ms: Math.round((sentAt - start) * 1000) / 1000,
                                completed_offset_ms: Math.round((completedAt - start) * 1000) / 1000,
                                latency_ms: Math.round(latencyMs * 1000) / 1000 });
                        } catch (error) {
                            recordError ??= error;
                        }
                    });
                pending.add(task);
                result.max_in_flight_observed = Math.max(result.max_in_flight_observed, pending.size);
                task.finally(() => pending.delete(task));
            }
            await sleep(Math.min(10, Math.max(1, durationSeconds * 1000 - (performance.now() - start))));
        }
        result.skipped += total - next;
        await Promise.all(pending);
        if (recordError) throw recordError;
    } finally {
        agent.destroy();
    }
    latencies.sort((left, right) => left - right);
    result.elapsed_seconds = Math.round((performance.now() - start) / 1000 * 1000) / 1000;
    result.finished_at_utc = new Date().toISOString();
    result.accepted_per_second = Math.round(result.accepted / result.elapsed_seconds * 1000) / 1000;
    result.latency_ms = { p50: percentile(latencies, 0.5),
        p95: percentile(latencies, 0.95), p99: percentile(latencies, 0.99) };
    return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    try {
        const flags = options(process.argv.slice(2));
        const settings = await localSettings();
        const manifest = JSON.parse(await readFile(flags.manifest ?? 'load-tests/local.credentials.json', 'utf8'));
        if (manifest.version !== 1) throw new Error('Unsupported manifest version');
        const runId = identifier(flags['run-id'] ?? randomUUID().slice(0, 12), 'run-id');
        const instanceId = identifier(flags['instance-id'] ?? 'one', 'instance-id');
        const output = flags.output ?? `load-tests/results/${runId}-${instanceId}.ndjson`;
        await mkdir(dirname(output), { recursive: true });
        const stream = createWriteStream(output, { flags: 'wx', mode: 0o600 });
        const writing = finished(stream).then(() => null, error => error);
        let streamError;
        stream.on('error', error => { streamError = error; });
        try {
            const summary = await runProducer({
                baseUrl: producerBaseUrl(settings),
                tenants: manifest.tenants, rate: number(flags.rate, 'rate', 0.001, 1000000),
                durationSeconds: number(flags['duration-seconds'], 'duration-seconds', 0.001, 86400),
                payloadBytes: integer(flags['payload-bytes'] ?? 1024, 'payload-bytes', 1, 262144),
                maxInFlight: integer(flags['max-in-flight'] ?? 1000, 'max-in-flight', 1, 1000000),
                timeoutMs: integer(flags['timeout-ms'] ?? 10000, 'timeout-ms', 1, 600000),
                runId, instanceId, startAtUtc: flags['start-at-utc'] ?? null,
                record: line => {
                    if (streamError) throw streamError;
                    stream.write(JSON.stringify(line) + '\n');
                    if (stream.writableLength > 16777216) {
                        throw new Error('Result writer exceeded 16 MiB backlog');
                    }
                },
            });
            stream.end();
            const writeError = await writing;
            if (writeError) throw writeError;
            console.log(JSON.stringify({ ...summary, output }));
        } finally {
            stream.destroy();
        }
    } catch (error) {
        console.error(error.message);
        process.exitCode = 1;
    }
}
