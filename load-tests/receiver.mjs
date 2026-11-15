import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';
import { integer, localSettings, options } from './common.mjs';

export function createReceiver({ latencyMs, failurePercent }) {
    integer(latencyMs, 'latencyMs', 0, 60000);
    integer(failurePercent, 'failurePercent', 0, 100);
    const stats = { requests: 0, successes: 0, failures: 0, bytes: 0 };
    return createServer(async (request, response) => {
        if (request.method === 'GET' && request.url === '/health') {
            response.writeHead(200).end('ok');
            return;
        }
        if (request.method === 'GET' && request.url === '/__stats') {
            response.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(stats));
            return;
        }
        if (request.method !== 'POST' || !request.url?.startsWith('/load/')) {
            response.writeHead(404).end();
            return;
        }
        let bytes = 0;
        for await (const chunk of request) {
            bytes += chunk.length;
            if (bytes > 524288) {
                response.writeHead(413).end();
                return;
            }
        }
        stats.requests += 1;
        stats.bytes += bytes;
        const failure = Math.floor(stats.requests * failurePercent / 100)
            > Math.floor((stats.requests - 1) * failurePercent / 100);
        if (latencyMs) {
            await new Promise(resolve => setTimeout(resolve, latencyMs));
        }
        stats[failure ? 'failures' : 'successes'] += 1;
        response.writeHead(failure ? 503 : 204).end();
    });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    try {
        const flags = options(process.argv.slice(2));
        const settings = await localSettings().catch(() => ({}));
        const latencyMs = integer(flags['latency-ms'] ?? process.env.LOAD_RECEIVER_LATENCY_MS
            ?? settings.LOAD_RECEIVER_LATENCY_MS ?? 0,
            'latency-ms', 0, 60000);
        const failurePercent = integer(flags['failure-percent']
            ?? process.env.LOAD_RECEIVER_FAILURE_PERCENT
            ?? settings.LOAD_RECEIVER_FAILURE_PERCENT ?? 0, 'failure-percent', 0, 100);
        const port = integer(flags.port ?? process.env.PORT ?? 8080, 'port', 1, 65535);
        const server = createReceiver({ latencyMs, failurePercent });
        server.listen(port, flags.host ?? '0.0.0.0', () => {
            console.log(JSON.stringify({ listening: port, latency_ms: latencyMs,
                failure_percent: failurePercent }));
        });
    } catch (error) {
        console.error(error.message);
        process.exitCode = 1;
    }
}
