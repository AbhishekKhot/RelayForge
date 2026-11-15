import { readFile } from 'node:fs/promises';

export function options(argumentsList) {
    const parsed = {};
    for (let index = 0; index < argumentsList.length; index += 2) {
        const name = argumentsList[index];
        if (!name?.startsWith('--') || !argumentsList[index + 1] || parsed[name.slice(2)] !== undefined) {
            throw new Error(`Invalid or duplicate option: ${name}`);
        }
        parsed[name.slice(2)] = argumentsList[index + 1];
    }
    return parsed;
}

export function number(value, label, minimum, maximum) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < minimum || parsed > maximum) {
        throw new Error(`${label} must be between ${minimum} and ${maximum}`);
    }
    return parsed;
}

export function integer(value, label, minimum, maximum) {
    const parsed = number(value, label, minimum, maximum);
    if (!Number.isInteger(parsed)) {
        throw new Error(`${label} must be an integer`);
    }
    return parsed;
}

export function identifier(value, label) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$/.test(value ?? '')) {
        throw new Error(`${label} must contain 1-48 letters, numbers, underscores or hyphens`);
    }
    return value;
}

export async function localSettings() {
    const lines = (await readFile(process.env.RELAYFORGE_ENV_FILE ?? '.env', 'utf8')).split(/\r?\n/);
    return Object.fromEntries(lines.filter(line => line && !line.startsWith('#') && line.includes('='))
        .map(line => [line.slice(0, line.indexOf('=')), line.slice(line.indexOf('=') + 1)]));
}
