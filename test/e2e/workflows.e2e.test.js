const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');

function loadEnvFile(filePath) {
	if (!fs.existsSync(filePath)) {
		return {};
	}
	const content = fs.readFileSync(filePath, 'utf8');
	const env = {};
	for (const line of content.split('\n')) {
		const trimmed = line.trim();
		if (!trimmed || trimmed.startsWith('#')) continue;
		const [key, ...rest] = trimmed.split('=');
		if (!key) continue;
		env[key] = rest.join('=').trim();
	}
	return env;
}

const envFile = path.join(process.cwd(), '.env.e2e');
const fileEnv = loadEnvFile(envFile);
const baseUrl = process.env.N8N_E2E_URL || fileEnv.N8N_E2E_URL || 'http://localhost:5678';
const apiKey = process.env.N8N_E2E_API_KEY || fileEnv.N8N_E2E_API_KEY;
const shouldSkip = !apiKey;

const headers = {
	'Content-Type': 'application/json',
	'X-N8N-API-KEY': apiKey,
};

async function apiFetch(endpoint, options = {}) {
	const res = await fetch(`${baseUrl}${endpoint}`, {
		...options,
		headers: { ...headers, ...(options.headers || {}) },
	});
	const text = await res.text();
	let data = null;
	try {
		data = text ? JSON.parse(text) : null;
	} catch {
		// ignore
	}
	return { res, data, text };
}

async function createWorkflow(definition) {
	const payload = {
		name: definition.name,
		nodes: definition.nodes,
		connections: definition.connections,
		settings: definition.settings || {},
	};
	const { res, data, text } = await apiFetch('/api/v1/workflows', {
		method: 'POST',
		body: JSON.stringify(payload),
	});
	assert.ok(res.ok, `Failed to create workflow: ${res.status} ${text}`);
	return data.id;
}

async function deleteWorkflow(id) {
	await apiFetch(`/api/v1/workflows/${id}`, { method: 'DELETE' });
}

async function runWorkflow(id) {
	const endpoints = [
		`/api/v1/workflows/${id}/run`,
		`/api/v1/workflows/${id}/execute`,
		`/rest/workflows/${id}/run`,
	];

	for (const endpoint of endpoints) {
		const result = await apiFetch(endpoint, { method: 'POST', body: '{}' });
		if (result.res.ok) return result;
	}
	throw new Error('No workflow run endpoint succeeded');
}

test(
	'e2e: import and run example workflows',
	{ skip: shouldSkip },
	async (t) => {
		const examplesDir = path.join(process.cwd(), 'examples');
	const entries = await fsp.readdir(examplesDir);
	const exampleFiles = entries.filter((file) => file.endsWith('.json'));

		for (const file of exampleFiles) {
			const content = await fsp.readFile(path.join(examplesDir, file), 'utf8');
			const workflow = JSON.parse(content);
			const id = await createWorkflow(workflow);
			t.after(() => deleteWorkflow(id));
			const runResult = await runWorkflow(id);
			assert.ok(runResult.text.length > 0);
		}
	},
);
