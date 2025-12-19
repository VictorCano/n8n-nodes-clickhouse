const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { spawnSync, execSync } = require('node:child_process');

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
const containerName = process.env.N8N_E2E_CONTAINER || fileEnv.N8N_E2E_CONTAINER || 'n8n-local';
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

function dockerExec(command, { allowFailure = false } = {}) {
	const result = spawnSync(
		'docker',
		['exec', containerName, 'sh', '-lc', command],
		{ encoding: 'utf8' },
	);
	if (!allowFailure && result.status !== 0) {
		const error = new Error(`docker exec failed: ${result.stderr || result.stdout}`);
		error.code = result.status;
		throw error;
	}
	return result;
}

function containerRunning() {
	try {
		const id = execSync(
			`docker ps --filter "name=${containerName}" --format "{{.ID}}"`,
			{ encoding: 'utf8' },
		).trim();
		return Boolean(id);
	} catch {
		return false;
	}
}

async function getClickHouseCredential() {
	const tempFile = `/tmp/creds-${Date.now()}.json`;
	dockerExec(`n8n export:credentials --all --decrypted --output=${tempFile}`);
	const rawResult = dockerExec(`cat ${tempFile}`);
	dockerExec(`rm -f ${tempFile}`);
	const list = JSON.parse(rawResult.stdout.trim());
	const candidates = Array.isArray(list) ? list : list.data || [];
	const match = candidates.find(
		(item) => item.name === 'ClickHouse Local' || item.type === 'ClickHouseApi',
	);
	if (!match) {
		throw new Error('No ClickHouseApi credential found in n8n');
	}
	return { id: match.id, name: match.name };
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

async function getWorkflow(id) {
	const { res, data, text } = await apiFetch(`/api/v1/workflows/${id}`);
	assert.ok(res.ok, `Failed to fetch workflow: ${res.status} ${text}`);
	return data;
}

async function deleteWorkflow(id) {
	await apiFetch(`/api/v1/workflows/${id}`, { method: 'DELETE' });
}

function extractJson(output) {
	const start = output.indexOf('{');
	if (start === -1) {
		throw new Error('No JSON payload found in output');
	}
	const jsonText = output.slice(start).trim();
	return JSON.parse(jsonText);
}

function runWorkflow(id) {
	const result = dockerExec(`n8n execute --id=${id} --rawOutput`, { allowFailure: true });
	const combined = `${result.stdout || ''}\n${result.stderr || ''}`.trim();
	if (result.status !== 0) {
		throw new Error(`Workflow execution failed: ${combined}`);
	}
	if (!combined) {
		throw new Error('Workflow execution returned empty output');
	}
	return extractJson(combined);
}

test(
	'e2e: import and run example workflows',
	{ skip: shouldSkip || !containerRunning() },
	async (t) => {
		const credential = await getClickHouseCredential();
		const examplesDir = path.join(process.cwd(), 'examples');
		const entries = await fsp.readdir(examplesDir);
		const exampleFiles = entries.filter((file) => file.endsWith('.json'));

		for (const file of exampleFiles) {
			const content = await fsp.readFile(path.join(examplesDir, file), 'utf8');
			const workflow = JSON.parse(content);
			for (const node of workflow.nodes || []) {
				if (node.credentials && node.credentials.ClickHouseApi) {
					node.credentials.ClickHouseApi = credential;
				}
			}
			const id = await createWorkflow(workflow);
			t.after(() => deleteWorkflow(id));
			const stored = await getWorkflow(id);
			const storedNodes = stored.nodes || [];
			const hasClickHouse = storedNodes.some((node) =>
				String(node.type || '').includes('n8n-nodes-clickhouse') ||
				String(node.type || '').includes('CUSTOM.clickhouse'),
			);
			assert.ok(
				hasClickHouse,
				`ClickHouse nodes missing after import (${file}). Make sure dist is built and mounted into n8n-local.`,
			);
			const runResult = runWorkflow(id);
			const resultData =
				runResult?.data?.resultData ||
				runResult?.resultData ||
				runResult?.executedData?.resultData;
			assert.ok(resultData, 'Missing result data');
			assert.ok(!resultData.error, 'Workflow execution reported an error');
			assert.ok(resultData.runData, 'Missing run data');
		}
	},
);
