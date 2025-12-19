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
	let data = match.data || {};
	if (typeof data === 'string') {
		try {
			data = JSON.parse(data);
		} catch {
			data = {};
		}
	}
	return { id: match.id, name: match.name, data };
}

function collectTablesFromWorkflow(workflow, credentialData) {
	const tables = new Set();
	const fallbackDb = (credentialData?.defaultDatabase || 'default').trim() || 'default';
	for (const node of workflow.nodes || []) {
		if (!node?.parameters) continue;
		if (node.parameters.resource === 'insert') {
			const table = String(node.parameters.table || '').trim();
			if (!table) continue;
			const override = String(node.parameters.databaseOverride || '').trim();
			const database = override || fallbackDb;
			const name = table.includes('.') ? table : `${database}.${table}`;
			tables.add(name);
			continue;
		}
		if (node.parameters.resource === 'command') {
			const sql = String(node.parameters.command || '');
			const match = sql.match(/create\s+table\s+(if\s+not\s+exists\s+)?([`"\w.]+)/i);
			if (!match) continue;
			let tableName = match[2].replace(/[`\"]/g, '');
			if (!tableName) continue;
			const override = String(node.parameters.databaseOverride || '').trim();
			const database = override || fallbackDb;
			if (!tableName.includes('.')) {
				tableName = `${database}.${tableName}`;
			}
			tables.add(tableName);
		}
	}
	return Array.from(tables);
}

async function cleanupTables(tables, credentialData) {
	if (!tables.length) return;
	const protocol = credentialData?.protocol || 'http';
	let host = credentialData?.host || 'localhost';
	const port = credentialData?.port || (protocol === 'https' ? 8443 : 8123);
	const username = credentialData?.username || 'default';
	const password = credentialData?.password || '';
	if (host === 'clickhouse' || host === 'clickhouse_tls') {
		host = 'localhost';
	}
	const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
	for (const table of tables) {
		const sql = `DROP TABLE IF EXISTS ${table}`;
		const url = `${protocol}://${host}:${port}/?query=${encodeURIComponent(sql)}`;
		if (protocol === 'https' && credentialData?.tlsIgnoreSsl) {
			const script = `node -e "const https=require('https');const url='${url}';const auth='${authHeader}';const req=https.request(url,{method:'POST',headers:{Authorization:auth},rejectUnauthorized:false},res=>{res.on('data',()=>{});res.on('end',()=>{process.exit(res.statusCode>=400?1:0);});});req.on('error',()=>process.exit(1));req.end();"`;
			dockerExec(script, { allowFailure: true });
			continue;
		}
		try {
			const res = await fetch(url, { method: 'POST', headers: { Authorization: authHeader } });
			if (!res.ok) {
				await res.text();
			}
		} catch {
			// ignore cleanup failures
		}
	}
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
		const tablesToCleanup = new Set();

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
			const tables = collectTablesFromWorkflow(workflow, credential.data);
			for (const table of tables) tablesToCleanup.add(table);
		}

		await cleanupTables(Array.from(tablesToCleanup), credential.data);
	},
);
