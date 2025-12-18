const test = require('node:test');
const assert = require('node:assert/strict');
const { execSync } = require('node:child_process');
const { request } = require('../dist/src/transport/clickhouseClient');
const { buildInsertQuery, buildNdjson } = require('../dist/src/operations/insertUtils');

const image = process.env.CLICKHOUSE_TEST_IMAGE || 'clickhouse/clickhouse-server:latest';
const containerName = `clickhouse-test-${Date.now()}`;

function dockerAvailable() {
	try {
		execSync('docker info', { stdio: 'ignore' });
		return true;
	} catch {
		return false;
	}
}

async function waitForPing(url, authHeader) {
	const timeoutMs = 30_000;
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		try {
			const res = await fetch(url, {
				headers: authHeader ? { Authorization: authHeader } : undefined,
			});
			if (res.ok) {
				return;
			}
		} catch {
			// ignore until timeout
		}
		await new Promise((resolve) => setTimeout(resolve, 500));
	}
	throw new Error('ClickHouse did not start in time');
}

test(
	'insert integration',
	{ skip: !dockerAvailable() },
	async (t) => {
		let hostPort = '';
		try {
			execSync(`docker rm -f ${containerName}`, { stdio: 'ignore' });
		} catch {
			// ignore
		}
		const username = 'default';
		const password = 'pass';
		const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
		try {
			execSync(
				`docker run -d --name ${containerName} -p 0:8123 ` +
					`-e CLICKHOUSE_USER=${username} -e CLICKHOUSE_PASSWORD=${password} ` +
					`-e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 ${image}`,
				{ stdio: 'ignore' },
			);
			const portLine = execSync(`docker port ${containerName} 8123/tcp`).toString().trim();
			hostPort = portLine.split(':').pop();
			await waitForPing(`http://127.0.0.1:${hostPort}/ping`, authHeader);
		} catch (error) {
			throw new Error(`Failed to start ClickHouse container: ${error}`);
		}

		t.after(() => {
			try {
				execSync(`docker rm -f ${containerName}`, { stdio: 'ignore' });
			} catch {
				// ignore
			}
		});

		const credentials = {
			protocol: 'http',
			host: '127.0.0.1',
			port: Number(hostPort),
			username: 'default',
			password: 'pass',
			defaultDatabase: 'default',
			tlsIgnoreSsl: false,
		};

		const table = `n8n_insert_${Date.now()}`;
		await request({
			credentials,
			sql: `CREATE TABLE ${table} (id UInt32, name String) ENGINE = Memory`,
			queryInUrl: true,
			compress: false,
		});

		const rows = [
			{ id: 1, name: 'alpha' },
			{ id: 2, name: 'beta' },
			{ id: 3, name: 'gamma' },
		];
		const insertQuery = buildInsertQuery({ table });
		await request({
			credentials,
			sql: insertQuery,
			queryInUrl: true,
			body: buildNdjson(rows),
			compress: false,
			waitEndOfQuery: true,
		});

		const selectResponse = await request({
			credentials,
			sql: `SELECT id, name FROM ${table} ORDER BY id FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});

		const payload = JSON.parse(selectResponse.body);
		assert.equal(payload.data.length, rows.length);
		assert.deepEqual(payload.data, rows);
	},
);
