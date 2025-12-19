const test = require('node:test');
const assert = require('node:assert/strict');
const { request } = require('../../dist/nodes/Clickhouse/transport/clickhouseClient');
const { startClickHouseContainer, dockerAvailable } = require('./helpers');

const containerName = `clickhouse-auth-${Date.now()}`;

test(
	'auth failure returns error',
	{ skip: !dockerAvailable(), timeout: 60000 },
	async (t) => {
		const { credentials, cleanup } = await startClickHouseContainer({ containerName });
		t.after(cleanup);

		const badCredentials = { ...credentials, password: 'wrong' };
		await assert.rejects(
			() =>
				request({
					credentials: badCredentials,
					sql: 'SELECT 1',
					compress: false,
				}),
			/ClickHouse request failed with status (401|403)/,
		);
	},
);
