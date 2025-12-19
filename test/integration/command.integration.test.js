const test = require('node:test');
const assert = require('node:assert/strict');
const { request } = require('../../dist/nodes/Clickhouse/transport/clickhouseClient');
const { startClickHouseContainer, dockerAvailable } = require('./helpers');

const containerName = `clickhouse-command-${Date.now()}`;

test(
	'command integration',
	{ skip: !dockerAvailable(), timeout: 60000 },
	async (t) => {
		const { credentials, cleanup } = await startClickHouseContainer({ containerName });
		t.after(cleanup);

		const table = `n8n_command_${Date.now()}`;
		await request({
			credentials,
			sql: `CREATE TABLE ${table} (id UInt8) ENGINE = Memory`,
			queryInUrl: true,
			compress: false,
		});
		const truncate = await request({
			credentials,
			sql: `TRUNCATE TABLE ${table}`,
			queryInUrl: true,
			compress: false,
			waitEndOfQuery: true,
		});
		assert.equal(truncate.status, 200);

		const drop = await request({
			credentials,
			sql: `DROP TABLE ${table}`,
			queryInUrl: true,
			compress: false,
			waitEndOfQuery: true,
		});
		assert.equal(drop.status, 200);
	},
);
