const test = require('node:test');
const assert = require('node:assert/strict');
const { request } = require('../../dist/nodes/Clickhouse/transport/clickhouseClient');
const { startClickHouseContainer, dockerAvailable } = require('./helpers');

const containerName = `clickhouse-query-${Date.now()}`;

test(
	'query integration',
	{ skip: !dockerAvailable(), timeout: 60000 },
	async (t) => {
		const { credentials, cleanup } = await startClickHouseContainer({ containerName });
		t.after(cleanup);

		const table = `n8n_query_${Date.now()}`;
		await request({
			credentials,
			sql: `CREATE TABLE ${table} (id UInt32, name String) ENGINE = Memory`,
			queryInUrl: true,
			compress: false,
		});
		await request({
			credentials,
			sql: `INSERT INTO ${table} FORMAT JSONEachRow`,
			queryInUrl: true,
			body: '{"id":1,"name":"alpha"}\n{"id":2,"name":"beta"}',
			compress: false,
		});
		await request({
			credentials,
			sql: `INSERT INTO ${table} FORMAT JSONEachRow`,
			queryInUrl: true,
			body: '{"id":3,"name":"gamma"}',
			compress: false,
		});

		const response = await request({
			credentials,
			sql: `SELECT id, name FROM ${table} ORDER BY id FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});
		const payload = JSON.parse(response.body);
		assert.equal(payload.data.length, 3);
		assert.deepEqual(payload.data[0], { id: 1, name: 'alpha' });

		const page1 = await request({
			credentials,
			sql: `SELECT * FROM (SELECT id, name FROM ${table} ORDER BY id) LIMIT 2 OFFSET 0 FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});
		const page2 = await request({
			credentials,
			sql: `SELECT * FROM (SELECT id, name FROM ${table} ORDER BY id) LIMIT 2 OFFSET 2 FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});
		const page1Payload = JSON.parse(page1.body);
		const page2Payload = JSON.parse(page2.body);
		assert.equal(page1Payload.data.length, 2);
		assert.equal(page2Payload.data.length, 1);
	},
);
