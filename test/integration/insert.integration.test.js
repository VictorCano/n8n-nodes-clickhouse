const test = require('node:test');
const assert = require('node:assert/strict');
const { request } = require('../../dist/nodes/Clickhouse/transport/clickhouseClient');
const { buildInsertQuery, buildNdjson } = require('../../dist/nodes/Clickhouse/operations/insertUtils');
const { dockerAvailable, startClickHouseContainer } = require('./helpers');

const containerName = `clickhouse-test-${Date.now()}`;

test(
	'insert integration',
	{ skip: !dockerAvailable(), timeout: 60000 },
	async (t) => {
		const { credentials, cleanup } = await startClickHouseContainer({ containerName });
		t.after(cleanup);

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

		await request({
			credentials,
			sql: insertQuery,
			queryInUrl: true,
			body: buildNdjson([{ id: 4, name: 'delta', extra: 'ignored' }]),
			compress: false,
			waitEndOfQuery: true,
			settings: { input_format_skip_unknown_fields: 1 },
		});

		const selectResponse = await request({
			credentials,
			sql: `SELECT id, name FROM ${table} ORDER BY id FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});

		const payload = JSON.parse(selectResponse.body);
		assert.equal(payload.data.length, rows.length + 1);
		assert.deepEqual(payload.data.slice(0, rows.length), rows);
	},
);
