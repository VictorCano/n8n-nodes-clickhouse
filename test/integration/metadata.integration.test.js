const test = require('node:test');
const assert = require('node:assert/strict');
const { request } = require('../../dist/nodes/Clickhouse/transport/clickhouseClient');
const { startClickHouseContainer, dockerAvailable } = require('./helpers');

const containerName = `clickhouse-metadata-${Date.now()}`;

test(
	'metadata integration',
	{ skip: !dockerAvailable() },
	async (t) => {
		const { credentials, cleanup } = await startClickHouseContainer({ containerName });
		t.after(cleanup);

		const database = `n8n_meta_${Date.now()}`;
		const table = `events_${Date.now()}`;

		await request({
			credentials,
			sql: `CREATE DATABASE ${database}`,
			queryInUrl: true,
			compress: false,
		});
		await request({
			credentials,
			sql: `CREATE TABLE ${database}.${table} (id UInt32, name String) ENGINE = Memory`,
			queryInUrl: true,
			compress: false,
		});

		const databases = await request({
			credentials,
			sql: 'SHOW DATABASES FORMAT JSON',
			compress: false,
			waitEndOfQuery: true,
		});
		const databasesPayload = JSON.parse(databases.body);
		assert.ok(databasesPayload.data.some((row) => row.name === database));

		const tables = await request({
			credentials,
			sql: `SHOW TABLES FROM ${database} FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});
		const tablesPayload = JSON.parse(tables.body);
		assert.ok(tablesPayload.data.some((row) => row.name === table));

		const columns = await request({
			credentials,
			sql: `DESCRIBE TABLE ${database}.${table} FORMAT JSON`,
			compress: false,
			waitEndOfQuery: true,
		});
		const columnsPayload = JSON.parse(columns.body);
		assert.ok(columnsPayload.data.some((row) => row.name === 'id'));
	},
);
