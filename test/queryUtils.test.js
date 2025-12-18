const test = require('node:test');
const assert = require('node:assert/strict');
const { buildPaginatedSql, shapeQueryOutput } = require('../dist/nodes/Clickhouse/operations/queryUtils');

test('buildPaginatedSql wraps query with limit and offset', () => {
	const sql = 'SELECT id FROM logs;';
	const wrapped = buildPaginatedSql(sql, 100, 200);

	assert.equal(wrapped, 'SELECT * FROM (SELECT id FROM logs) LIMIT 100 OFFSET 200');
});

test('shapeQueryOutput returns consistent structure', () => {
	const output = shapeQueryOutput({
		rows: [{ id: 1 }],
		meta: [{ name: 'id', type: 'UInt64' }],
		statistics: { elapsed: 0.01 },
		summary: { rowCount: 1 },
	});

	assert.deepEqual(output.rows, [{ id: 1 }]);
	assert.deepEqual(output.meta, [{ name: 'id', type: 'UInt64' }]);
	assert.deepEqual(output.statistics, { elapsed: 0.01 });
	assert.deepEqual(output.summary, { rowCount: 1 });

	const fallback = shapeQueryOutput({});
	assert.deepEqual(fallback.rows, []);
	assert.deepEqual(fallback.meta, []);
	assert.deepEqual(fallback.statistics, {});
	assert.deepEqual(fallback.summary, { rowCount: 0 });
});
