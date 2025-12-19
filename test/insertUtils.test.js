const test = require('node:test');
const assert = require('node:assert/strict');
const {
	buildInsertQuery,
	buildNdjson,
	chunkRows,
	parseColumns,
} = require('../dist/nodes/Clickhouse/operations/insertUtils');

test('buildInsertQuery builds basic insert statement', () => {
	const sql = buildInsertQuery({ table: 'events' });
	assert.equal(sql, 'INSERT INTO events FORMAT JSONEachRow');
});

test('buildInsertQuery qualifies table with database', () => {
	const sql = buildInsertQuery({ table: 'events', database: 'analytics' });
	assert.equal(sql, 'INSERT INTO analytics.events FORMAT JSONEachRow');
});

test('buildInsertQuery includes columns when provided', () => {
	const sql = buildInsertQuery({ table: 'events', columns: ['id', 'name'] });
	assert.equal(sql, 'INSERT INTO events (id, name) FORMAT JSONEachRow');
});

test('buildInsertQuery trims table and database', () => {
	const sql = buildInsertQuery({ table: ' events ', database: ' test ' });
	assert.equal(sql, 'INSERT INTO test.events FORMAT JSONEachRow');
});

test('buildInsertQuery throws on empty table name', () => {
	assert.throws(() => buildInsertQuery({ table: '  ' }), /Table name is required/);
});

test('buildNdjson converts rows to newline-delimited JSON', () => {
	const output = buildNdjson([{ id: 1 }, { id: 2 }]);
	assert.equal(output, '{"id":1}\n{"id":2}');
});

test('buildNdjson handles empty array', () => {
	assert.equal(buildNdjson([]), '');
});

test('chunkRows splits into batches', () => {
	const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
	const batches = chunkRows(rows, 2);
	assert.equal(batches.length, 2);
	assert.deepEqual(batches[0], [{ id: 1 }, { id: 2 }]);
	assert.deepEqual(batches[1], [{ id: 3 }]);
});

test('chunkRows enforces minimum batch size of 1', () => {
	const rows = [{ id: 1 }, { id: 2 }];
	const batches = chunkRows(rows, 0);
	assert.equal(batches.length, 2);
});

test('parseColumns prefers UI collection over CSV', () => {
	const columns = parseColumns({
		columnsCsv: 'id,name',
		columnsUi: { columns: [{ column: 'first' }, { column: 'second' }] },
	});
	assert.deepEqual(columns, ['first', 'second']);
});

test('parseColumns falls back to CSV', () => {
	const columns = parseColumns({ columnsCsv: ' id , name ,, ' });
	assert.deepEqual(columns, ['id', 'name']);
});
