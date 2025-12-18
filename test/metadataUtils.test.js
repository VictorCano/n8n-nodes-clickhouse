const test = require('node:test');
const assert = require('node:assert/strict');
const { parseMetadataJson } = require('../dist/nodes/Example/operations/metadataUtils');

test('parseMetadataJson returns rows and meta', () => {
	const payload = {
		meta: [{ name: 'name', type: 'String' }],
		data: [{ name: 'default' }, { name: 'analytics' }],
		statistics: { elapsed: 0.001 },
	};

	const result = parseMetadataJson(JSON.stringify(payload));
	assert.deepEqual(result.rows, payload.data);
	assert.deepEqual(result.meta, payload.meta);
	assert.deepEqual(result.statistics, payload.statistics);
});

test('parseMetadataJson handles missing fields', () => {
	const result = parseMetadataJson(JSON.stringify({ data: 'oops' }));
	assert.deepEqual(result.rows, []);
	assert.deepEqual(result.meta, []);
	assert.deepEqual(result.statistics, {});
});
