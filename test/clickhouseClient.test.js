const test = require('node:test');
const assert = require('node:assert/strict');
const { buildBaseUrl, buildQueryString } = require('../dist/nodes/Example/transport/clickhouseClient');

test('buildBaseUrl builds protocol host port', () => {
	const baseUrl = buildBaseUrl({
		protocol: 'https',
		host: 'clickhouse.internal',
		port: 8443,
	});

	assert.equal(baseUrl, 'https://clickhouse.internal:8443');
});

test('buildQueryString includes database, compression, wait_end_of_query', () => {
	const query = buildQueryString({
		database: 'analytics',
		waitEndOfQuery: true,
	});

	const params = new URLSearchParams(query.replace(/^\?/, ''));

	assert.equal(params.get('database'), 'analytics');
	assert.equal(params.get('enable_http_compression'), '1');
	assert.equal(params.get('wait_end_of_query'), '1');
});
