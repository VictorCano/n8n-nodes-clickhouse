const test = require('node:test');
const assert = require('node:assert/strict');
const {
	buildBaseUrl,
	buildQueryString,
	request,
} = require('../dist/nodes/Clickhouse/transport/clickhouseClient');

test('buildBaseUrl builds protocol host port', () => {
	const baseUrl = buildBaseUrl({
		protocol: 'https',
		host: 'clickhouse.internal',
		port: 8443,
	});

	assert.equal(baseUrl, 'https://clickhouse.internal:8443');
});

test('buildBaseUrl strips protocol and trailing slashes', () => {
	const baseUrl = buildBaseUrl({
		protocol: 'http',
		host: 'https://example.com/',
		port: 8123,
	});

	assert.equal(baseUrl, 'http://example.com:8123');
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

test('buildQueryString supports compression off and custom settings', () => {
	const query = buildQueryString({
		compress: false,
		settings: { max_result_rows: 10, allow_experimental: true },
		query: 'SELECT 1',
	});

	const params = new URLSearchParams(query.replace(/^\?/, ''));
	assert.equal(params.get('enable_http_compression'), '0');
	assert.equal(params.get('max_result_rows'), '10');
	assert.equal(params.get('allow_experimental'), '1');
	assert.equal(params.get('query'), 'SELECT 1');
});

test('request builds headers and body for POST', async () => {
	const calls = [];
	const httpRequest = async (options) => {
		calls.push(options);
		return { statusCode: 200, headers: { 'x-test': '1' }, body: 'ok' };
	};

	const credentials = {
		protocol: 'http',
		host: 'localhost',
		port: 8123,
		username: 'user',
		password: 'secret',
		defaultDatabase: 'default',
		tlsIgnoreSsl: false,
	};

	const response = await request({ credentials, sql: 'SELECT 1', httpRequest });
	assert.equal(response.body, 'ok');
	assert.equal(calls.length, 1);
	assert.equal(calls[0].method, 'POST');
	assert.equal(calls[0].body, 'SELECT 1');
	assert.equal(calls[0].headers['Accept-Encoding'], 'gzip');
	assert.equal(calls[0].headers['Content-Type'], 'text/plain; charset=utf-8');
	assert.ok(calls[0].headers.Authorization.startsWith('Basic '));
});

test('request sends SQL in URL when queryInUrl is true', async () => {
	let captured;
	const httpRequest = async (options) => {
		captured = options;
		return { statusCode: 200, headers: {}, body: 'ok' };
	};

	const credentials = {
		protocol: 'http',
		host: 'localhost',
		port: 8123,
		username: 'user',
		password: 'secret',
		tlsIgnoreSsl: false,
	};

	await request({ credentials, sql: 'SELECT 1', queryInUrl: true, httpRequest });
	const url = new URL(captured.url);
	assert.equal(url.searchParams.get('query'), 'SELECT 1');
	assert.equal(captured.body, '');
});

test('request redacts credentials in error output', async () => {
	const httpRequest = async () => ({
		statusCode: 401,
		headers: { 'content-type': 'text/plain' },
		body: 'user:secret failed',
	});
	const credentials = {
		protocol: 'http',
		host: 'localhost',
		port: 8123,
		username: 'user',
		password: 'secret',
		tlsIgnoreSsl: false,
	};

	await assert.rejects(
		() => request({ credentials, sql: 'SELECT 1', httpRequest }),
		(error) => {
			assert.ok(error.message.includes('***'));
			assert.ok(!error.message.includes('secret'));
			return true;
		},
	);
});

test('request throws when gzipRequest is enabled without CompressionStream', async () => {
	const original = global.CompressionStream;
	global.CompressionStream = undefined;
	const credentials = {
		protocol: 'http',
		host: 'localhost',
		port: 8123,
		username: 'user',
		password: 'secret',
		tlsIgnoreSsl: false,
	};

	await assert.rejects(
		() => request({ credentials, sql: 'SELECT 1', gzipRequest: true, httpRequest: async () => ({}) }),
		/CompressionStream is not available/,
	);
	global.CompressionStream = original;
});
