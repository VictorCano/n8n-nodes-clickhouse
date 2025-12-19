const test = require('node:test');
const assert = require('node:assert/strict');
const { Clickhouse } = require('../dist/nodes/Clickhouse/Clickhouse.node');

function createContext({ items, params, credentials, httpRequest }) {
	return {
		helpers: {
			httpRequest,
		},
		getInputData() {
			return items;
		},
		getNodeParameter(name, itemIndex) {
			const fallback = params[0] || {};
			const scope = params[itemIndex] || fallback;
			return scope[name];
		},
		async getCredentials() {
			return credentials;
		},
	};
}

const baseCredentials = {
	protocol: 'http',
	host: 'localhost',
	port: 8123,
	username: 'user',
	password: 'secret',
	defaultDatabase: 'default',
	tlsIgnoreSsl: false,
};

test('executeQuery respects limit toggle off (no wrapper)', async () => {
	let capturedSql = '';
	const httpRequest = async (options) => {
		capturedSql = options.body;
		return {
			statusCode: 200,
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ data: [{ value: 1 }], meta: [], statistics: {} }),
		};
	};

	const node = new Clickhouse();
	const items = [{ json: {} }];
	const params = [
		{
			resource: 'query',
			operation: 'executeQuery',
			query: 'SELECT 1',
			limitEnabled: false,
			limit: 50,
			paginate: false,
			outputMode: 'single',
			databaseOverride: '',
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	const result = await node.execute.call(context);
	assert.equal(capturedSql, 'SELECT 1');
	assert.equal(result[0][0].json.rows.length, 1);
});

test('executeQuery wraps SQL when limit is enabled', async () => {
	let capturedSql = '';
	const httpRequest = async (options) => {
		capturedSql = options.body;
		return {
			statusCode: 200,
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ data: [{ value: 1 }], meta: [], statistics: {} }),
		};
	};

	const node = new Clickhouse();
	const items = [{ json: {} }];
	const params = [
		{
			resource: 'query',
			operation: 'executeQuery',
			query: 'SELECT 1',
			limitEnabled: true,
			limit: 10,
			paginate: false,
			outputMode: 'single',
			databaseOverride: '',
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	await node.execute.call(context);
	assert.equal(capturedSql, 'SELECT * FROM (SELECT 1) LIMIT 10');
});

test('executeQuery rejects pagination when limit is disabled', async () => {
	const node = new Clickhouse();
	const items = [{ json: {} }];
	const params = [
		{
			resource: 'query',
			operation: 'executeQuery',
			query: 'SELECT 1',
			limitEnabled: false,
			limit: 10,
			paginate: true,
			outputMode: 'single',
			databaseOverride: '',
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({
		items,
		params,
		credentials: baseCredentials,
		httpRequest: async () => ({}),
	});

	await assert.rejects(() => node.execute.call(context), /Limit must be greater than 0/);
});

test('executeQuery output mode perRow returns each row', async () => {
	const httpRequest = async () => ({
		statusCode: 200,
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ data: [{ id: 1 }, { id: 2 }], meta: [], statistics: {} }),
	});

	const node = new Clickhouse();
	const items = [{ json: {} }];
	const params = [
		{
			resource: 'query',
			operation: 'executeQuery',
			query: 'SELECT 1',
			limitEnabled: false,
			limit: 50,
			paginate: false,
			outputMode: 'perRow',
			databaseOverride: '',
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	const result = await node.execute.call(context);
	assert.equal(result[0].length, 2);
	assert.deepEqual(result[0][0].json, { id: 1 });
});

test('executeCommand returns summary with query id and headers', async () => {
	const httpRequest = async () => ({
		statusCode: 200,
		headers: { 'x-clickhouse-query-id': 'qid', 'set-cookie': 'ignore' },
		body: 'OK',
	});
	const node = new Clickhouse();
	const items = [{ json: {} }];
	const params = [
		{
			resource: 'command',
			operation: 'executeCommand',
			command: 'CREATE TABLE test (id UInt8) ENGINE=Memory',
			databaseOverride: '',
			timeoutMs: 1000,
			compress: true,
		},
	];
	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	const result = await node.execute.call(context);
	const summary = result[0][0].json;
	assert.equal(summary.queryId, 'qid');
	assert.equal(summary.body, 'OK');
	assert.equal(summary.headers['set-cookie'], undefined);
});

test('executeInsert from items batches rows and returns summary', async () => {
	const calls = [];
	const httpRequest = async (options) => {
		calls.push(options);
		return { statusCode: 200, headers: { 'x-insert': String(calls.length) }, body: '' };
	};

	const node = new Clickhouse();
	const items = [{ json: { id: 1 } }, { json: { id: 2 } }];
	const params = [
		{
			resource: 'insert',
			operation: 'insertFromItems',
			databaseOverride: '',
			table: 'events',
			columnsCsv: 'id',
			columnsUi: {},
			batchSize: 1,
			ignoreUnknownFields: false,
			gzipRequest: false,
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	const result = await node.execute.call(context);
	assert.equal(calls.length, 2);
	assert.equal(result[0][0].json.inserted, 2);
	assert.equal(result[0][0].json.batches, 2);
});

test('executeInsert from JSON array field uses path', async () => {
	let capturedBody = '';
	const httpRequest = async (options) => {
		capturedBody = options.body;
		return { statusCode: 200, headers: {}, body: '' };
	};
	const node = new Clickhouse();
	const items = [{ json: { rows: [{ id: 1 }, { id: 2 }] } }];
	const params = [
		{
			resource: 'insert',
			operation: 'insertFromJson',
			jsonArrayField: 'rows',
			databaseOverride: '',
			table: 'events',
			columnsCsv: '',
			columnsUi: {},
			batchSize: 1000,
			ignoreUnknownFields: false,
			gzipRequest: false,
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	await node.execute.call(context);
	assert.equal(capturedBody, '{"id":1}\n{"id":2}');
});

test('executeInsert passes ignoreUnknownFields setting', async () => {
	let capturedUrl = '';
	const httpRequest = async (options) => {
		capturedUrl = options.url;
		return { statusCode: 200, headers: {}, body: '' };
	};
	const node = new Clickhouse();
	const items = [{ json: { id: 1 } }];
	const params = [
		{
			resource: 'insert',
			operation: 'insertFromItems',
			databaseOverride: '',
			table: 'events',
			columnsCsv: 'id',
			columnsUi: {},
			batchSize: 1000,
			ignoreUnknownFields: true,
			gzipRequest: false,
			timeoutMs: 1000,
			compress: true,
		},
	];

	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	await node.execute.call(context);
	const url = new URL(capturedUrl);
	assert.equal(url.searchParams.get('input_format_skip_unknown_fields'), '1');
});

test('metadata listDatabases returns rows', async () => {
	const httpRequest = async () => ({
		statusCode: 200,
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ data: [{ name: 'default' }] }),
	});
	const node = new Clickhouse();
	const items = [{ json: {} }];
	const params = [
		{
			resource: 'metadata',
			operation: 'listDatabases',
			databaseOverride: '',
			metadataDatabase: '',
			timeoutMs: 1000,
			compress: true,
		},
	];
	const context = createContext({ items, params, credentials: baseCredentials, httpRequest });
	const result = await node.execute.call(context);
	assert.deepEqual(result[0][0].json, { name: 'default' });
});

test('loadOptions getTables uses metadata database override', async () => {
	let capturedSql = '';
	const httpRequest = async (options) => {
		capturedSql = options.body;
		return {
			statusCode: 200,
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ data: [{ name: 'events' }] }),
		};
	};
	const node = new Clickhouse();
	const context = {
		helpers: { httpRequest },
		getCredentials: async () => baseCredentials,
		getNodeParameter: (name) => {
			if (name === 'databaseOverride') return '';
			if (name === 'metadataDatabase') return 'analytics';
			if (name === 'timeoutMs') return 1000;
			if (name === 'compress') return true;
			return '';
		},
	};

	const options = await node.methods.loadOptions.getTables.call(context);
	assert.equal(capturedSql, 'SHOW TABLES FROM analytics FORMAT JSON');
	assert.deepEqual(options, [{ name: 'events', value: 'events' }]);
});
