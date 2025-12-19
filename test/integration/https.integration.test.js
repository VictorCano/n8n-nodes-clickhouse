const test = require('node:test');
const assert = require('node:assert/strict');
const https = require('node:https');
const { request } = require('../../dist/nodes/Clickhouse/transport/clickhouseClient');
const { startClickHouseTlsContainer, dockerAvailable } = require('./helpers');

const containerName = `clickhouse-https-${Date.now()}`;

async function httpsRequest(options) {
	return new Promise((resolve, reject) => {
		const url = new URL(options.url);
		const req = https.request(
			{
				method: options.method,
				hostname: url.hostname,
				port: url.port,
				path: `${url.pathname}${url.search}`,
				headers: options.headers,
				rejectUnauthorized: !options.skipSslCertificateValidation,
			},
			(res) => {
				let body = '';
				res.on('data', (chunk) => {
					body += chunk;
				});
				res.on('end', () => {
					resolve({ statusCode: res.statusCode, headers: res.headers, body });
				});
			},
		);
		req.on('error', reject);
		if (options.body) {
			req.write(options.body);
		}
		req.end();
	});
}

test(
	'https integration with tlsIgnoreSsl',
	{ skip: !dockerAvailable(), timeout: 60000 },
	async (t) => {
		const { credentials, cleanup } = await startClickHouseTlsContainer({ containerName });
		t.after(cleanup);

		const response = await request({
			credentials,
			sql: 'SELECT 1 FORMAT JSON',
			compress: false,
			waitEndOfQuery: true,
			httpRequest: httpsRequest,
		});
		const payload = JSON.parse(response.body);
		assert.equal(payload.data[0]['1'], 1);
	},
);
