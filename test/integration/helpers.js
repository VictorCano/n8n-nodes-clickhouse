const { execSync } = require('node:child_process');
const https = require('node:https');
const path = require('node:path');

const image = process.env.CLICKHOUSE_TEST_IMAGE || 'clickhouse/clickhouse-server:latest';

function dockerAvailable() {
	try {
		execSync('docker info', { stdio: 'ignore' });
		return true;
	} catch {
		return false;
	}
}

async function waitForPing(url, authHeader, insecure = false) {
	const timeoutMs = 30_000;
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		try {
			if (url.startsWith('https://')) {
				await new Promise((resolve, reject) => {
					const req = https.request(
						url,
						{
							rejectUnauthorized: !insecure,
							headers: authHeader ? { Authorization: authHeader } : undefined,
						},
						(res) => {
							if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
								resolve();
							} else {
								reject(new Error(`Ping failed with status ${res.statusCode}`));
							}
						},
					);
					req.on('error', reject);
					req.end();
				});
				return;
			}
			const res = await fetch(url, {
				headers: authHeader ? { Authorization: authHeader } : undefined,
			});
			if (res.ok) return;
		} catch {
			// ignore until timeout
		}
		await new Promise((resolve) => setTimeout(resolve, 500));
	}
	throw new Error('ClickHouse did not start in time');
}

async function startClickHouseContainer({
	containerName,
	username = 'default',
	password = 'pass',
} = {}) {
	try {
		execSync(`docker rm -f ${containerName}`, { stdio: 'ignore' });
	} catch {
		// ignore
	}

	const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
	execSync(
		`docker run -d --name ${containerName} -p 0:8123 ` +
			`-e CLICKHOUSE_USER=${username} -e CLICKHOUSE_PASSWORD=${password} ` +
			`-e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 ${image}`,
		{ stdio: 'ignore' },
	);
	const portLine = execSync(`docker port ${containerName} 8123/tcp`).toString().trim();
	const hostPort = portLine.split(':').pop();
	await waitForPing(`http://127.0.0.1:${hostPort}/ping`, authHeader);

	const credentials = {
		protocol: 'http',
		host: '127.0.0.1',
		port: Number(hostPort),
		username,
		password,
		defaultDatabase: 'default',
		tlsIgnoreSsl: false,
	};

	return {
		credentials,
		hostPort,
		authHeader,
		cleanup: () => {
			try {
				execSync(`docker rm -f ${containerName}`, { stdio: 'ignore' });
			} catch {
				// ignore
			}
		},
	};
}

async function startClickHouseTlsContainer({
	containerName,
	username = 'default',
	password = 'pass',
} = {}) {
	try {
		execSync(`docker rm -f ${containerName}`, { stdio: 'ignore' });
	} catch {
		// ignore
	}
	const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
	const tlsDir = path.join(process.cwd(), 'docker', 'clickhouse', 'tls');
	execSync(
		`docker run -d --name ${containerName} -p 0:8443 ` +
			`-e CLICKHOUSE_USER=${username} -e CLICKHOUSE_PASSWORD=${password} ` +
			`-e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 ` +
			`-v ${tlsDir}/https.xml:/etc/clickhouse-server/config.d/https.xml:ro ` +
			`-v ${tlsDir}/server.crt:/etc/clickhouse-server/certs/server.crt:ro ` +
			`-v ${tlsDir}/server.key:/etc/clickhouse-server/certs/server.key:ro ${image}`,
		{ stdio: 'ignore' },
	);
	const portLine = execSync(`docker port ${containerName} 8443/tcp`).toString().trim();
	const hostPort = portLine.split(':').pop();
	await waitForPing(`https://127.0.0.1:${hostPort}/ping`, authHeader, true);

	const credentials = {
		protocol: 'https',
		host: '127.0.0.1',
		port: Number(hostPort),
		username,
		password,
		defaultDatabase: 'default',
		tlsIgnoreSsl: true,
	};

	return {
		credentials,
		hostPort,
		authHeader,
		cleanup: () => {
			try {
				execSync(`docker rm -f ${containerName}`, { stdio: 'ignore' });
			} catch {
				// ignore
			}
		},
	};
}

module.exports = {
	dockerAvailable,
	startClickHouseContainer,
	startClickHouseTlsContainer,
};
