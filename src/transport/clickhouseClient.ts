import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import type { IncomingHttpHeaders, RequestOptions } from 'node:http';
import { createGunzip } from 'node:zlib';

export type ClickHouseCredentials = {
	protocol: 'http' | 'https';
	host: string;
	port: number | string;
	username: string;
	password: string;
	defaultDatabase?: string;
	tlsIgnoreSsl: boolean;
	ca?: string;
	cert?: string;
	key?: string;
	passphrase?: string;
};

export type ClickHouseRequestOptions = {
	credentials: ClickHouseCredentials;
	sql: string;
	databaseOverride?: string;
	format?: string;
	compress?: boolean;
	timeoutMs?: number;
	waitEndOfQuery?: boolean;
};

export type ClickHouseResponse = {
	status: number;
	headers: Record<string, string>;
	body: string;
};

export function buildBaseUrl(credentials: Pick<ClickHouseCredentials, 'protocol' | 'host' | 'port'>): string {
	const protocol = credentials.protocol;
	const host = sanitizeHost(credentials.host);
	const port = String(credentials.port);
	return `${protocol}://${host}:${port}`;
}

export function buildQueryString(options: {
	database?: string;
	format?: string;
	compress?: boolean;
	waitEndOfQuery?: boolean;
}): string {
	const params = new URLSearchParams();

	if (options.database) {
		params.set('database', options.database);
	}

	if (options.format) {
		params.set('default_format', options.format);
	}

	const enableCompression = options.compress ?? true;
	params.set('enable_http_compression', enableCompression ? '1' : '0');

	if (options.waitEndOfQuery) {
		params.set('wait_end_of_query', '1');
	}

	const query = params.toString();
	return query ? `?${query}` : '';
}

export async function request(options: ClickHouseRequestOptions): Promise<ClickHouseResponse> {
	const { credentials, sql } = options;
	const timeoutMs = options.timeoutMs ?? 60_000;
	const database = options.databaseOverride ?? credentials.defaultDatabase;
	const queryString = buildQueryString({
		database,
		format: options.format,
		compress: options.compress,
		waitEndOfQuery: options.waitEndOfQuery,
	});

	const hostname = sanitizeHost(credentials.host);
	const path = queryString ? `/${queryString}` : '/';
	const headers: Record<string, string> = {
		'Accept-Encoding': 'gzip',
		'Content-Type': 'text/plain; charset=utf-8',
	};

	const authToken = Buffer.from(`${credentials.username}:${credentials.password}`, 'utf8').toString(
		'base64',
	);
	headers.Authorization = `Basic ${authToken}`;
	headers['Content-Length'] = Buffer.byteLength(sql, 'utf8').toString();

	const requestOptions: RequestOptions & {
		rejectUnauthorized?: boolean;
		ca?: string;
		cert?: string;
		key?: string;
		passphrase?: string;
	} = {
		method: 'POST',
		hostname,
		port: credentials.port,
		path,
		headers,
	};

	if (credentials.protocol === 'https') {
		requestOptions.rejectUnauthorized = !credentials.tlsIgnoreSsl;
		if (credentials.ca) requestOptions.ca = credentials.ca;
		if (credentials.cert) requestOptions.cert = credentials.cert;
		if (credentials.key) requestOptions.key = credentials.key;
		if (credentials.passphrase) requestOptions.passphrase = credentials.passphrase;
	}

	return await new Promise<ClickHouseResponse>((resolve, reject) => {
		const transport = credentials.protocol === 'https' ? httpsRequest : httpRequest;
		const req = transport(requestOptions, (res) => {
			const status = res.statusCode ?? 0;
			const headersMap = normalizeHeaders(res.headers);
			const encoding = (headersMap['content-encoding'] || '').toLowerCase();
			const stream = encoding.includes('gzip') ? res.pipe(createGunzip()) : res;
			const chunks: Buffer[] = [];

			stream.on('data', (chunk: Buffer) => {
				chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
			});
			stream.on('error', (error) => {
				reject(wrapError(error, credentials));
			});
			stream.on('end', () => {
				const body = Buffer.concat(chunks).toString('utf8');
				if (status >= 400 || status === 0) {
					reject(buildResponseError(status, res.statusMessage, headersMap, body, credentials));
					return;
				}
				resolve({ status, headers: headersMap, body });
			});
		});

		req.on('error', (error) => {
			reject(wrapError(error, credentials));
		});

		req.setTimeout(timeoutMs, () => {
			req.destroy(new Error(`Request timed out after ${timeoutMs}ms`));
		});

		req.write(sql);
		req.end();
	});
}

function sanitizeHost(host: string): string {
	const trimmed = host.trim();
	const withoutProtocol = trimmed.replace(/^https?:\/\//i, '');
	return withoutProtocol.replace(/\/+$/, '');
}

function normalizeHeaders(headers: IncomingHttpHeaders): Record<string, string> {
	const result: Record<string, string> = {};
	for (const [key, value] of Object.entries(headers)) {
		if (!key || value === undefined) continue;
		result[key.toLowerCase()] = Array.isArray(value) ? value.join(', ') : String(value);
	}
	return result;
}

function wrapError(error: Error, credentials: ClickHouseCredentials): Error {
	const message = redactSecrets(error.message, credentials);
	const wrapped = new Error(`ClickHouse request failed: ${message}`);
	(wrapped as { cause?: Error }).cause = error;
	return wrapped;
}

function buildResponseError(
	status: number,
	statusMessage: string | undefined,
	headers: Record<string, string>,
	body: string,
	credentials: ClickHouseCredentials,
): Error {
	const statusLabel = statusMessage ? ` ${statusMessage}` : '';
	const detail = buildErrorDetail(body, headers, credentials);
	return new Error(`ClickHouse request failed with status ${status}${statusLabel}. ${detail}`);
}

function buildErrorDetail(
	body: string,
	headers: Record<string, string>,
	credentials: ClickHouseCredentials,
): string {
	const contentType = headers['content-type'] || '';
	const looksJson = contentType.includes('application/json') || body.trim().startsWith('{') || body.trim().startsWith('[');

	if (looksJson) {
		try {
			const parsed = JSON.parse(body) as unknown;
			return `Response JSON: ${redactSecrets(JSON.stringify(parsed), credentials)}`;
		} catch {
			return `Response: ${safeExcerpt(body, credentials)}`;
		}
	}

	return `Response: ${safeExcerpt(body, credentials)}`;
}

function safeExcerpt(body: string, credentials: ClickHouseCredentials, maxLength = 500): string {
	const excerpt = body.length > maxLength ? `${body.slice(0, maxLength)}...` : body;
	return redactSecrets(excerpt, credentials);
}

function redactSecrets(value: string, credentials: ClickHouseCredentials): string {
	let output = value;
	const secrets = [credentials.username, credentials.password].filter((item) => item);
	for (const secret of secrets) {
		output = output.split(secret).join('***');
	}
	return output;
}
