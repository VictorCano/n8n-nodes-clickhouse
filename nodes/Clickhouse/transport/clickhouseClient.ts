import type { IHttpRequestOptions, IN8nHttpFullResponse } from 'n8n-workflow';

export type ClickHouseCredentials = {
	protocol: 'http' | 'https';
	host: string;
	port: number | string;
	username: string;
	password: string;
	defaultDatabase?: string;
	tlsIgnoreSsl: boolean;
};

export type ClickHouseRequestOptions = {
	credentials: ClickHouseCredentials;
	sql: string;
	databaseOverride?: string;
	format?: string;
	compress?: boolean;
	settings?: Record<string, string | number | boolean>;
	timeoutMs?: number;
	waitEndOfQuery?: boolean;
	queryInUrl?: boolean;
	body?: string | Buffer;
	requestHeaders?: Record<string, string>;
	gzipRequest?: boolean;
	httpRequest?: HttpRequestFn;
};

export type ClickHouseResponse = {
	status: number;
	headers: Record<string, string>;
	body: string;
};

type HttpRequestFn = (options: IHttpRequestOptions) => Promise<unknown>;

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
	settings?: Record<string, string | number | boolean>;
	waitEndOfQuery?: boolean;
	query?: string;
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

	if (options.query) {
		params.set('query', options.query);
	}

	if (options.settings) {
		for (const [key, value] of Object.entries(options.settings)) {
			params.set(key, normalizeSettingValue(value));
		}
	}

	const query = params.toString();
	return query ? `?${query}` : '';
}

export async function request(options: ClickHouseRequestOptions): Promise<ClickHouseResponse> {
	const { credentials, sql, httpRequest } = options;
	const timeoutMs = options.timeoutMs ?? 60_000;
	const database = options.databaseOverride ?? credentials.defaultDatabase;
	const queryInUrl = options.queryInUrl ?? false;
	const queryString = buildQueryString({
		database,
		format: options.format,
		compress: options.compress,
		settings: options.settings,
		waitEndOfQuery: options.waitEndOfQuery,
		query: queryInUrl ? sql : undefined,
	});

	const baseUrl = buildBaseUrl(credentials);
	const url = `${baseUrl}/${queryString}`;
	const headers: Record<string, string> = {
		'Accept-Encoding': 'gzip',
		'Content-Type': 'text/plain; charset=utf-8',
	};
	if (options.requestHeaders) {
		Object.assign(headers, options.requestHeaders);
	}

	const authToken = Buffer.from(`${credentials.username}:${credentials.password}`, 'utf8').toString(
		'base64',
	);
	headers.Authorization = `Basic ${authToken}`;

	const bodyPayload = options.body ?? (queryInUrl ? '' : sql);
	const requestBody = options.gzipRequest ? await gzipPayload(bodyPayload) : bodyPayload;
	if (options.gzipRequest) {
		headers['Content-Encoding'] = 'gzip';
	}

	const requestOptions: IHttpRequestOptions = {
		method: 'POST',
		url,
		body: requestBody,
		headers,
		timeout: timeoutMs,
		encoding: 'text',
		json: false,
		returnFullResponse: true,
		ignoreHttpStatusErrors: true,
	};

	if (credentials.protocol === 'https') {
		requestOptions.skipSslCertificateValidation = credentials.tlsIgnoreSsl;
	}

	const response = httpRequest
		? await httpRequest(requestOptions)
		: await fetchRequest({
			url,
			headers,
			body: requestBody,
			timeoutMs,
			allowUnsafeTls: credentials.tlsIgnoreSsl,
		});

	const normalized = normalizeResponse(response, credentials);
	if (normalized.status >= 400 || normalized.status === 0) {
		throw buildResponseError(
			normalized.status,
			undefined,
			normalized.headers,
			normalized.body,
			credentials,
		);
	}

	return normalized;
}

async function fetchRequest(options: {
	url: string;
	headers: Record<string, string>;
	body: string | Buffer;
	timeoutMs: number;
	allowUnsafeTls: boolean;
}): Promise<ClickHouseResponse> {
	if (options.allowUnsafeTls) {
		throw new Error('TLS verification cannot be disabled without n8n httpRequest support.');
	}

	const response = await fetch(options.url, {
		method: 'POST',
		headers: options.headers,
		body: options.body,
		signal: AbortSignal.timeout(options.timeoutMs),
	});

	const body = await response.text();
	const headers = normalizeFetchHeaders(response.headers);
	return {
		status: response.status,
		headers,
		body,
	};
}

function normalizeResponse(response: unknown, credentials: ClickHouseCredentials): ClickHouseResponse {
	if (!response || typeof response !== 'object') {
		return {
			status: 0,
			headers: {},
			body: '',
		};
	}

	if (isFullResponse(response)) {
		const headers = normalizeHeaders(response.headers);
		const body = normalizeBody(response.body, credentials);
		return {
			status: response.statusCode ?? 0,
			headers,
			body,
		};
	}

	if ('status' in response && 'headers' in response && 'body' in response) {
		const value = response as ClickHouseResponse;
		return {
			status: value.status ?? 0,
			headers: normalizeHeaders(value.headers ?? {}),
			body: normalizeBody(value.body, credentials),
		};
	}

	return {
		status: 0,
		headers: {},
		body: normalizeBody(response, credentials),
	};
}

function isFullResponse(response: object): response is IN8nHttpFullResponse {
	return 'statusCode' in response && 'headers' in response;
}

function normalizeBody(body: unknown, credentials: ClickHouseCredentials): string {
	if (body === undefined || body === null) {
		return '';
	}
	if (typeof body === 'string') {
		return body;
	}
	if (Buffer.isBuffer(body)) {
		return body.toString('utf8');
	}
	try {
		return JSON.stringify(body);
	} catch {
		return safeExcerpt(String(body), credentials);
	}
}

function sanitizeHost(host: string): string {
	const trimmed = host.trim();
	const withoutProtocol = trimmed.replace(/^https?:\/\//i, '');
	return withoutProtocol.replace(/\/+$/, '');
}

function normalizeHeaders(headers: Record<string, unknown>): Record<string, string> {
	const result: Record<string, string> = {};
	for (const [key, value] of Object.entries(headers)) {
		if (!key || value === undefined || value === null) continue;
		result[key.toLowerCase()] = Array.isArray(value) ? value.join(', ') : String(value);
	}
	return result;
}

function normalizeFetchHeaders(headers: Headers): Record<string, string> {
	const normalized: Record<string, string> = {};
	for (const [key, value] of headers.entries()) {
		normalized[key.toLowerCase()] = value;
	}
	return normalized;
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
	const trimmed = body.trim();
	const looksJson =
		contentType.includes('application/json') || trimmed.startsWith('{') || trimmed.startsWith('[');

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

function normalizeSettingValue(value: string | number | boolean): string {
	if (typeof value === 'boolean') {
		return value ? '1' : '0';
	}
	return String(value);
}

async function gzipPayload(payload: string | Buffer): Promise<Buffer> {
	if (typeof CompressionStream !== 'function') {
		throw new Error('CompressionStream is not available to gzip the request body.');
	}
	const stream = new CompressionStream('gzip');
	const writer = stream.writable.getWriter();
	const data = typeof payload === 'string' ? new TextEncoder().encode(payload) : payload;
	await writer.write(data);
	await writer.close();
	const arrayBuffer = await new Response(stream.readable).arrayBuffer();
	return Buffer.from(arrayBuffer);
}
