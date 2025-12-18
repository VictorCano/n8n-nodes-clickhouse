import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';
import {
	request as clickhouseRequest,
	type ClickHouseCredentials,
} from '../../src/transport/clickhouseClient';
import { buildPaginatedSql, shapeQueryOutput } from '../../src/operations/queryUtils';
import { buildInsertQuery, buildNdjson, chunkRows, parseColumns } from '../../src/operations/insertUtils';

export class Example implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'ClickHouse',
		name: 'clickhouse',
		icon: { light: 'file:example.svg', dark: 'file:example.dark.svg' },
		group: ['input'],
		version: 1,
		description: 'Work with ClickHouse',
		defaults: {
			name: 'ClickHouse',
		},
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'ClickHouseApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				options: [
					{ name: 'Query', value: 'query' },
					{ name: 'Command', value: 'command' },
					{ name: 'Insert', value: 'insert' },
					{ name: 'Metadata', value: 'metadata' },
				],
				default: 'query',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['query'],
					},
				},
				options: [{ name: 'Execute Query', value: 'executeQuery' }],
				default: 'executeQuery',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['command'],
					},
				},
				options: [{ name: 'Execute Command', value: 'executeCommand' }],
				default: 'executeCommand',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['insert'],
					},
				},
				options: [
					{ name: 'Insert Rows (from input items)', value: 'insertFromItems' },
					{ name: 'Insert Rows (from JSON array field)', value: 'insertFromJson' },
				],
				default: 'insertFromItems',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['metadata'],
					},
				},
				options: [
					{ name: 'List Databases', value: 'listDatabases' },
					{ name: 'List Tables', value: 'listTables' },
					{ name: 'List Columns', value: 'listColumns' },
				],
				default: 'listDatabases',
			},
			{
				displayName: 'Database Override',
				name: 'databaseOverride',
				type: 'string',
				default: '',
				description: 'Override the default database from credentials',
				displayOptions: {
					show: {
						resource: ['query', 'command', 'insert', 'metadata'],
					},
				},
			},
			{
				displayName: 'Timeout (ms)',
				name: 'timeoutMs',
				type: 'number',
				default: 60000,
				description: 'Request timeout in milliseconds',
				displayOptions: {
					show: {
						resource: ['query', 'command', 'insert', 'metadata'],
					},
				},
			},
			{
				displayName: 'Enable Compression',
				name: 'compress',
				type: 'boolean',
				default: true,
				description: 'Enable HTTP compression',
				displayOptions: {
					show: {
						resource: ['query', 'command', 'insert', 'metadata'],
					},
				},
			},
			{
				displayName: 'SQL Query',
				name: 'query',
				type: 'string',
				default: '',
				typeOptions: {
					rows: 4,
				},
				description: 'SQL query to execute',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['executeQuery'],
					},
				},
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				default: 100,
				description: 'Maximum number of rows to return',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['executeQuery'],
					},
				},
			},
			{
				displayName: 'Pagination',
				name: 'paginate',
				type: 'boolean',
				default: false,
				description: 'Fetch additional pages until no more results are returned',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['executeQuery'],
					},
				},
			},
			{
				displayName: 'Output Mode',
				name: 'outputMode',
				type: 'options',
				options: [
					{ name: 'Single Item (Rows Array)', value: 'single' },
					{ name: 'One Item per Row', value: 'perRow' },
				],
				default: 'single',
				displayOptions: {
					show: {
						resource: ['query'],
						operation: ['executeQuery'],
					},
				},
			},
			{
				displayName: 'SQL Command',
				name: 'command',
				type: 'string',
				default: '',
				typeOptions: {
					rows: 4,
				},
				description: 'SQL command to execute',
				displayOptions: {
					show: {
						resource: ['command'],
						operation: ['executeCommand'],
					},
				},
			},
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				default: '',
				description: 'Target table for insert operations',
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromItems', 'insertFromJson'],
					},
				},
			},
			{
				displayName: 'Columns (CSV)',
				name: 'columnsCsv',
				type: 'string',
				default: '',
				description: 'Comma-separated list of columns to insert',
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromItems', 'insertFromJson'],
					},
				},
			},
			{
				displayName: 'Columns',
				name: 'columnsUi',
				type: 'fixedCollection',
				default: {},
				typeOptions: {
					multipleValues: true,
				},
				description: 'Optional list of columns to insert',
				options: [
					{
						name: 'columns',
						displayName: 'Columns',
						values: [
							{
								displayName: 'Column',
								name: 'column',
								type: 'string',
								default: '',
							},
						],
					},
				],
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromItems', 'insertFromJson'],
					},
				},
			},
			{
				displayName: 'Batch Size',
				name: 'batchSize',
				type: 'number',
				default: 1000,
				description: 'Number of rows per insert batch',
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromItems', 'insertFromJson'],
					},
				},
			},
			{
				displayName: 'Ignore Unknown Fields',
				name: 'ignoreUnknownFields',
				type: 'boolean',
				default: false,
				description: 'Skip fields not present in the target table',
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromItems', 'insertFromJson'],
					},
				},
			},
			{
				displayName: 'Gzip Request',
				name: 'gzipRequest',
				type: 'boolean',
				default: false,
				description: 'Compress request payload with gzip',
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromItems', 'insertFromJson'],
					},
				},
			},
			{
				displayName: 'JSON Array Field',
				name: 'jsonArrayField',
				type: 'string',
				default: 'rows',
				description: 'Field that contains a JSON array of rows to insert',
				displayOptions: {
					show: {
						resource: ['insert'],
						operation: ['insertFromJson'],
					},
				},
			},
			{
				displayName: 'Table',
				name: 'metadataTable',
				type: 'string',
				default: '',
				description: 'Table name for column metadata',
				displayOptions: {
					show: {
						resource: ['metadata'],
						operation: ['listColumns'],
					},
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const results: INodeExecutionData[] = [];
		const credentials = normalizeCredentials(
			(await this.getCredentials('ClickHouseApi')) as ClickHouseCredentials,
		);
		const firstResource = this.getNodeParameter('resource', 0) as string;
		const firstOperation = this.getNodeParameter('operation', 0) as string;

		if (firstResource === 'insert') {
			const databaseOverride = normalizeOptionalString(
				this.getNodeParameter('databaseOverride', 0) as string,
			);
			const table = this.getNodeParameter('table', 0) as string;
			const columnsCsv = this.getNodeParameter('columnsCsv', 0) as string;
			const columnsUi = this.getNodeParameter('columnsUi', 0);
			const batchSize = this.getNodeParameter('batchSize', 0) as number;
			const ignoreUnknownFields = this.getNodeParameter('ignoreUnknownFields', 0) as boolean;
			const gzipRequest = this.getNodeParameter('gzipRequest', 0) as boolean;
			const timeoutMs = this.getNodeParameter('timeoutMs', 0) as number;
			const compress = this.getNodeParameter('compress', 0) as boolean;

			let rows: IDataObject[] = [];
			if (firstOperation === 'insertFromItems') {
				rows = items.map((item) => item.json).filter(isRecord);
			} else if (firstOperation === 'insertFromJson') {
				rows = collectRowsFromJsonField(items, (itemIndex) =>
					this.getNodeParameter('jsonArrayField', itemIndex) as string,
				);
			}

			const summary = await executeInsert({
				credentials,
				databaseOverride,
				table,
				columns: parseColumns({ columnsCsv, columnsUi }),
				batchSize,
				ignoreUnknownFields,
				gzipRequest,
				timeoutMs,
				compress,
				rows,
			});

			results.push({
				json: summary,
				pairedItem: { item: 0 },
			});

			return [results];
		}

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			const resource = this.getNodeParameter('resource', itemIndex) as string;
			const operation = this.getNodeParameter('operation', itemIndex) as string;

			if (resource === 'query' && operation === 'executeQuery') {
				const sql = this.getNodeParameter('query', itemIndex) as string;
				const limit = this.getNodeParameter('limit', itemIndex) as number;
				const paginate = this.getNodeParameter('paginate', itemIndex) as boolean;
				const outputMode = this.getNodeParameter('outputMode', itemIndex) as string;
				const databaseOverride = normalizeOptionalString(
					this.getNodeParameter('databaseOverride', itemIndex) as string,
				);
				const timeoutMs = this.getNodeParameter('timeoutMs', itemIndex) as number;
				const compress = this.getNodeParameter('compress', itemIndex) as boolean;

				const queryResult = await executeQuery({
					credentials,
					sql,
					limit,
					paginate,
					databaseOverride,
					timeoutMs,
					compress,
				});

				if (outputMode === 'perRow') {
					for (const row of queryResult.rows) {
						results.push({
							json: row,
							pairedItem: { item: itemIndex },
						});
					}
					continue;
				}

				results.push({
					json: shapeQueryOutput({
						rows: queryResult.rows,
						meta: queryResult.meta,
						statistics: queryResult.statistics,
						summary: queryResult.summary,
					}),
					pairedItem: { item: itemIndex },
				});
				continue;
			}

			if (resource === 'command' && operation === 'executeCommand') {
				const sql = this.getNodeParameter('command', itemIndex) as string;
				const databaseOverride = normalizeOptionalString(
					this.getNodeParameter('databaseOverride', itemIndex) as string,
				);
				const timeoutMs = this.getNodeParameter('timeoutMs', itemIndex) as number;
				const compress = this.getNodeParameter('compress', itemIndex) as boolean;

				const response = await clickhouseRequest({
					credentials,
					sql,
					databaseOverride,
					timeoutMs,
					compress,
					waitEndOfQuery: true,
				});

				const sanitizedHeaders = sanitizeHeaders(response.headers);
				const queryId = response.headers['x-clickhouse-query-id'] ?? null;
				const body = response.body.trim();
				const summary: IDataObject = {
					queryId,
					headers: sanitizedHeaders,
				};
				if (body) {
					summary.body = body;
				}

				results.push({
					json: summary,
					pairedItem: { item: itemIndex },
				});
				continue;
			}

			results.push({
				json: {
					resource,
					operation,
					stub: true,
				},
				pairedItem: { item: itemIndex },
			});
		}

		return [results];
	}
}

type QueryExecutionOptions = {
	credentials: ClickHouseCredentials;
	sql: string;
	limit: number;
	paginate: boolean;
	databaseOverride?: string;
	timeoutMs: number;
	compress: boolean;
};

type QueryExecutionResult = {
	rows: IDataObject[];
	meta: IDataObject[];
	statistics: IDataObject;
	summary: IDataObject;
};

async function executeQuery(options: QueryExecutionOptions): Promise<QueryExecutionResult> {
	const { credentials, sql, limit, paginate, databaseOverride, timeoutMs, compress } = options;
	const safeLimit = Math.max(0, Math.floor(limit));
	const shouldPaginate = paginate && safeLimit > 0;

	const rows: IDataObject[] = [];
	let meta: IDataObject[] = [];
	let statistics: IDataObject = {};
	let pageCount = 0;

	while (true) {
		const offset = shouldPaginate ? pageCount * safeLimit : 0;
		const pagedSql = buildPaginatedSql(sql, safeLimit, offset);
		const response = await clickhouseRequest({
			credentials,
			sql: pagedSql,
			databaseOverride,
			timeoutMs,
			compress,
			format: 'JSON',
			waitEndOfQuery: true,
		});

		const parsed = parseJsonResponse(response.body, credentials);
		const pageRows = normalizeRows(parsed.data);
		if (pageCount === 0) {
			meta = normalizeMeta(parsed.meta);
		}
		statistics = normalizeStatistics(parsed.statistics);
		rows.push(...pageRows);
		pageCount += 1;

		if (!shouldPaginate || pageRows.length < safeLimit) {
			break;
		}
	}

	const summary: IDataObject = {
		rowCount: rows.length,
		limit: safeLimit,
		pages: pageCount,
		paginated: shouldPaginate,
	};

	return { rows, meta, statistics, summary };
}

function normalizeRows(data: unknown): IDataObject[] {
	if (!Array.isArray(data)) {
		return [];
	}
	return data.filter(isRecord);
}

function isRecord(value: unknown): value is IDataObject {
	return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function parseJsonResponse(body: string, credentials: ClickHouseCredentials): {
	data?: unknown;
	meta?: unknown;
	statistics?: unknown;
} {
	try {
		const parsed = JSON.parse(body) as { data?: unknown; meta?: unknown; statistics?: unknown };
		return parsed ?? {};
	} catch (error) {
		const excerpt = safeExcerpt(body, credentials);
		const message = `Failed to parse ClickHouse JSON response. ${excerpt}`;
		throw new Error(message, { cause: error instanceof Error ? error : undefined });
	}
}

function normalizeMeta(meta: unknown): IDataObject[] {
	if (!Array.isArray(meta)) {
		return [];
	}
	return meta.filter(isRecord);
}

function normalizeStatistics(statistics: unknown): IDataObject {
	return isRecord(statistics) ? statistics : {};
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

function normalizeOptionalString(value: string): string | undefined {
	const trimmed = value.trim();
	return trimmed ? trimmed : undefined;
}

function sanitizeHeaders(headers: Record<string, string>): Record<string, string> {
	const blocked = new Set(['authorization', 'proxy-authorization', 'set-cookie', 'cookie']);
	const sanitized: Record<string, string> = {};
	for (const [key, value] of Object.entries(headers)) {
		if (blocked.has(key.toLowerCase())) continue;
		sanitized[key] = value;
	}
	return sanitized;
}

function normalizeCredentials(credentials: ClickHouseCredentials): ClickHouseCredentials {
	return {
		...credentials,
		protocol: credentials.protocol === 'http' ? 'http' : 'https',
		port: credentials.port ?? 8123,
		tlsIgnoreSsl: Boolean(credentials.tlsIgnoreSsl),
	};
}

type InsertExecutionOptions = {
	credentials: ClickHouseCredentials;
	databaseOverride?: string;
	table: string;
	columns: string[];
	batchSize: number;
	ignoreUnknownFields: boolean;
	gzipRequest: boolean;
	timeoutMs: number;
	compress: boolean;
	rows: IDataObject[];
};

async function executeInsert(options: InsertExecutionOptions): Promise<IDataObject> {
	const {
		credentials,
		databaseOverride,
		table,
		columns,
		batchSize,
		ignoreUnknownFields,
		gzipRequest,
		timeoutMs,
		compress,
		rows,
	} = options;

	if (!rows.length) {
		return {
			inserted: 0,
			batches: 0,
			headers: {},
		};
	}

	const query = buildInsertQuery({
		database: databaseOverride,
		table,
		columns,
	});
	const batches = chunkRows(rows, batchSize);
	let inserted = 0;
	let headers: Record<string, string> = {};

	for (const batch of batches) {
		const body = buildNdjson(batch);
		const response = await clickhouseRequest({
			credentials,
			sql: query,
			databaseOverride,
			compress,
			timeoutMs,
			waitEndOfQuery: true,
			queryInUrl: true,
			body,
			gzipRequest,
			settings: ignoreUnknownFields ? { input_format_skip_unknown_fields: 1 } : undefined,
		});

		inserted += batch.length;
		headers = sanitizeHeaders(response.headers);
	}

	return {
		inserted,
		batches: batches.length,
		headers,
	};
}

function collectRowsFromJsonField(
	items: INodeExecutionData[],
	getPathForItem: (index: number) => string,
): IDataObject[] {
	const rows: IDataObject[] = [];
	for (let index = 0; index < items.length; index++) {
		const path = getPathForItem(index);
		const value = getValueAtPath(items[index].json, path);
		if (!Array.isArray(value)) {
			continue;
		}
		for (const entry of value) {
			if (isRecord(entry)) {
				rows.push(entry);
			}
		}
	}
	return rows;
}

function getValueAtPath(input: IDataObject, path: string): unknown {
	if (!path) return undefined;
	const segments = path.match(/[^.[\]]+/g) ?? [];
	let current: unknown = input;
	for (const segment of segments) {
		if (current && typeof current === 'object' && segment in (current as IDataObject)) {
			current = (current as IDataObject)[segment];
		} else {
			return undefined;
		}
	}
	return current;
}
