import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';

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

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			const resource = this.getNodeParameter('resource', itemIndex) as string;
			const operation = this.getNodeParameter('operation', itemIndex) as string;
			const item = items[itemIndex];

			results.push({
				json: {
					...item.json,
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
