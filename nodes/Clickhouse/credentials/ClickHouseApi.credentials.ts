import type { ICredentialTestRequest, ICredentialType, INodeProperties, Icon } from 'n8n-workflow';

export class ClickHouseApi implements ICredentialType {
	name = 'ClickHouseApi';
	displayName = 'ClickHouse API';
	icon: Icon = { light: 'file:../clickhouse.svg', dark: 'file:../clickhouse.dark.svg' };
	properties: INodeProperties[] = [
		{
			displayName: 'Protocol',
			name: 'protocol',
			type: 'options',
			options: [
				{ name: 'HTTP', value: 'http' },
				{ name: 'HTTPS', value: 'https' },
			],
			default: 'https',
			description: 'Protocol to use for the ClickHouse HTTP interface',
		},
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: '',
			placeholder: 'clickhouse.example.com',
			description: 'Hostname or IP address of the ClickHouse server',
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 8123,
			description: 'HTTP port exposed by ClickHouse',
		},
		{
			displayName: 'Username',
			name: 'username',
			type: 'string',
			default: '',
			description: 'Username for HTTP authentication',
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Password for HTTP authentication',
		},
		{
			displayName: 'Default Database',
			name: 'defaultDatabase',
			type: 'string',
			default: '',
			description: 'Database to use when none is provided per request',
		},
		{
			displayName: 'Ignore SSL Issues',
			name: 'tlsIgnoreSsl',
			type: 'boolean',
			default: false,
			description: 'Whether to disable SSL certificate verification (HTTPS only)',
			displayOptions: {
				show: {
					protocol: ['https'],
				},
			},
		},
		{
			displayName: 'CA Certificate (PEM)',
			name: 'ca',
			type: 'string',
			typeOptions: {
				password: true,
				rows: 4,
			},
			default: '',
			description: 'CA certificate in PEM format',
			displayOptions: {
				show: {
					protocol: ['https'],
				},
			},
		},
		{
			displayName: 'Client Certificate (PEM)',
			name: 'cert',
			type: 'string',
			typeOptions: {
				password: true,
				rows: 4,
			},
			default: '',
			description: 'Client certificate in PEM format',
			displayOptions: {
				show: {
					protocol: ['https'],
				},
			},
		},
		{
			displayName: 'Client Key (PEM)',
			name: 'key',
			type: 'string',
			typeOptions: {
				password: true,
				rows: 4,
			},
			default: '',
			description: 'Client key in PEM format',
			displayOptions: {
				show: {
					protocol: ['https'],
				},
			},
		},
		{
			displayName: 'Client Key Passphrase',
			name: 'passphrase',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Passphrase for the client key, if needed',
			displayOptions: {
				show: {
					protocol: ['https'],
				},
			},
		},
	];

	test: ICredentialTestRequest = {
		request: {
			method: 'GET',
			url: '={{$credentials.protocol}}://{{$credentials.host}}:{{$credentials.port}}/?query=SELECT%201',
			auth: {
				username: '={{$credentials.username}}',
				password: '={{$credentials.password}}',
			},
			skipSslCertificateValidation: '={{$credentials.tlsIgnoreSsl}}',
			returnFullResponse: true,
		},
	};
}
