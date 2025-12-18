import type { IDataObject } from 'n8n-workflow';

export type MetadataResult = {
	rows: IDataObject[];
	meta: IDataObject[];
	statistics: IDataObject;
};

export function parseMetadataJson(body: string): MetadataResult {
	const parsed = JSON.parse(body) as {
		data?: unknown;
		meta?: unknown;
		statistics?: unknown;
	};

	const rows = Array.isArray(parsed.data) ? parsed.data.filter(isRecord) : [];
	const meta = Array.isArray(parsed.meta) ? parsed.meta.filter(isRecord) : [];
	const statistics = isRecord(parsed.statistics) ? parsed.statistics : {};

	return { rows, meta, statistics };
}

function isRecord(value: unknown): value is IDataObject {
	return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
