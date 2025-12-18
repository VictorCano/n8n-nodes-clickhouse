import type { IDataObject } from 'n8n-workflow';

export type QueryOutput = {
	rows: IDataObject[];
	meta: IDataObject[];
	statistics: IDataObject;
	summary: IDataObject;
};

export function buildPaginatedSql(sql: string, limit: number, offset = 0): string {
	const cleanedSql = stripTrailingSemicolon(sql.trim());
	const safeLimit = Math.max(0, Math.floor(limit));
	const safeOffset = Math.max(0, Math.floor(offset));
	const offsetClause = safeOffset > 0 ? ` OFFSET ${safeOffset}` : '';

	return `SELECT * FROM (${cleanedSql}) LIMIT ${safeLimit}${offsetClause}`;
}

export function shapeQueryOutput(input: {
	rows?: IDataObject[];
	meta?: IDataObject[];
	statistics?: IDataObject;
	summary?: IDataObject;
}): QueryOutput {
	const rows = Array.isArray(input.rows) ? input.rows : [];
	return {
		rows,
		meta: Array.isArray(input.meta) ? input.meta : [],
		statistics: input.statistics ?? {},
		summary: input.summary ?? { rowCount: rows.length },
	};
}

function stripTrailingSemicolon(sql: string): string {
	let cleaned = sql;
	while (cleaned.endsWith(';')) {
		cleaned = cleaned.slice(0, -1).trimEnd();
	}
	return cleaned;
}
