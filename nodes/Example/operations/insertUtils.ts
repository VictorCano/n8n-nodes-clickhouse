import type { IDataObject } from 'n8n-workflow';

export type InsertTarget = {
	database?: string;
	table: string;
	columns?: string[];
};

export function buildInsertQuery(target: InsertTarget): string {
	const tableName = target.table.trim();
	if (!tableName) {
		throw new Error('Table name is required for insert');
	}

	const qualifiedTable = target.database
		? `${target.database.trim()}.${tableName}`
		: tableName;
	const columns = target.columns && target.columns.length > 0 ? ` (${target.columns.join(', ')})` : '';

	return `INSERT INTO ${qualifiedTable}${columns} FORMAT JSONEachRow`;
}

export function buildNdjson(rows: IDataObject[]): string {
	return rows.map((row) => JSON.stringify(row)).join('\n');
}

export function chunkRows(rows: IDataObject[], batchSize: number): IDataObject[][] {
	const safeBatch = Math.max(1, Math.floor(batchSize));
	const batches: IDataObject[][] = [];
	for (let i = 0; i < rows.length; i += safeBatch) {
		batches.push(rows.slice(i, i + safeBatch));
	}
	return batches;
}

export function parseColumns(input: {
	columnsCsv?: string;
	columnsUi?: unknown;
}): string[] {
	const uiColumns = extractColumnsFromUi(input.columnsUi);
	if (uiColumns.length) {
		return uiColumns;
	}
	return splitColumnsCsv(input.columnsCsv ?? '');
}

function splitColumnsCsv(value: string): string[] {
	return value
		.split(',')
		.map((entry) => entry.trim())
		.filter((entry) => entry.length > 0);
}

function extractColumnsFromUi(value: unknown): string[] {
	if (!value || typeof value !== 'object') {
		return [];
	}
	const record = value as Record<string, unknown>;
	const columns = record.columns ?? record.values ?? record.column;
	if (!columns) {
		return [];
	}

	const list = Array.isArray(columns) ? columns : Array.isArray((columns as any).columns) ? (columns as any).columns : [];
	const result: string[] = [];
	for (const entry of list) {
		if (!entry || typeof entry !== 'object') continue;
		const column = (entry as Record<string, unknown>).column;
		if (typeof column === 'string' && column.trim()) {
			result.push(column.trim());
		}
	}
	return result;
}
