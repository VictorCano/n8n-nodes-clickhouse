const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const fs = require('node:fs/promises');
const path = require('node:path');

const execFileAsync = promisify(execFile);

async function runTsc() {
	await execFileAsync('tsc', [], { stdio: 'inherit' });
}

async function copyStaticFiles() {
	const roots = ['nodes'];
	for (const root of roots) {
		await copyFromRoot(root);
	}
}

async function copyFromRoot(root) {
	const rootPath = path.join(process.cwd(), root);
	const entries = await fs.readdir(rootPath, { withFileTypes: true });
	for (const entry of entries) {
		await walkEntry(path.join(rootPath, entry.name), entry);
	}
}

async function walkEntry(fullPath, entry) {
	if (entry.isDirectory()) {
		const entries = await fs.readdir(fullPath, { withFileTypes: true });
		for (const child of entries) {
			await walkEntry(path.join(fullPath, child.name), child);
		}
		return;
	}

	if (!entry.isFile()) return;

	const relPath = path.relative(process.cwd(), fullPath);
	const ext = path.extname(entry.name).toLowerCase();
	const isSchemaJson = relPath.includes(`${path.sep}__schema__${path.sep}`) && ext === '.json';
	const isStaticAsset = ext === '.svg' || ext === '.png';

	if (!isSchemaJson && !isStaticAsset) return;

	const destPath = path.join(process.cwd(), 'dist', relPath);
	await fs.mkdir(path.dirname(destPath), { recursive: true });
	await fs.cp(fullPath, destPath, { recursive: true });
}

async function main() {
	await fs.rm(path.join(process.cwd(), 'dist'), { recursive: true, force: true });
	await runTsc();
	await copyStaticFiles();
}

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
