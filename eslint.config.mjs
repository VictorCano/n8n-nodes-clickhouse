import { config as n8nConfig } from '@n8n/node-cli/eslint';
import tseslint from '@typescript-eslint/eslint-plugin';
import tsparser from '@typescript-eslint/parser';

const baseConfig = Array.isArray(n8nConfig) ? n8nConfig : [n8nConfig];

export default [
	...baseConfig,
	{
		files: ['**/*.ts'],
		ignores: ['dist/**'],
		languageOptions: {
			parser: tsparser,
			parserOptions: {
				sourceType: 'module',
			},
		},
		plugins: {
			'@typescript-eslint': tseslint,
		},
		rules: {
			...tseslint.configs.recommended.rules,
		},
	},
];
