# CI

The PR workflow runs on every pull request and validates:

- `npm ci`
- `npm run lint`
- `npm run build`
- `npm run test:unit`
- `npm run test:integration` (uses Docker ClickHouse)
- `npx @n8n/scan-community-package @victorcano/n8n-nodes-clickhouse`

## Local equivalents

```sh
npm run lint
npm run build
npm run test:unit
npm run test:integration
npx @n8n/scan-community-package @victorcano/n8n-nodes-clickhouse
```
