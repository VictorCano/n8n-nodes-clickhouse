# Local Development

## Start services

```sh
docker compose up -d
```

This will start:

- ClickHouse (HTTP) on `localhost:8123`
- ClickHouse (HTTPS, self-signed) on `localhost:8443`
- n8n on `http://localhost:5678`

## Open the n8n UI

Open `http://localhost:5678` in your browser.

The n8n database is stored in `docker/data/n8n` so it persists across container recreations.
ClickHouse data is stored in `docker/data/clickhouse` (HTTP) and `docker/data/clickhouse-tls` (HTTPS).

## Develop with live code

The docker-compose setup mounts this repo into the n8n community nodes folder so updates to `dist/` are picked up automatically.

```sh
npm install
npm run build --watch
```

If you do not want watch mode, run `npm run build` after changes and restart the `n8n` container.

## Example credentials

### ClickHouse (HTTP)

- **Protocol:** `http`
- **Host:** `clickhouse`
- **Port:** `8123`
- **Username:** `default`
- **Password:** `clickhouse`
- **Default Database:** `test`
- **Ignore SSL Issues:** `false`

### ClickHouse (HTTPS, self-signed)

- **Protocol:** `https`
- **Host:** `clickhouse_tls`
- **Port:** `8443`
- **Username:** `default`
- **Password:** `clickhouse`
- **Default Database:** `test`
- **Ignore SSL Issues:** `true`

## E2E workflow tests (local n8n)

The E2E test uses your local docker-compose n8n instance by default (`http://localhost:5678`).
Create an API key in n8n (**Settings → n8n API**) and store it in a local `.env.e2e` file:

```sh
N8N_E2E_URL=http://localhost:5678
N8N_E2E_API_KEY=your_api_key_here
```

Then run:

```sh
node --test test/e2e/workflows.e2e.test.js
```
