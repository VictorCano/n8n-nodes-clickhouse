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

## Example credentials

### ClickHouse (HTTP)

- **Protocol:** `http`
- **Host:** `clickhouse`
- **Port:** `8123`
- **Username:** `default`
- **Password:** *(empty)*
- **Default Database:** `test`
- **Ignore SSL Issues:** `false`

### ClickHouse (HTTPS, self-signed)

- **Protocol:** `https`
- **Host:** `clickhouse_tls`
- **Port:** `8443`
- **Username:** `default`
- **Password:** *(empty)*
- **Default Database:** `test`
- **Ignore SSL Issues:** `true`
