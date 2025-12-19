# n8n-nodes-clickhouse

This is an n8n community node. It lets you use ClickHouse in your n8n workflows.

ClickHouse is an open-source, column-oriented database designed for high-performance analytics on large datasets.

Note: This project uses AI-assisted development.

[Installation (Self-Hosted)](#installation-self-hosted)
[n8n Cloud](#n8n-cloud)
[Credentials](#credentials)
[Operations](#operations)
[Examples](#examples)
[Pagination and Output Options](#pagination-and-output-options)
[TLS Notes (Self-Signed Certificates)](#tls-notes-self-signed-certificates)
[Known Limitations](#known-limitations)
[Example Workflows](#example-workflows)
[Resources](#resources)
[Release Automation](#release-automation)

## Installation (Self-Hosted)

Install from the n8n UI:

1. Go to **Settings > Community Nodes**.
2. Select **Install** and enter `@victorcano/n8n-nodes-clickhouse`.
3. Restart n8n if prompted.

For alternative installation methods, see the
[n8n community nodes installation guide](https://docs.n8n.io/integrations/community-nodes/installation/).

For local development with the provided docker-compose, the repo is mounted into the n8n community nodes folder.
Run `npm run build --watch` while n8n is running so changes in `dist/` are picked up automatically.

## n8n Cloud

n8n Cloud only allows installation of **verified** community nodes from the Cloud panel. If this node is not listed
there, you cannot install it in n8n Cloud.

## Credentials

Create a **ClickHouse API** credential and fill in the following fields.

### Local ClickHouse (HTTP 8123)

- **Protocol:** `http`
- **Host:** `localhost` (or `clickhouse` when using the provided docker-compose)
- **Port:** `8123`
- **Username:** `default`
- **Password:** `clickhouse` (when using the provided docker-compose)
- **Default Database:** `test` (optional)
- **Ignore SSL Issues:** `false`

### ClickHouse Cloud (HTTPS 8443)

- **Protocol:** `https`
- **Host:** your ClickHouse Cloud hostname (for example, `xxxxxx.aws.clickhouse.cloud`)
- **Port:** `8443`
- **Username / Password:** from your ClickHouse Cloud service
- **Default Database:** your target database (optional)
- **Ignore SSL Issues:** `false`

## Operations

Resources and operations:

- **Query**: Execute Query
- **Command**: Execute Command
- **Insert**: Insert Rows (from input items), Insert Rows (from JSON array field)
- **Metadata**: List Databases, List Tables, List Columns

## Examples

### Query

```sql
SELECT * FROM events WHERE event_date >= today() - 7
```

### Command

```sql
CREATE TABLE IF NOT EXISTS events (
  id UInt64,
  name String,
  event_date Date
) ENGINE = MergeTree()
ORDER BY id
```

### Insert

Insert rows from input items into `events` with columns `id,name,event_date`.

```sql
INSERT INTO events (id, name, event_date) FORMAT JSONEachRow
```

### Metadata

```sql
SHOW DATABASES
SHOW TABLES FROM test
DESCRIBE TABLE test.events
```

## Pagination and Output Options

For **Execute Query**:

- **Limit**: default is 50, applied by wrapping your SQL in `SELECT * FROM (<sql>) LIMIT <limit>`.
- **Pagination**: when enabled, the node loops with `OFFSET` and aggregates results.
- **Output Mode**:
  - **Single item (Rows Array)**: one item with `{ rows, meta, statistics, summary }`.
  - **One item per row**: each row becomes its own item.

## TLS Notes (Self-Signed Certificates)

If you use self-signed certificates (local TLS or custom ClickHouse TLS), enable **Ignore SSL Issues** in credentials.

## Known Limitations

- Single statement only (no multi-statement SQL).
- HTTP interface only (no native TCP protocol support).

## Example Workflows

Workflow exports are included in the `/examples` folder:

- `examples/query-basic.json`
- `examples/command-create-table.json`
- `examples/insert-from-items.json`
- `examples/metadata-list-tables.json`

## Resources

* [n8n community nodes documentation](https://docs.n8n.io/integrations/#community-nodes)
* [ClickHouse HTTP interface](https://clickhouse.com/docs/en/interfaces/http)

## Release Automation

Releases are fully automated on merge to `main`. The workflow determines the version bump based on PR labels:

- `Major` → major bump
- `Minor` → minor bump
- `Patch` → patch bump
- No label → patch bump

Required repository secrets:

- `NPM_TOKEN` (npm publish)

The npm token user must be a maintainer for the package name (`@victorcano/n8n-nodes-clickhouse`). If the name is already owned on npm, add the user as a collaborator or rename the package before releasing.

Because this is a scoped package, the token must have access to the `@victorcano` scope and the package publishes as public (`publishConfig.access=public`).

The workflow uses `npm run release` (n8n-node release) and creates tags + GitHub releases.
