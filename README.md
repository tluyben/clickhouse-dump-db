# clickhouse-dump-db

A tool to dump and restore ClickHouse databases.

## Environment Variables

Required:
- `CLICKHOUSE_USER`: ClickHouse username
- `CLICKHOUSE_PASSWORD`: ClickHouse password
- `CLICKHOUSE_HOST`: ClickHouse host address

Optional:
- `CLICKHOUSE_PORT`: ClickHouse port (default: 9000)

## Usage

```bash
# Dump a database
./clickhouse-dump-db dump database_name output.json

# Restore a database
./clickhouse-dump-db restore database_name input.json
