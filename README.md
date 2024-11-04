# clickhouse-dump-db

A tool to dump and restore ClickHouse databases with efficient streaming and batched processing.

## Features

- Streaming data processing with configurable batch sizes
- Efficient memory usage for large databases
- Automatic schema preservation
- Support for all ClickHouse data types
- Organized dump structure with metadata

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
./clickhouse-dump-db dump database_name output_directory

# Restore a database
./clickhouse-dump-db restore database_name input_directory
```

## Dump Format

The tool creates a directory structure for the dump with the following layout:

```
output_directory/
├── metadata.json           # Database-level metadata
├── table1/                # Directory for each table
│   ├── schema.json        # Table schema and column information
│   ├── data_0.json       # Data file for first batch
│   ├── data_1.json       # Data file for second batch
│   └── ...               # Additional data files
├── table2/
│   ├── schema.json
│   ├── data_0.json
│   └── ...
└── ...
```

### Metadata File
The `metadata.json` file contains information about all tables in the database, including their schemas and column definitions.

### Schema Files
Each table directory contains a `schema.json` file that includes:
- Table name
- CREATE TABLE statement
- Column names and types

### Data Files
Data is split into multiple files, each containing a batch of rows. This approach:
- Reduces memory usage during dump and restore
- Enables efficient streaming of large datasets
- Allows for parallel processing

## Performance Considerations

The tool uses batched processing for both dump and restore operations:
- Dump batch size: 10,000 rows per file
- Restore batch size: 1,000 rows per insert operation

These batch sizes are optimized for:
- Memory efficiency
- Processing speed
- Network performance

## Data Type Support

The tool automatically handles conversion for all ClickHouse data types:
- Integers (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64)
- Floating point (Float32, Float64)
- Decimal
- String
- DateTime
- Date
- Boolean
- Arrays and other complex types

## Error Handling

- Automatically creates the target database if it doesn't exist
- Drops and recreates the database during restore to ensure clean state
- Validates schema compatibility
- Provides detailed error messages for troubleshooting

## Building from Source

```bash
make build
```

This will create the `clickhouse-dump-db` binary in the current directory.
