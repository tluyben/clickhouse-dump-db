package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
)

type TableSchema struct {
	Name    string   `json:"name"`
	Schema  string   `json:"schema"`
	Columns []string `json:"columns"`
	Types   []string `json:"types"`
}

type DatabaseMetadata struct {
	Tables []TableSchema `json:"tables"`
}

func main() {
	// Load .env if it exists
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Fatal("Error loading .env file:", err)
		}
	}

	// Check required environment variables
	required := []string{"CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_HOST"}
	for _, env := range required {
		if os.Getenv(env) == "" {
			log.Fatalf("Required environment variable %s is not set", env)
		}
	}

	// Get port from env or use default
	port := 9000
	if portStr := os.Getenv("CLICKHOUSE_PORT"); portStr != "" {
		var err error
		port, err = strconv.Atoi(portStr)
		if err != nil {
			log.Fatal("Invalid CLICKHOUSE_PORT value:", err)
		}
	}

	// Validate command-line arguments
	if len(os.Args) != 4 {
		log.Fatal("Usage: program [dump|restore] database filepath")
	}

	command := strings.ToLower(os.Args[1])
	database := os.Args[2]
	filePath := os.Args[3]

	// Create ClickHouse connection
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", os.Getenv("CLICKHOUSE_HOST"), port)},
		Auth: clickhouse.Auth{
			Username: os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
		Debug: true,
	})
	if err != nil {
		log.Fatal("Error connecting to ClickHouse:", err)
	}
	defer conn.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	switch command {
	case "dump":
		if err := dumpDatabase(ctx, conn, database, filePath); err != nil {
			log.Fatal("Error dumping database:", err)
		}
	case "restore":
		if err := restoreDatabase(ctx, conn, database, filePath); err != nil {
			log.Fatal("Error restoring database:", err)
		}
	default:
		log.Fatal("Invalid command. Use 'dump' or 'restore'")
	}
}

func databaseExists(ctx context.Context, conn clickhouse.Conn, database string) (bool, error) {
	rows, err := conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var dbName string
	for rows.Next() {
		if err := rows.Scan(&dbName); err != nil {
			return false, err
		}
		if dbName == database {
			return true, nil
		}
	}
	return false, nil
}

func getScanType(typ string) any {
	typeLower := strings.ToLower(typ)
	switch {
	case strings.Contains(typeLower, "int8"):
		var v int8
		return &v
	case strings.Contains(typeLower, "int16"):
		var v int16
		return &v
	case strings.Contains(typeLower, "int32"):
		var v int32
		return &v
	case strings.Contains(typeLower, "int64"):
		var v int64
		return &v
	case strings.Contains(typeLower, "uint8"):
		var v uint8
		return &v
	case strings.Contains(typeLower, "uint16"):
		var v uint16
		return &v
	case strings.Contains(typeLower, "uint32"):
		var v uint32
		return &v
	case strings.Contains(typeLower, "uint64"):
		var v uint64
		return &v
	case strings.Contains(typeLower, "float32"):
		var v float32
		return &v
	case strings.Contains(typeLower, "float64"):
		var v float64
		return &v
	case strings.Contains(typeLower, "decimal"):
		var v float64
		return &v
	case strings.Contains(typeLower, "string"):
		var v string
		return &v
	case strings.Contains(typeLower, "datetime"):
		var v time.Time
		return &v
	case strings.Contains(typeLower, "date"):
		var v time.Time
		return &v
	case strings.Contains(typeLower, "bool"):
		var v bool
		return &v
	default:
		var v any
		return &v
	}
}

func writeRowToFile(file *os.File, row []any) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	return err
}

const (
	batchSize       = 10000 // Number of rows to process in memory at once
	restoreBatchSize = 1000 // Number of rows to batch insert during restore
)

func dumpDatabase(ctx context.Context, conn clickhouse.Conn, database string, filePath string) error {
	exists, err := databaseExists(ctx, conn, database)
	if err != nil {
		return fmt.Errorf("error checking database existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("database %s does not exist", database)
	}

	// Create dump directory
	dumpDir := filePath
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return err
	}

	// Get all tables
	rows, err := conn.Query(ctx, fmt.Sprintf("SHOW TABLES FROM %s", database))
	if err != nil {
		return err
	}
	defer rows.Close()

	metadata := DatabaseMetadata{}
	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return err
		}

		// Get table schema
		var schema string
		err = conn.QueryRow(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, tableName)).Scan(&schema)
		if err != nil {
			return err
		}

		// Get column information
		colRows, err := conn.Query(ctx, fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s'", database, tableName))
		if err != nil {
			return err
		}

		var columns []string
		var types []string
		for colRows.Next() {
			var name, typ string
			if err := colRows.Scan(&name, &typ); err != nil {
				colRows.Close()
				return err
			}
			columns = append(columns, name)
			types = append(types, typ)
		}
		colRows.Close()

		tableSchema := TableSchema{
			Name:    tableName,
			Schema:  schema,
			Columns: columns,
			Types:   types,
		}
		metadata.Tables = append(metadata.Tables, tableSchema)

		// Create table directory
		tableDir := filepath.Join(dumpDir, tableName)
		if err := os.MkdirAll(tableDir, 0755); err != nil {
			return err
		}

		// Write table schema
		schemaFile := filepath.Join(tableDir, "schema.json")
		schemaData, err := json.MarshalIndent(tableSchema, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(schemaFile, schemaData, 0644); err != nil {
			return err
		}

		// Get table data in batches
		dataRows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s.%s", database, tableName))
		if err != nil {
			return err
		}

		var currentBatch int
		var rowCount int
		var currentFile *os.File

		for dataRows.Next() {
			if rowCount%batchSize == 0 {
				// Close previous file if exists
				if currentFile != nil {
					currentFile.Close()
				}

				// Create new file for next batch
				currentBatch = rowCount / batchSize
				dataFile := filepath.Join(tableDir, fmt.Sprintf("data_%d.json", currentBatch))
				currentFile, err = os.Create(dataFile)
				if err != nil {
					dataRows.Close()
					return err
				}
			}

			// Create properly typed scan targets based on column types
			scanArgs := make([]any, len(types))
			for i, typ := range types {
				scanArgs[i] = getScanType(typ)
			}

			if err := dataRows.Scan(scanArgs...); err != nil {
				currentFile.Close()
				dataRows.Close()
				return err
			}

			// Extract values and convert to appropriate format for JSON
			row := make([]any, len(scanArgs))
			for i, arg := range scanArgs {
				switch v := arg.(type) {
				case *time.Time:
					if *v != (time.Time{}) {
						row[i] = v.Format(time.RFC3339)
					} else {
						row[i] = nil
					}
				case *int8:
					row[i] = *v
				case *int16:
					row[i] = *v
				case *int32:
					row[i] = *v
				case *int64:
					row[i] = *v
				case *uint8:
					row[i] = *v
				case *uint16:
					row[i] = *v
				case *uint32:
					row[i] = *v
				case *uint64:
					row[i] = *v
				case *float32:
					row[i] = *v
				case *float64:
					row[i] = *v
				case *string:
					row[i] = *v
				case *bool:
					row[i] = *v
				default:
					if v != nil {
						row[i] = *v.(*any)
					} else {
						row[i] = nil
					}
				}
			}

			if err := writeRowToFile(currentFile, row); err != nil {
				currentFile.Close()
				dataRows.Close()
				return err
			}

			rowCount++
		}

		if currentFile != nil {
			currentFile.Close()
		}
		dataRows.Close()

		log.Printf("Dumped table %s: %d rows", tableName, rowCount)
	}

	// Write database metadata
	metadataFile := filepath.Join(dumpDir, "metadata.json")
	metadataData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metadataFile, metadataData, 0644)
}

func convertValue(val any, typ string) (any, error) {
	if val == nil {
		return nil, nil
	}

	typeLower := strings.ToLower(typ)
	switch {
	case strings.Contains(typeLower, "int8"):
		return int8(val.(float64)), nil
	case strings.Contains(typeLower, "int16"):
		return int16(val.(float64)), nil
	case strings.Contains(typeLower, "int32"):
		return int32(val.(float64)), nil
	case strings.Contains(typeLower, "int64"):
		return int64(val.(float64)), nil
	case strings.Contains(typeLower, "uint8"):
		return uint8(val.(float64)), nil
	case strings.Contains(typeLower, "uint16"):
		return uint16(val.(float64)), nil
	case strings.Contains(typeLower, "uint32"):
		return uint32(val.(float64)), nil
	case strings.Contains(typeLower, "uint64"):
		return uint64(val.(float64)), nil
	case strings.Contains(typeLower, "float32"):
		return float32(val.(float64)), nil
	case strings.Contains(typeLower, "float64"):
		return val.(float64), nil
	case strings.Contains(typeLower, "decimal"):
		return val.(float64), nil
	case strings.Contains(typeLower, "datetime"), strings.Contains(typeLower, "date"):
		if str, ok := val.(string); ok {
			t, err := time.Parse(time.RFC3339, str)
			if err != nil {
				return nil, fmt.Errorf("error parsing time: %w", err)
			}
			return t, nil
		}
		return nil, fmt.Errorf("expected string for datetime, got %T", val)
	case strings.Contains(typeLower, "string"):
		return val.(string), nil
	case strings.Contains(typeLower, "bool"):
		return val.(bool), nil
	default:
		return val, nil
	}
}

func processDataFile(file *os.File, table TableSchema, batch interface{ Append(...any) error }, batchRows *int) error {
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB buffer size

	for scanner.Scan() {
		var row []any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return err
		}

		// Convert values to appropriate types for insertion
		convertedRow := make([]any, len(row))
		for i, val := range row {
			converted, err := convertValue(val, table.Types[i])
			if err != nil {
				return fmt.Errorf("error converting value for column %s: %w", table.Columns[i], err)
			}
			convertedRow[i] = converted
		}

		if err := batch.Append(convertedRow...); err != nil {
			return fmt.Errorf("error appending to batch: %w", err)
		}

		*batchRows++
	}

	return scanner.Err()
}

func restoreDatabase(ctx context.Context, conn clickhouse.Conn, database string, filePath string) error {
	// Read metadata file
	metadataFile := filepath.Join(filePath, "metadata.json")
	metadataData, err := os.ReadFile(metadataFile)
	if err != nil {
		return err
	}

	var metadata DatabaseMetadata
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		return err
	}

	// Check if database exists and drop it
	exists, err := databaseExists(ctx, conn, database)
	if err != nil {
		return err
	}
	if exists {
		if err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", database)); err != nil {
			return err
		}
	}

	// Create database
	if err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", database)); err != nil {
		return err
	}

	// Restore tables
	for _, table := range metadata.Tables {
		// Create table - replace only the database name after "CREATE TABLE"
		parts := strings.SplitN(table.Schema, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid table schema format for table %s", table.Name)
		}
		createSQL := fmt.Sprintf("CREATE TABLE %s.%s", database, parts[1])
		
		if err := conn.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("error creating table %s: %w", table.Name, err)
		}

		tableDir := filepath.Join(filePath, table.Name)
		dataFiles, err := filepath.Glob(filepath.Join(tableDir, "data_*.json"))
		if err != nil {
			return err
		}

		// Prepare insert statement
		placeholders := make([]string, len(table.Columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)",
			database, table.Name, strings.Join(placeholders, ","))

		var totalRows int
		// Process each data file
		for _, dataFile := range dataFiles {
			file, err := os.Open(dataFile)
			if err != nil {
				return err
			}

			batch, err := conn.PrepareBatch(ctx, insertSQL)
			if err != nil {
				file.Close()
				return err
			}

			batchRows := 0
			if err := processDataFile(file, table, batch, &batchRows); err != nil {
				file.Close()
				return fmt.Errorf("error processing data file %s: %w", dataFile, err)
			}

			// Send batch if we have any rows
			if batchRows > 0 {
				if err := batch.Send(); err != nil {
					file.Close()
					return fmt.Errorf("error sending batch: %w", err)
				}
			}

			totalRows += batchRows
			file.Close()
		}

		log.Printf("Restored table %s: processed %d rows", table.Name, totalRows)
	}

	return nil
}
