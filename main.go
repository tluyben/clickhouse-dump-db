package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	Data    [][]any  `json:"data"`
	Columns []string `json:"columns"`
	Types   []string `json:"types"`
}

type DatabaseDump struct {
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

func dumpDatabase(ctx context.Context, conn clickhouse.Conn, database string, filePath string) error {
	exists, err := databaseExists(ctx, conn, database)
	if err != nil {
		return fmt.Errorf("error checking database existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("database %s does not exist", database)
	}

	// Get all tables
	rows, err := conn.Query(ctx, fmt.Sprintf("SHOW TABLES FROM %s", database))
	if err != nil {
		return err
	}
	defer rows.Close()

	dump := DatabaseDump{}
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
		defer colRows.Close()

		var columns []string
		var types []string
		for colRows.Next() {
			var name, typ string
			if err := colRows.Scan(&name, &typ); err != nil {
				return err
			}
			columns = append(columns, name)
			types = append(types, typ)
		}

		// Get table data
		dataRows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s.%s", database, tableName))
		if err != nil {
			return err
		}
		defer dataRows.Close()

		var tableData [][]any
		for dataRows.Next() {
			// Create properly typed scan targets based on column types
			scanArgs := make([]any, len(types))
			for i, typ := range types {
				scanArgs[i] = getScanType(typ)
			}

			if err := dataRows.Scan(scanArgs...); err != nil {
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
			tableData = append(tableData, row)
		}

		dump.Tables = append(dump.Tables, TableSchema{
			Name:    tableName,
			Schema:  schema,
			Data:    tableData,
			Columns: columns,
			Types:   types,
		})
	}

	// Write to file
	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return ioutil.WriteFile(filePath, data, 0644)
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

func restoreDatabase(ctx context.Context, conn clickhouse.Conn, database string, filePath string) error {
	// Read dump file
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	var dump DatabaseDump
	if err := json.Unmarshal(data, &dump); err != nil {
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
	for _, table := range dump.Tables {
		// Create table
		createSQL := strings.Replace(table.Schema, table.Name, fmt.Sprintf("%s.%s", database, table.Name), 1)
		if err := conn.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("error creating table %s: %w", table.Name, err)
		}

		// Insert data if any exists
		if len(table.Data) > 0 {
			placeholders := make([]string, len(table.Data[0]))
			for i := range placeholders {
				placeholders[i] = "?"
			}
			insertSQL := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)",
				database, table.Name, strings.Join(placeholders, ","))

			batch, err := conn.PrepareBatch(ctx, insertSQL)
			if err != nil {
				return err
			}

			for _, row := range table.Data {
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
					return fmt.Errorf("error inserting data into %s: %w", table.Name, err)
				}
			}

			if err := batch.Send(); err != nil {
				return fmt.Errorf("error sending batch for %s: %w", table.Name, err)
			}
		}
	}

	return nil
}
