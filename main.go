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
	Name   string  `json:"name"`
	Schema string  `json:"schema"`
	Data   [][]any `json:"data"`
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

		// Get table data
		dataRows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s.%s", database, tableName))
		if err != nil {
			return err
		}
		defer dataRows.Close()

		columnTypes := dataRows.ColumnTypes()
		var tableData [][]any
		for dataRows.Next() {
			values := make([]any, len(columnTypes))
			scanArgs := make([]any, len(columnTypes))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := dataRows.Scan(scanArgs...); err != nil {
				return err
			}
			tableData = append(tableData, values)
		}

		dump.Tables = append(dump.Tables, TableSchema{
			Name:   tableName,
			Schema: schema,
			Data:   tableData,
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
				if err := batch.Append(row...); err != nil {
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
