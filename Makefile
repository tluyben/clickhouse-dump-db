BINARY_NAME=clickhouse-dump-db
build:
	go build -o $(BINARY_NAME) main.go
run:
	go run main.go
clean:
	go clean
	rm -f $(BINARY_NAME)
test:
	go test ./...

linux-amd64:
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux-amd64 main.go

deploy-s3: linux-amd64
	source .env && rclone --s3-provider Other \
		--s3-endpoint=$$S3_ENDPOINT \
		--s3-access-key-id=$$S3_ACCESS_KEY \
		--s3-secret-access-key=$$S3_SECRET_KEY \
		moveto ./$(BINARY_NAME)-linux-amd64 :s3:binaries/${BINARY_NAME}
.PHONY: build run clean test linux-amd64 deploy-s3