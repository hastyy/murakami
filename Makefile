.PHONY: build build-server build-client run run-client fmt lint vet test test-race benchmark clean

SERVER_BINARY=murakami-server
CLIENT_BINARY=murakami-client
BIN_DIR=bin

build: build-server build-client

build-server:
	@echo "Building server..."
	@go build -o $(BIN_DIR)/$(SERVER_BINARY) cmd/server/main.go

build-client:
	@echo "Building client..."
	@go build -o $(BIN_DIR)/$(CLIENT_BINARY) cmd/client/main.go

run: build-server
	@echo "Running server..."
	@./$(BIN_DIR)/$(SERVER_BINARY)

run-client: build-client
	@echo "Running client..."
	@./$(BIN_DIR)/$(CLIENT_BINARY)

fmt:
	@echo "Formatting..."
	@go fmt ./...

lint:
	@echo "Linting..."
	@golangci-lint run

vet:
	@echo "Vetting..."
	@go vet ./...

test:
	@echo "Testing..."
	@go test -v ./...

test-race:
	@echo "Testing with race detector..."
	@go test -race -v ./...

benchmark:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@mkdir -p $(BIN_DIR)
