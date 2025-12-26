.PHONY: build run fmt lint vet test test-race clean

BINARY_NAME=murakami-server
BIN_DIR=bin

build:
	@echo "Building..."
	@go build -o $(BIN_DIR)/$(BINARY_NAME) cmd/main.go

run: build
	@echo "Running..."
	@./$(BIN_DIR)/$(BINARY_NAME)

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

clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@mkdir -p $(BIN_DIR)
