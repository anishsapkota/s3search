.PHONY: build test up down rebuild seed query clean tidy

BINARY := s3search
CMD     := ./cmd/s3search
IMAGE   := s3-search-s3search

build:
	go build -o $(BINARY) $(CMD)

tidy:
	go mod tidy

test:
	go test ./...

test-e2e:
	go test -tags=e2e ./e2e/...

up:
	docker compose up -d

# Stop containers, remove the s3search image, rebuild from scratch, bring up.
rebuild:
	docker compose down --remove-orphans
	docker image rm -f $(IMAGE) 2>/dev/null || true
	docker compose build --no-cache s3search

down:
	docker compose down --remove-orphans

logs:
	docker compose logs -f s3search

seed: build
	@echo "Creating index 'logs'..."
	curl -sf -X PUT http://localhost:7700/index/logs \
		-H "Content-Type: application/json" \
		-d @examples/schema-logs.json
	@echo "\nIngesting sample docs..."
	curl -sf -X POST http://localhost:7700/index/logs/docs \
		-H "Content-Type: application/x-ndjson" \
		--data-binary @examples/sample-logs.ndjson
	@echo "\nDone."

query:
	curl -sf -X POST http://localhost:7700/search/logs \
		-H "Content-Type: application/json" \
		-d '{"query":"error","size":5}' | python3 -m json.tool

generate:
	go run ./tools/generate -n 100000 -o edifact.ndjson
	@echo "generated edifact.ndjson (100k messages)"

clean:
	rm -f $(BINARY) edifact.ndjson
	go clean -testcache
