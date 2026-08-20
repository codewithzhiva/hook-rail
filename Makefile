.PHONY: bootstrap lint test integration demo benchmark verify
bootstrap:
	cd web && npm install
lint:
	test -z "$$(gofmt -l .)"
	go vet ./...
test:
	go test -race ./...
integration: test
demo:
	docker compose up --build
benchmark:
	go test -bench=. -benchmem ./...
verify: lint test
	cd web && npm run build
