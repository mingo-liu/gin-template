.PHONY: run test check infra topic
run:
	go run ./cmd/server
test:
	go test -race ./...
check:
	go vet ./...
	@test -z "$$(gofmt -l cmd internal web)"
infra:
	docker compose -f deploy/docker-compose.yml up -d
topic:
	docker compose -f deploy/docker-compose.yml exec broker sh mqadmin updateTopic -n namesrv:9876 -b 127.0.0.1:10911 -t like-events
