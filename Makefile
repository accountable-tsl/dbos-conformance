# Application processes stay on the host because fault injection needs precise
# process control; only durable infrastructure runs in Docker.

export DBOS_DATABASE_URL      ?= postgres://postgres:dbos@localhost:5344/dbos?sslmode=disable
export ACCOUNTABLE_DB_URL     ?= postgres://postgres:accountable@localhost:5434/accountable?sslmode=disable
export AUTHORITY_DB_URL       ?= postgres://postgres:authority@localhost:5444/authority?sslmode=disable
export ACCOUNTABLE_URL        ?= http://localhost:8081
export AUTHORITY_URL          ?= http://localhost:8082
export CALLBACK_URL           ?= http://localhost:8080/callbacks
export KAFKA_BROKERS          ?= localhost:19092
export APP_VERSION            ?= v1

.PHONY: up down nuke build test check check-full worker authority accountable seed psql-dbos psql-accountable psql-authority

up:
	docker compose up -d --wait

down:
	docker compose down

nuke:
	docker compose down -v

build:
	go build -ldflags '-X main.buildVariant=v1' -o bin/worker ./cmd/worker
	go build -ldflags '-X main.buildVariant=v1' -o bin/worker-v1 ./cmd/worker
	go build -ldflags '-X main.buildVariant=v2' -o bin/worker-v2 ./cmd/worker
	go build -o bin/authority ./cmd/authority
	go build -o bin/accountable ./cmd/accountable
	go build -o bin/opsctl ./cmd/opsctl
	go build -o bin/producer ./cmd/producer

test:
	go test ./...
	bash -n scenarios/*.sh

check: up build
	DBOS_LAB_MODE=fast DBOS_LAB_SKIP_BUILD=1 ./scenarios/run-all.sh

check-full: up build
	DBOS_LAB_MODE=full DBOS_LAB_SKIP_BUILD=1 ./scenarios/run-all.sh

worker: build
	./bin/worker

authority: build
	./bin/authority

accountable: build
	./bin/accountable

seed:
	curl -s -XPOST $(ACCOUNTABLE_URL)/seed -d '{"prefix":"F","count":5,"tax_year":2025,"scenario":"ok"}' | jq .

psql-dbos:
	docker compose exec dbos-postgres psql -U postgres dbos

psql-accountable:
	docker compose exec accountable-postgres psql -U postgres accountable

psql-authority:
	docker compose exec authority-postgres psql -U postgres authority
