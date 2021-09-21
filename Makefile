# REDPANDA

clean-redpanda:
	docker-compose -f containers/dc.dev.yml down --volume

run-redpanda:
	docker-compose -f containers/dc.dev.yml up -d

logs-redpanda:
	docker-compose -f containers/dc.dev.yml logs -f

# TEST

test: 
	go test ./... -v -race -count=1 -cover -timeout 6m
