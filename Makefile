# REDPANDA

clean-redpanda:
	docker-compose -f containers/dc.redpanda.yml down --volume

run-redpanda:
	docker-compose -f containers/dc.redpanda.yml up -d

logs-redpanda:
	docker-compose -f containers/dc.redpanda.yml logs -f

# TEST

test: 
	go test ./... -v -race -count=1 -cover -timeout 6m
