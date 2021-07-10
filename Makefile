# KAFKA

clean-kafka:
	docker-compose -f containers/dc.kafka.yml down --volume

run-kafka:
	docker-compose -f containers/dc.kafka.yml up -d

logs-kafka:
	docker-compose -f containers/dc.kafka.yml logs -f

# TEST

test: 
	go test ./... -v -race -count=1 -cover -timeout 6m
