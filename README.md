# kafka-do

<div align="center">
	<div align="right">
		<strong><code>v0.3.4</code></strong>
	</div>
	<img height="100px" src="doc/seo.do.png"><br>
	<strong>kafka-do</strong>
</div>

[![Go Reference](https://pkg.go.dev/badge/github.com/teamseodo/kafka-do.svg)](https://pkg.go.dev/github.com/teamseodo/kafka-do)

## What

Higher level abstraction for franz-go. 

## Why

We want to be able to write our kafka applications without making the same things over and over.

**Batch Consume**  
Consume messages as much as you defined.

**Batch Produce**  
Produce messages as a batch to a topic.


## Example

For e2e example, check [**here**](https://github.com/teamseodo/kafka-do-example).

```go
	producer, err := kafka.NewProducer("127.0.0.1:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	producer.Produce(context.Background(), []kafka.Message{
		kafka.Message("message 1"),
		kafka.Message("message 2"),
		kafka.Message("message 3"),
		kafka.Message("message 4"),
	}, "messages")

	consumer, err := kafka.NewConsumer("kafka_do", []string{"messages"}, []string{"127.0.0.1:9092"})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	messages, errs := consumer.ConsumeBatch(context.Background(), 2)
	for _, message := range messages {
		log.Println(message)
	}

	for _, err := range errs {
		log.Println(err)
	}
```

## Development

To run tests, start a kafka that runs on ":9092".  
```sh
go test ./... -v -cover -count=1 -race
```
