# kafka-do

<div align="center">
	<div align="right">
		<strong><code>v0.1.2</code></strong>
	</div>
	<img height="100px" src="https://github.com/teamseodo/kafka-do/blob/main/doc/seo.do.png"><br>
	<strong>kafka-do</strong>
</div>

[![Go Reference](https://pkg.go.dev/badge/github.com/teamseodo/kafka-do.svg)](https://pkg.go.dev/github.com/teamseodo/kafka-do)

## What

Higher level abstraction for Sarama. 

## Why

We want to be able to write our kafka applications without making the same things over and over.

**Batch Consume**  
Consume messages as much as you defined.

**Chan Consume**  
Consume messages and streams them to a channel.

**Batch Produce**  
Produce messages as a batch to a topic.

**Chan Produce**  
Read from a channel and produce them to a topic.

## Example

```go
package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	do "github.com/teamseodo/kafka-do"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	topicName := "kafka-do-testing"

	producer, err := do.NewProducer([]string{"127.0.0.1:9094"}, 5)
	if err != nil {
		log.Fatalf("error while creating consumer group, error: %s", err)
	}
	defer producer.Close()

	consumer, err := do.NewConsumerGroup([]string{"127.0.0.1:9094"}, topicName)
	if err != nil {
		log.Fatalf("error while creating consumer group, error: %s", err)
	}
	defer consumer.Close()

	messages := [][]byte{ // for testing.
		[]byte("message 1"), []byte("message 2"), []byte("message 3"),
		[]byte("message 1"), []byte("message 2"), []byte("message 3"),
		[]byte("message 1"), []byte("message 2"), []byte("message 3"),
		[]byte("message 1"), []byte("message 2"), []byte("message 3"),
	}

	err = do.ProduceBatch(ctx, producer, messages, topicName) // produce messages as a batch.
	if err != nil {
		log.Fatalf("error while writin to Kafka, error: %s", err)
	}

	outChan := make(chan sarama.ConsumerMessage, 1)
	defer close(outChan)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go do.ConsumeChan(ctx, &wg, consumer, []string{topicName}, outChan) // consume messages as a chan.
	}

out:
	for {
		select {
		case msg := <-outChan:
			fmt.Printf("message: %s, %s", msg.Timestamp, msg.Value)
		case <-time.After(15 * time.Second): // maximum wait time.
			break out
		}
	}

	cancel()
	wg.Wait()
}
```

## TODOs

- [ ] Add an e2e example repo.
- [ ] Add priority usage support to consuming.

## Development

To run tests, start a kafka that runs on ":9094".  
```sh
go test ./... -v -cover -count=1 -race
```
