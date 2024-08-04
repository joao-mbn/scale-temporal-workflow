package app

import (
	"log"
	"sync"
	"time"
)


func main() {
	brokers := []string{"localhost:9092"}
	topic := "hello-world"

	var wg sync.WaitGroup
	wg.Add(1)

	go consume(brokers, topic)

	message := "Hello, Kafka! " + time.Now().Format(time.RFC3339)

	err := produce(brokers, topic, message)
	if err != nil {
		log.Fatalf("failed to produce message: %v", err)
	}
	log.Println("Message produced successfully")

	go func() {
		time.Sleep(15 * time.Second)
		wg.Done()
	}()

	wg.Wait()
}