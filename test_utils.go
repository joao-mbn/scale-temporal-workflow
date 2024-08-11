package app

import (
	"log"

	"github.com/IBM/sarama"
)



func ClearTopic() error {
	// Create a new Sarama client
	config := sarama.NewConfig()
	client, err := sarama.NewClient([]string{broker}, config)
	if err != nil {
			log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create a new Sarama broker
	brokerConn := client.Brokers()[0]
	err = brokerConn.Open(config)
	if err != nil && err != sarama.ErrAlreadyConnected {
			log.Fatalf("Failed to open broker connection: %v", err)
	}

	// Create a DeleteRecordsRequest
	deleteRecordsRequest := &sarama.DeleteRecordsRequest{
			Topics: map[string]*sarama.DeleteRecordsRequestTopic{
					topic: {
							PartitionOffsets: map[int32]int64{
								0: -1,
							},
					},
			},
	}

	// Send the DeleteRecordsRequest
	_, err = brokerConn.DeleteRecords(deleteRecordsRequest)
	return err
}