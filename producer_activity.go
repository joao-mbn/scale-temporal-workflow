package app


import (
	"github.com/IBM/sarama"
)

func produce(brokers []string, topic string, message string) error {
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)

	return err
}


