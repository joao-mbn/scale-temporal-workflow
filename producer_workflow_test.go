package app

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestProduceMessageActivityIntegration(t *testing.T) {
	err := ClearTopic()
	assert.NoError(t, err)

	err = ProduceMessageActivity(context.Background(), time.Now().Add(-time.Hour), time.Now())
	assert.NoError(t, err)

	ConsumeOneMessage(t)
}

func ConsumeOneMessage(t *testing.T) {
	// Create a new Sarama consumer
	consumer, err := sarama.NewConsumer([]string{broker}, nil)
	assert.NoError(t, err)
	defer consumer.Close()

	// Consume the message
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	assert.NoError(t, err)
	defer partitionConsumer.Close()

	select {
	case msg := <-partitionConsumer.Messages():
		assert.Contains(t, string(msg.Value), "thisRunTime_include")
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive a message in time")
	}
}

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (s *UnitTestSuite) TestProducerWorkflow() {
	testProducerWorkflow := func(ctx workflow.Context) error {
		workflowId := "cron_" + uuid.New()
		ctx1 := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: workflowId,
			CronSchedule: "* * * * *",
		})

		cronFuture := workflow.ExecuteChildWorkflow(ctx1, ProducerWorkflow)

		_ = workflow.Sleep(ctx, time.Minute*2)
		s.False(cronFuture.IsReady())
		return nil
	}

	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(testProducerWorkflow)
	env.RegisterWorkflow(ProducerWorkflow)
	env.RegisterWorkflow(ProducerChildWorkflow)
	env.RegisterActivity(ProduceMessageActivity)

	env.OnActivity(ProduceMessageActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(childWorkflowsCount * 2)

	env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
		var startTime, endTime time.Time
		err := args.Get(&startTime, &endTime)
		s.NoError(err)

		err = ProduceMessageActivity(context.Background(), time.Now().Add(-time.Hour), time.Now())
		s.NoError(err)
	})

	startTime, err := time.Parse(time.RFC3339, "2024-08-10T01:00:00Z")
	s.NoError(err)

	err = ClearTopic()
	s.NoError(err)

	env.SetStartTime(startTime)

	env.ExecuteWorkflow(testProducerWorkflow)

	s.True(env.IsWorkflowCompleted())

	err = env.GetWorkflowError()
	s.NoError(err)

	env.AssertExpectations(s.T())

	ConsumeManyMessages(s, childWorkflowsCount * 2)
}

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

func ConsumeManyMessages(s *UnitTestSuite, messagesCount int)  {
	// Create a new Sarama consumer
	consumer, err := sarama.NewConsumer([]string{broker}, nil)
	s.NoError(err)
	defer consumer.Close()

	// Consume the message
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	s.NoError(err)
	defer partitionConsumer.Close()

	consumed := 0
	ConsumerLoop:
	for {
		select {
		case <-partitionConsumer.Messages():
			consumed++
		case <-time.After(1 * time.Second):
			break ConsumerLoop
		}
	}

	s.Equal(messagesCount, consumed, "Expected to consume %d messages, but consumed %d", messagesCount, consumed)
}