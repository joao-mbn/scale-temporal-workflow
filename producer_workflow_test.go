package app

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

func TestProduceMessageActivityIntegration(t *testing.T) {
	err := ClearTopic()
	assert.NoError(t, err)

	err = ProduceMessageActivity(context.Background(), time.Now())
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
		assert.Contains(t, string(msg.Value), "produced message at")
	case <-time.After(1 * time.Second):
		t.Fatal("Did not receive a message in time")
	}
}

func (s *UnitTestSuite) TestProducerWorkflow() {
	testProducerWorkflow := func(ctx workflow.Context) error {
		workflowId := "cron_" + uuid.New()
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: workflowId,
			CronSchedule: "* * * * *",
		})

		cronFuture := workflow.ExecuteChildWorkflow(childCtx, ProducerWorkflow)

		_ = workflow.Sleep(ctx, time.Minute*2)
		s.False(cronFuture.IsReady())
		return nil
	}

	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(testProducerWorkflow)
	env.RegisterWorkflow(ProducerWorkflow)
	env.RegisterWorkflow(ProducerChildWorkflow)
	env.RegisterActivity(ProduceMessageActivity)

	env.OnActivity(ProduceMessageActivity, mock.Anything, mock.Anything).Return(nil).Times(childWorkflowsCount * 2)

	env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
		var thisRunTime time.Time
		err := args.Get(&thisRunTime)
		s.NoError(err)

		err = ProduceMessageActivity(context.Background(), time.Now())
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