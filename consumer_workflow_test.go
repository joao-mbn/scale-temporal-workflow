package app

import (
	"context"
	"fmt"
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

func TestConsumeMessageActivityIntegration(t *testing.T) {
	err := ClearTopic()
	assert.NoError(t, err)

	started := make(chan struct{})
	var messages []string
	var consumeErr error

	duration := 1 * time.Second
	go func() {
			close(started) // Signal that ConsumeMessageActivity has started
			messages, consumeErr = ConsumeMessageActivity(context.Background(), duration)
	}()

	<-started // Wait for the signal that ConsumeMessageActivity has started

	_, err = ProduceOneMessage(t, 1)
	assert.NoError(t, err)

	// Wait for the goroutine to finish
	time.Sleep(duration + 1 * time.Second)

	assert.NoError(t, consumeErr)
	assert.Equal(t, 1, len(messages), "Expected to consume 1 message, but consumed %d", len(messages))
	assert.Contains(t, messages[0], "message: 1")
}

func ProduceOneMessage(t *testing.T, messageId int) (string, error) {
	brokers := []string{broker}
	producer, err := sarama.NewSyncProducer(brokers, nil)
	assert.NoError(t, err)
	defer producer.Close()

	message := fmt.Sprintf("message: %d", messageId)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)
	assert.NoError(t, err)
	return message, err
}

func (s *UnitTestSuite) TestConsumerWorkflow() {
	testConsumerWorkflow := func(ctx workflow.Context) error {
		workflowId := "cron_" + uuid.New()
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: workflowId,
			CronSchedule: "*/2 * * * *",
		})

		cronFuture := workflow.ExecuteChildWorkflow(childCtx, ConsumerWorkflow)

		_ = workflow.Sleep(ctx, time.Minute*4)
		s.False(cronFuture.IsReady())
		return nil
	}

	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(testConsumerWorkflow)
	env.RegisterWorkflow(ConsumerWorkflow)
	env.RegisterWorkflow(ConsumerChildWorkflow)
	env.RegisterActivity(ConsumeMessageActivity)

	activityCallsCount := childWorkflowsCount * 2
	env.OnActivity(ConsumeMessageActivity, mock.Anything, mock.Anything).Return(make([]string, 0), nil).Times(activityCallsCount)

	duration := 1 * time.Second
	allMessages := make([]string, 0)

	numberOfCalls := 0
	producedMessages := make([]string, 2)

	env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {

		started := make(chan struct{})

		go func() {
				close(started) // Signal that ConsumeMessageActivity has started
				messages, err := ConsumeMessageActivity(context.Background(), duration)
				s.NoError(err)

				allMessages = append(allMessages, messages...)
		}()

		<-started // Wait for the signal that ConsumeMessageActivity has started
		numberOfCalls++

		if numberOfCalls == activityCallsCount {
			firstMessage, err := ProduceOneMessage(s.T(), 0)
			s.NoError(err)
			secondMessage, err := ProduceOneMessage(s.T(), 1)
			s.NoError(err)

			producedMessages[0] = firstMessage
			producedMessages[1] = secondMessage
		}
	})

	startTime, err := time.Parse(time.RFC3339, "2024-08-10T01:00:00Z")
	s.NoError(err)

	env.SetStartTime(startTime)

	err = ClearTopic()
	s.NoError(err)

	env.ExecuteWorkflow(testConsumerWorkflow)

	// Wait for the goroutine to finish
	time.Sleep(duration + 1 * time.Second)

	s.True(env.IsWorkflowCompleted())
	env.AssertExpectations(s.T())

	err = env.GetWorkflowError()
	s.NoError(err)

	/* It cannot be expected that every consumer consumed every message, but a reasonable amount of them */
	s.True(activityCallsCount * 2 >= len(allMessages), "Expected %d to be greater or equal than %d", activityCallsCount * 2, len(allMessages))
	s.True(int(float64(activityCallsCount * 2) * 0.5) <= len(allMessages), "Expected to have consumed at least 10%% of the messages, but consumed only %d", len(allMessages))

	for i := 0; i < activityCallsCount - 1; i += 2 {
		s.Equal(producedMessages[0], allMessages[i])
		s.Equal(producedMessages[1], allMessages[i + 1])
	}
}
