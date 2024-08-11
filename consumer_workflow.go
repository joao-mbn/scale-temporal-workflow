package app

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"go.temporal.io/sdk/workflow"
)

func ConsumerWorkflow(ctx workflow.Context) error {
	workflow.GetLogger(ctx).Info("Cron workflow started.", "StartTime", workflow.Now(ctx))

	cwo := workflow.ChildWorkflowOptions{
		WorkflowExecutionTimeout: 100 * time.Second,
	}

	ctx = workflow.WithChildOptions(ctx, cwo)

	futures := make([]workflow.Future, childWorkflowsCount)

	for i, id := range futures {
		futures[i] = workflow.ExecuteChildWorkflow(ctx, ConsumerChildWorkflow, id)
	}

	// Use a select statement to wait for all child workflows to complete or for the timeout
	selector := workflow.NewSelector(ctx)
	for _, future := range futures {
		f := future
		selector.AddFuture(f, func(f workflow.Future) {
			var result interface{}
			err := f.Get(ctx, &result)
			if err != nil {
				workflow.GetLogger(ctx).Error("Child workflow failed", "Error", err)
			} else {
				workflow.GetLogger(ctx).Info("Child workflow completed", "Result", result)
			}
		})
	}

	// Wait for either all child workflows to complete or the parent workflow timeout
	for i := 0; i < len(futures); i++ {
		selector.Select(ctx)
	}

	return nil
}

func ConsumerChildWorkflow(ctx workflow.Context) (*CronResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 90 * time.Second,
	}
	childCtx := workflow.WithActivityOptions(ctx, ao)

	err := workflow.ExecuteActivity(childCtx, ConsumeMessageActivity, 80 * time.Second).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Cron job failed.", "Error", err)
		return nil, err
	}

	thisRunTime := workflow.Now(ctx)
	return &CronResult{RunTime: thisRunTime}, nil
}

func ConsumeMessageActivity(ctx context.Context, duration time.Duration) ([]string, error) {
	brokers := []string{broker}
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	timeoutChan := time.After(duration)

	messageArray := make([]string, 0)

	ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messageArray = append(messageArray, string(msg.Value))
		case <-signals:
			break ConsumerLoop
		case <-timeoutChan:
			break ConsumerLoop
		}
	}

	return messageArray, nil
}

