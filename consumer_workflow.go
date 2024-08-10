package app

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"go.temporal.io/sdk/activity"
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
	ctx1 := workflow.WithActivityOptions(ctx, ao)

	lastRunTime := time.Time{}

	if workflow.HasLastCompletionResult(ctx) {
		var lastResult CronResult
		if err := workflow.GetLastCompletionResult(ctx, &lastResult); err == nil {
			lastRunTime = lastResult.RunTime
		}
	}
	thisRunTime := workflow.Now(ctx)

	err := workflow.ExecuteActivity(ctx1, ConsumeMessageActivity, lastRunTime, thisRunTime).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Cron job failed.", "Error", err)
		return nil, err
	}

	return &CronResult{RunTime: thisRunTime}, nil
}

func ConsumeMessageActivity(ctx context.Context, lastRunTime, thisRunTime time.Time) error {
	activity.GetLogger(ctx).Info("Consumer message activity running.", "lastRunTime_exclude", lastRunTime, "thisRunTime_include", thisRunTime)

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

	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	timeoutChan := time.After(80 * time.Second)
	consumed := 0
	ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			activity.GetLogger(ctx).Info("Received message", "Key", string(msg.Key), "Value", string(msg.Value))
			consumed++
		case <-signals:
			activity.GetLogger(ctx).Info("Interrupt Consumer Loop. Shutting down...")
			break ConsumerLoop
		case <-timeoutChan:
			activity.GetLogger(ctx).Info("Consumer time limit reached. Shutting down...")
			break ConsumerLoop
		}
	}

	activity.GetLogger(ctx).Info("Consumed", consumed)

	return nil
}

