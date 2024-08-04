package app

import (
	"context"
	"log"
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
		WorkflowID: "cron_child_workflow",
	}

	ctx = workflow.WithChildOptions(ctx, cwo)

	for i := 0; i < 5; i++ {
		var result interface{}
		err := workflow.ExecuteChildWorkflow(ctx, ConsumerChildWorkflow).Get(ctx, &result)

		if err != nil {
			workflow.GetLogger(ctx).Error("Parent execution received child execution failure.", "Error", err)
			return err
		}
	}

	return nil
}

func ConsumerChildWorkflow(ctx workflow.Context) (*CronResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
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

	err := workflow.ExecuteActivity(ctx1, ProduceMessageActivity, lastRunTime, thisRunTime).Get(ctx, nil)
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

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
	ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)

	return nil
}

