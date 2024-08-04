package app

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)



func ProducerWorkflow(ctx workflow.Context) error {

	workflow.GetLogger(ctx).Info("Cron workflow started.", "StartTime", workflow.Now(ctx))

	cwo := workflow.ChildWorkflowOptions{
		WorkflowID: "cron_child_workflow",
	}

	ctx = workflow.WithChildOptions(ctx, cwo)

	for i := 0; i < 5; i++ {
		var result interface{}
		err := workflow.ExecuteChildWorkflow(ctx, ProducerChildWorkflow).Get(ctx, &result)

		if err != nil {
			workflow.GetLogger(ctx).Error("Parent execution received child execution failure.", "Error", err)
			return err
		}
	}

	return nil
}

func ProducerChildWorkflow(ctx workflow.Context) (*CronResult, error) {
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

func ProduceMessageActivity(ctx context.Context, lastRunTime, thisRunTime time.Time) error {
	brokers := []string{broker}
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	activity.GetLogger(ctx).Info("Producer message activity running.", "lastRunTime_exclude", lastRunTime, "thisRunTime_include", thisRunTime)

	message := "message: " + lastRunTime.String() + " thisRunTime_include " + thisRunTime.String()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)

	return err
}

