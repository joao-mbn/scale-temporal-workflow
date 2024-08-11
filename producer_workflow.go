package app

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"go.temporal.io/sdk/workflow"
)

func ProducerWorkflow(ctx workflow.Context) error {
	workflow.GetLogger(ctx).Info("Cron workflow started.", "StartTime", workflow.Now(ctx))

	cwo := workflow.ChildWorkflowOptions{ }

	ctx = workflow.WithChildOptions(ctx, cwo)

	futures := make([]workflow.Future, childWorkflowsCount)

	for i, id := range futures {
		futures[i] = workflow.ExecuteChildWorkflow(ctx, ProducerChildWorkflow, id)
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
			}
		})
	}

	// Wait for either all child workflows to complete or the parent workflow timeout
	for i := 0; i < len(futures); i++ {
		selector.Select(ctx)
	}

	workflow.GetLogger(ctx).Info("Cron workflow finished.", "EndTime", workflow.Now(ctx))

	return nil
}

func ProducerChildWorkflow(ctx workflow.Context) (*CronResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	childCtx := workflow.WithActivityOptions(ctx, ao)

	thisRunTime := workflow.Now(ctx)

	err := workflow.ExecuteActivity(childCtx, ProduceMessageActivity, thisRunTime).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Cron job failed.", "Error", err)
		return nil, err
	}

	return &CronResult{RunTime: thisRunTime}, nil
}

func ProduceMessageActivity(ctx context.Context, thisRunTime time.Time) error {
	brokers := []string{broker}
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	message := "message: produced message at" + thisRunTime.String()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)

	return err
}

