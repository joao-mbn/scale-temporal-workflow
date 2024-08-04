package main

import (
	"context"
	"log"

	"github.com/pborman/uuid"
	app "github.com/scale-temporal-workflow"

	"go.temporal.io/sdk/client"
)

func main() {
	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	executeCronWorkflow(c, app.ProducerWorkflow, "* * * * *")
	executeCronWorkflow(c, app.ConsumerWorkflow, "*/2 * * * *")
}

func executeCronWorkflow(c client.Client, workflow interface {}, cronSchedule string ) {
	workflowId := "cron_" + uuid.New()
	producerWorkflowOptions := client.StartWorkflowOptions{
		ID:           workflowId,
		TaskQueue:    app.ScaleTemporalWorkflowTaskQueue,
		CronSchedule: cronSchedule,
	}

	we, err := c.ExecuteWorkflow(context.Background(), producerWorkflowOptions, workflow)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())
}