package main

import (
	"log"

	app "github.com/scale-temporal-workflow"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
    // Create the client object just once per process
    c, err := client.Dial(client.Options{})
    if err != nil {
        log.Fatalln("unable to create Temporal client", err)
    }
    defer c.Close()

    // This worker hosts both Workflow and Activity functions
    w := worker.New(c, app.ScaleTemporalWorkflowTaskQueue, worker.Options{})

    w.RegisterWorkflow(app.ProducerWorkflow)
		w.RegisterWorkflow(app.ProducerChildWorkflow)
    w.RegisterActivity(app.ProduceMessageActivity)

		w.RegisterWorkflow(app.ConsumerWorkflow)
		w.RegisterWorkflow(app.ConsumerChildWorkflow)
		w.RegisterActivity(app.ConsumeMessageActivity)

    // Start listening to the Task Queue
    err = w.Run(worker.InterruptCh())
    if err != nil {
        log.Fatalln("unable to start Worker", err)
    }
}