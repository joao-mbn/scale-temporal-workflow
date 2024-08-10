package app

import "time"

const ScaleTemporalWorkflowTaskQueue = "SCALE_TEMPORAL_WORKFLOW_TASK_QUEUE"
const broker = "localhost:9092"
const topic = "scale-temporal-workflow"
const childWorkflowsCount = 200

type CronResult struct {
	RunTime time.Time
}