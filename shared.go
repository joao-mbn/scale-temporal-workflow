package app

import "time"

const ScaleTemporalWorkflowTaskQueue = "SCALE_TEMPORAL_WORKFLOW_TASK_QUEUE"
const broker = "localhost:9092"
const topic = "scale-temporal-workflow"

type CronResult struct {
	RunTime time.Time
}