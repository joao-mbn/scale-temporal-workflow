# Scale Temporal Workflow

A service that leverages Apache Kafka and Temporal to produce and consume messages. The service includes two Temporal tasks: one for producing messages, triggered every minute, and one for consuming messages, triggered every two minutes.

## Start Kafka Server

[Kafka Quickstart](https://kafka.apache.org/quickstart)

~/path-to-kafka

```sh
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

## Create Topic

~/path-to-kafka

```sh
bin/kafka-topics.sh --create --topic scale-temporal-workflow --bootstrap-server localhost:9092
```

## Create Temporal Server

[Set up Temporal locally](https://learn.temporal.io/getting_started/go/dev_environment/)

~/scale-temporal-workflow

```sh
temporal server start-dev
```

## Create Worker

~/scale-temporal-workflow

```sh
go run worker/main.go
```

## Execute Workflows

~/scale-temporal-workflow

```sh
go run start/main.go
```

## TODO

- Write tests for the workflows
- Improve teardown of ConsumerActivity
- Improve workflow cancellation method
