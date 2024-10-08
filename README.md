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

## Tests

### Run tests

~/scale-temporal-workflow

```sh
go test
```

### Purge Kafka Test Topics

~/path-to-kafka

```sh
bin/kafka-topics.sh --delete --topic scale-temporal-workflow --bootstrap-server localhost:9092 --if-exists && bin/kafka-topics.sh --create --topic scale-temporal-workflow --bootstrap-server localhost:9092 --if-not-exists
```

**The tests will already handle reseting the topic. You don't have to do this manually.**

### See existing messages on topic

```sh
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic scale-temporal-workflow --from-beginning
```

**Exiting the process will tell you how many messages you have consumed.**
