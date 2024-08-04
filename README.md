# Scale Temporal Workflow

## Start Kafka Server

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

~/scale-temporal-workflow

```sh
temporal server start-dev
```

- Create Producer Workflow
- Create Producer Child Workflows
- Create Producer Activity

- Create Consumer Workflow
- Create Consumer Child Workflows
- Create Consumer Activity

- Link Parent Workflows to Worker
- Start Workflows on start/main
  - Start Consumer with one minute of delay
