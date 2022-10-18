<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Data Importer

Pravega Data Importer is a collection of Apache Flink applications for importing stream data to Pravega from various sources.

It provides the following Flink jobs:

- **stream-mirroring**: Continuously copy a Pravega stream to another Pravega stream, even on a different Pravega cluster
- **kafka-stream-mirroring**: Continuously copy a Kafka stream to a Pravega stream

Each job uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
They automatically recover from failures and resume where they left off.
They can use parallelism for high-volume streams with multiple segments.

To learn more about Pravega, visit http://pravega.io

## Prerequisites

- Java JDK 8.x.
  On Ubuntu, this can be installed with:
  ```shell script
  sudo apt-get install openjdk-8-jdk
  ```

- A [Pravega](http://pravega.io) installation
- A [Flink](https://flink.apache.org/) installation

- The deployment scripts in this project are designed to work with
  Dell EMC Streaming Data Platform (SDP).
  These Flink tools may also be used in other Flink installations,
  including open-source, although the exact
  deployment methods depend on your environment and are not documented here.

## Stream-Mirroring: Continuously copying a Pravega stream to another Pravega stream

### Overview

This Flink job will continuously copy a Pravega stream to another Pravega stream.
It uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
It automatically recovers from failures and resumes where it left off.
It can use parallelism for high-volume streams with multiple segments.
It marks the associated input and output Pravega streams with stream tag `"mirror"`.

### Run Locally with Flink CLI

First build project:
```shell
./gradlew clean build -x test
```
Make sure you have a Flink cluster running locally.
In the input and output Pravega clusters, make sure the associated stream scopes have already been created.
The `help` command in the Pravega CLI shows how this can be done.
Run using Flink CLI from `pravega-data-importer` directory.
This example takes a Pravega stream running at `localhost:9090` with stream `examples/network` as input, and outputs to
Pravega stream `examples2/network-cloud` running at `localhost:9990`.
```shell
flink run build/libs/pravega-data-importer-1.0-SNAPSHOT.jar \
  --action-type stream-mirroring \
  --input-controller tcp://localhost:9090 \
  --input-stream examples/network \
  --input-startAtTail false \
  --output-stream examples2/network-cloud \
  --output-controller tcp://127.0.0.1:9990
```

## Kafka-Stream-Mirroring: Continuously copying a Pravega stream to another Pravega stream

### Overview

This Flink job will continuously copy a Kafka stream to a Pravega stream.
It uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
It can use parallelism for high-volume streams with multiple segments.
It marks the associated output Pravega stream with stream tag `"kafka-mirror"`.

### Run Locally with Flink CLI

First build project:
```shell
./gradlew clean build -x test
```
Make sure you have a Flink cluster running locally.
In the output Pravega cluster, make sure the associated stream scope has already been created.
The `help` command in the Pravega CLI shows how this can be done.
Run using Flink CLI from `pravega-data-importer` directory.
This example takes a Kafka stream running at `localhost:9092` with topic `test-input` and outputs to
Pravega stream `examples/from-kafka` running at `localhost:9090`.
```shell
flink run build/libs/pravega-data-importer-1.0-SNAPSHOT.jar \
  --action-type kafka-stream-mirroring \
  --input-topic test-input \
  --bootstrap.servers localhost:9092 \
  --output-stream examples/from-kafka \
  --output-controller tcp://127.0.0.1:9090 \
  --isStreamOrdered true
```