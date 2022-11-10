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

### Run Locally

First build project:
```shell
./gradlew clean build installDist -x test
```
Make sure you have a Flink cluster running locally.
In the input and output Pravega clusters, make sure the associated stream scopes have already been created.
The `help` command in the Pravega CLI shows how this can be done.
Please navigate to this directory once gradle build is complete:
```shell
cd pravega-data-importer/build/install/pravega-data-importer/bin
```
Running this help command in above directory shows the parameters needed to run the script:
```shell
pravega-data-importer --help
```
This example takes a Pravega stream running at `localhost:9090` with stream `source/sourceStream` as input, and outputs to
Pravega stream `destination/destinationStream` running at `localhost:9990`.
```shell
pravega-data-importer \
  --action-type stream-mirroring \
  --input-controller tcp://localhost:9090 \
  --input-stream source/sourceStream \
  --input-startAtTail false \
  --output-stream destination/destinationStream \
  --output-controller tcp://127.0.0.1:9990 \
  --flinkHost localhost \
  --flinkPort 8081
```

## Kafka-Stream-Mirroring: Continuously copying a Kafka stream to another Pravega stream

### Overview

This Flink job will continuously copy a Kafka stream to a Pravega stream.
It uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
It can use parallelism for high-volume streams with multiple segments.
It marks the associated output Pravega stream with stream tag `"kafka-mirror"`.

### Run Locally

First build project:
```shell
./gradlew clean build installDist -x test
```
Make sure you have a Flink cluster running locally.
In the output Pravega cluster, make sure the associated stream scope has already been created.
The `help` command in the Pravega CLI shows how this can be done.
Please navigate to this directory once gradle build is complete:
```shell
cd pravega-data-importer/build/install/pravega-data-importer/bin
```
Running this help command in above directory shows the parameters needed to run the script:
```shell
pravega-data-importer --help
```
This example takes a Kafka stream running at `localhost:9092` with topic `test-input` and outputs to
Pravega stream `destination/from-kafka` running at `localhost:9090`.
```shell
pravega-data-importer \
  --action-type kafka-stream-mirroring \
  --input-topic test-input \
  --bootstrap.servers localhost:9092 \
  --output-stream destination/from-kafka \
  --output-controller tcp://127.0.0.1:9090 \
  --isStreamOrdered true
```