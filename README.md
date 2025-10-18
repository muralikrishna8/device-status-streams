## Device Status Streams — Delayed OFFLINE Finalization

Overview
- Purpose: Process device online/offline status events using Kafka Streams and emit an OFFLINE_FINALIZED event only if a device remains OFFLINE for ~65 seconds. If the device comes ONLINE within that window, the pending OFFLINE_FINALIZED is canceled.
- Components:
  - REST API to publish DeviceEvent messages to an input Kafka topic.
  - Kafka Streams topology with a persistent state store that tracks OFFLINE timers and a wall-clock punctuation every 60s to finalize overdue OFFLINE statuses.
  - Output Kafka topic where OFFLINE_FINALIZED events are written.

Core behavior
- Input event: { deviceId, status = ONLINE|OFFLINE, timestamp }
- On OFFLINE: start/refresh a timer (now + 65,000 ms) in the state store.
- On ONLINE: clear any existing timer for that device to cancel finalization.
- Periodic punctuation (every 60s):
  - For each device with an expired timer, emit { deviceId, status: "OFFLINE_FINALIZED", timestamp: now } to the output topic and remove the timer.

Project stack
- Java 21, Spring Boot 3.5.x
- Spring for Apache Kafka and Spring Cloud Stream (Kafka Streams binder)
- Apache Kafka Streams

Kafka topics (defaults)
- Input topic: device-status-input
- Output topic: device-status-output
- You can change these via application.yml or environment variables (see Configuration section).

Prerequisites
- Java 21 (verify with: java -version)
- Maven 3.9+
- A running Apache Kafka broker accessible at localhost:9092
  - You can run Kafka locally via Docker (example below) or a native installation.
- Kafka CLI tools (kafka-topics, kafka-console-producer, kafka-console-consumer), or equivalent UI.

Quick start
1) Clone and build
- mvn -v  # ensure Maven is installed
- mvn clean package

2) Configure application.yml if needed
- Default bootstrap servers: localhost:9092
- Default topics: device-status-input (input), device-status-output (output)
- Application ID: device-status-processor

3) Create topics (if auto-creation is disabled)
- kafka-topics --bootstrap-server localhost:9092 --create --topic device-status-input --partitions 3 --replication-factor 1
- kafka-topics --bootstrap-server localhost:9092 --create --topic device-status-output --partitions 3 --replication-factor 1

4) Run the app
- mvn spring-boot:run
  or
- java -jar target/device-status-streams-0.0.1-SNAPSHOT.jar

5) Publish sample events via REST
- Endpoint: POST http://localhost:8080/api/events
- Content-Type: application/json
- Examples:
  - OFFLINE event (starts the 65s timer):
    { "deviceId": "dev-1", "status": "OFFLINE" }
  - ONLINE event (cancels timer if present):
    { "deviceId": "dev-1", "status": "ONLINE" }
  - With explicit timestamp (optional):
    { "deviceId": "dev-1", "status": "OFFLINE", "timestamp": 1730000000000 }

6) Observe results on the output topic
- kafka-console-consumer --bootstrap-server localhost:9092 --topic device-status-output --from-beginning
- After ~65 seconds without an ONLINE for the same deviceId, you should see:
  {"deviceId":"dev-1","status":"OFFLINE_FINALIZED","timestamp":<emitted_time_ms>}

Running Kafka locally with Docker (example)
- docker network create kafka-net || true
- docker run -d --name zookeeper --network kafka-net -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:7.6.1
- docker run -d --name kafka --network kafka-net -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -p 9092:9092 confluentinc/cp-kafka:7.6.1

Configuration
- File: src/main/resources/application.yml
- Relevant keys:
  spring.kafka.bootstrap-servers: localhost:9092
  spring.kafka.producer.key-serializer: org.apache.kafka.common.serialization.StringSerializer
  spring.kafka.producer.value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
  spring.kafka.producer.properties.spring.json.add.type.headers: false
  spring.kafka.streams.application-id: device-status-processor
  spring.kafka.streams.properties:
    default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
    default.value.serde: org.springframework.kafka.support.serializer.JsonSerde
    spring.json.value.default.type: com.example.streams.model.DeviceEvent
    auto.offset.reset: earliest
  cloud.stream.bindings.input.destination: device-status-input
  cloud.stream.bindings.output.destination: device-status-output

- Environment variable overrides (Spring Boot relaxed binding):
  SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  CLOUD_STREAM_BINDINGS_INPUT_DESTINATION=my-input
  CLOUD_STREAM_BINDINGS_OUTPUT_DESTINATION=my-output
  SPRING_KAFKA_STREAMS_APPLICATION_ID=my-app-id

REST API details
- POST /api/events
  - Body: { deviceId: string, status: "ONLINE"|"OFFLINE", timestamp?: number(ms) }
  - Validation: deviceId required; status must be ONLINE or OFFLINE. If timestamp is absent, server sets current time.
  - Response: 202 Accepted on success; 400 Bad Request on validation errors.

Operational notes
- Timer window: 65 seconds from the moment an OFFLINE event is processed; punctuation runs every 60s, so finalization occurs shortly after expiry.
- State store: Persistent key-value store named offline-timer-store.
- Idempotence: Posting multiple OFFLINE events for the same device refreshes the timer to now + 65s.
- ONLINE before expiry cancels the timer—no OFFLINE_FINALIZED will be emitted.

Troubleshooting
- No messages on output topic:
  - Ensure Kafka is reachable at spring.kafka.bootstrap-servers.
  - Confirm topics exist and are correctly named; check application.yml or env overrides.
  - Verify the app log for "Initialized device-status-processor" and timer registration messages.
- Serialization issues:
  - The producer disables type headers (spring.json.add.type.headers=false). The Streams serde is configured with spring.json.value.default.type to com.example.streams.model.DeviceEvent.
- Topic auto-creation:
  - If your broker has auto-creation disabled, create the topics before running the app.
