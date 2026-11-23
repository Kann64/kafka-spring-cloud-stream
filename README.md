# kafka-spring-cloud-stream

An event-driven demo built with Spring Cloud Stream and Apache Kafka: a REST endpoint
publishes page-view events, a consumer logs them, a supplier generates a continuous
stream, and a Kafka Streams function counts views per page in 5-second windows.

Coursework project, written while following the tutorial by Pr. Mohamed Youssfi —
reference repository: https://github.com/mohamedYoussfi/event-driven-kafka-spring-cloud-stream

## Stack

| | |
|---|---|
| Java | 21 |
| Spring Boot | 4.2.0 |
| Spring Cloud | 2026.0.0-SNAPSHOT |
| Kafka client / Streams | 4.4.0 |
| Broker (Docker) | Confluent Platform 7.9.0 (Kafka 3.9, ZooKeeper mode) |

## Running it

Start the broker (Docker Desktop must be running first):

```bash
docker compose up -d
```

Then run the application:

```bash
./mvnw spring-boot:run
```

Stop the broker when you're done:

```bash
docker compose down
```

Topics and offsets persist in named Docker volumes across restarts. Use
`docker compose down -v` to wipe them.

## How it fits together

Three functional beans in `PageEventHandler`, all activated through
`spring.cloud.function.definition`:

| Bean | Type | Reads | Writes |
|---|---|---|---|
| `pageEventConsumer` | `Consumer<PageEvent>` | T2 | — (logs to console) |
| `pageEventSupplier` | `Supplier<PageEvent>` | — | T3 (every 200 ms) |
| `kStreamFunction` | `Function<KStream, KStream>` | T3 | T4 (windowed counts) |

`kStreamFunction` filters events over 100 ms, re-keys by page name, then counts per
page in 5-second tumbling windows.

`PageEventController` publishes to an arbitrary topic via `StreamBridge`:

```
GET http://localhost:8080/publish?name=P1&topic=T2
```

## Watching the topics

Raw events on T3:

```bash
docker exec -it bdcc-kafka-broker kafka-console-consumer --bootstrap-server broker:9092 --topic T3
```

Windowed counts on T4 (String key, Long value):

```bash
docker exec -it bdcc-kafka-broker kafka-console-consumer --bootstrap-server broker:9092 --topic T4 --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

Check how many records a topic holds:

```bash
docker exec bdcc-kafka-broker kafka-get-offsets --bootstrap-server broker:9092 --topic T4
```

## Notes

- The compose file uses Confluent 7.9.0 rather than the reference project's 7.3.0 —
  7.9 / Kafka 3.9 is the last release supporting ZooKeeper mode, and sits closer to
  the Kafka 4.4.0 clients that Spring Boot 4.2 pulls in. It also adds healthchecks,
  explicit listeners and named volumes.
- `TimeWindows.of(...)` was removed in Kafka 4.0; this project uses
  `TimeWindows.ofSizeWithNoGrace(...)` instead.
- Changing a Kafka Streams topology while keeping the same `application.id` leaves an
  incompatible state store behind. If the Streams thread dies on startup, delete
  `%TEMP%\kafka-streams\kafka-spring-cloud-stream` along with the
  `*-changelog` and `*-repartition` topics, then restart.
