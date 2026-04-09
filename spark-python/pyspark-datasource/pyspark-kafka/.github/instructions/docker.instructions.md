---
applyTo: "**/Dockerfile,**/docker-compose*.yml"
---

# Docker and Kafka Cluster Configuration

## Confluent Platform Images

- Use **Confluent** community images for the Kafka cluster:
  - ZooKeeper: `confluentinc/cp-zookeeper:${confluent_version}`
  - Kafka: `confluentinc/cp-kafka:${confluent_version}`
- The `confluent_version` variable is passed via a `.env` file or shell environment — do not hardcode version tags in the compose file.

## ZooKeeper Ensemble (3 nodes)

| Service | Client Port (host) | Server Ports |
|---------|-------------------|--------------|
| `zookeeper-1` | 22181 | 22888:23888 |
| `zookeeper-2` | 32181 | 32888:33888 |
| `zookeeper-3` | 42181 | 42888:43888 |

- Each ZooKeeper node must define `ZOOKEEPER_SERVER_ID` (1, 2, 3) and `ZOOKEEPER_SERVERS` listing all three nodes.
- Use consistent `ZOOKEEPER_TICK_TIME`, `ZOOKEEPER_INIT_LIMIT`, and `ZOOKEEPER_SYNC_LIMIT` across all nodes.

## Kafka Brokers (3 nodes)

| Service | Broker ID | Internal Port | External (host) Port | Hostname |
|---------|-----------|---------------|---------------------|----------|
| `kafka-1` | 1 | 19092 | 19091 | `broker-1` |
| `kafka-2` | 2 | 29092 | 29091 | `broker-2` |
| `kafka-3` | 3 | 39092 | 39091 | `broker-3` |

- Each broker uses dual listeners:
  - `PLAINTEXT` — inter-broker communication (e.g., `broker-1:19092`)
  - `PLAINTEXT_HOST` — host-accessible (e.g., `localhost:19091`)
- Configure `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` and `KAFKA_ADVERTISED_LISTENERS` for both protocols.
- All brokers connect to the full ZooKeeper ensemble: `zookeeper-1:22181,zookeeper-2:32181,zookeeper-3:42181`.
- Brokers depend on all three ZooKeeper services.

## Monitoring Tools

| Service | Image | Host Port |
|---------|-------|-----------|
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 |
| `kafdrop` | `obsidiandynamics/kafdrop` | 9000 |

- Kafka-UI uses `DYNAMIC_CONFIG_ENABLED: true` for runtime broker registration.
- Kafdrop connects via `KAFKA_BROKERCONNECT` using the internal broker ports (e.g., `broker-1:19092,broker-2:29092,broker-3:39092`).

## Best Practices

- Use `hostname` and `container_name` consistently for each service.
- Keep each broker's port scheme consistent: broker N uses `N9092` (internal) and `N9091` (external) pattern.
- Add `depends_on` to ensure brokers start after ZooKeeper nodes and monitoring tools start after brokers.
- Set `restart: "no"` for development monitoring tools (Kafdrop) to avoid restart loops.
- When adding new services, follow the established naming and port conventions.
