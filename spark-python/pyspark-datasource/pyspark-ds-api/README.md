# Spark Api integration

1. Ingesting data into Apache Spark from APIs – You want to use Spark to pull data from external APIs (like REST APIs) and process it.
2. Building an API with Spark – You’re building a web API using a framework (like Flask or FastAPI), and you want to ingest data into Spark from that API.
3. Using Spark's own APIs for ingestion – You’re asking about Spark’s DataFrame or RDD API to ingest data from different sources (like Kafka, JSON, CSV, JDBC, etc.).
4. Streaming API ingestion – You want to use Spark Structured Streaming or Spark Streaming to ingest data in real time from APIs or message brokers like Kafka.

## Flow

```mermaid
flowchart TD
    subgraph Spark Application
        SPARK["read_api(config.yaml)"]
        SCHEMA[Data Extraction + Schema Inference]
        DF[Create DataFrame]
    end

    SPARK --> AUTHCHECK{Auth Type?}

    AUTHCHECK -->|Basic Auth| BASIC["Add Authorization Header:<br>Base64(user:pass)"]
    AUTHCHECK -->|Bearer Token| BEARER[Add Authorization Header:<br>Bearer token]
    AUTHCHECK -->|"API Key (Header)"| APIHEAD[Add X-API-Key Header]
    AUTHCHECK -->|"API Key (Query)"| APIQUERY[Add ?apikey=value in URL]
    AUTHCHECK -->|mTLS| MTLS[Attach client.crt/client.key<br>Verify with ca.pem]
    AUTHCHECK -->|OAuth2| OAUTH2[Fetch token via token_url<br>Attach Bearer token]

    BASIC --> REQ
    BEARER --> REQ
    APIHEAD --> REQ
    APIQUERY --> REQ
    MTLS --> REQ
    OAUTH2 --> REQ

    REQ["requests.request(...)"] --> RESP["API Response (JSON)"]
    RESP --> SCHEMA
    SCHEMA --> DF

    DF --> END[Use in Spark SQL / Save as Parquet, etc.]
```
