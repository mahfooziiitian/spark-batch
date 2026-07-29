# Docker Infrastructure

Build and run tests inside a containerized Spark environment.

## Build

```bash
docker compose -f infra/docker-compose.yml build
```

## Run Tests

```bash
docker compose -f infra/docker-compose.yml run --rm test
```