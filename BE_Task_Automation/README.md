# Email Service – API Automation Framework

Java 25 · Maven · JUnit 5 · REST Assured · Kafka clients · Testcontainers (Docker Compose) · Awaitility · AssertJ · Allure

## Run

```bash
mvn test                       # starts docker compose, runs everything, tears it down
mvn test -Dgroups=smoke        # tags: smoke | api | kafka | e2e | negative
mvn test -Denv.manage=false -Dservice.base.url=https://staging -Dkafka.bootstrap.servers=broker:9092   # existing env
mvn allure:serve               # (optional, needs allure-maven plugin / CLI) view report from target/allure-results
```

Requirements: Docker and JDK 25 (latest LTS).

## Layout

```
docker/docker-compose.yml          Kafka (KRaft) + email service (stub – swap for the real image)
docker/email-service-stub/         tiny Node service: REST + Kafka, so the suite runs out of the box
src/test/java/com/nuvei/emailtests
  config/TestConfig                -D > ENV > config.properties
  infra/DockerEnvironment          Testcontainers ComposeContainer, once per JVM
  api/EmailApiClient               REST Assured client (no assertions)
  api/model/Email                  DTO
  kafka/KafkaEmailClient           producer + consumer with await helpers
  data/EmailFactory                valid, unique test data
  base/BaseTest                    lifecycle: env up, clients created/closed
  tests/EmailApiTest               REST contract, validation, auth, negative
  tests/EmailKafkaFlowTest         REST <-> Kafka flows
src/test/resources                 config.properties, JSON schemas
```
