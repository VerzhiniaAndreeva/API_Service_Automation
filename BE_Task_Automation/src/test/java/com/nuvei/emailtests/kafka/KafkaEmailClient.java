package com.nuvei.emailtests.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nuvei.emailtests.config.TestConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Kafka test helper: publish messages and wait for events.
 * Each instance uses a unique consumer group and reads the topic from the beginning,
 * so a test can never miss an event emitted before it subscribed. Tests filter by email id,
 * which keeps them independent and safe to run in parallel.
 */
public class KafkaEmailClient implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final List<JsonNode> seen = new ArrayList<>();

    public KafkaEmailClient() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestConfig.kafkaBootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(p);

        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestConfig.kafkaBootstrap());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "tests-" + UUID.randomUUID());
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(c);
        consumer.subscribe(List.of(TestConfig.eventsTopic()));
    }

    public void publishSendRequest(String key, Object payload) {
        try {
            publishRaw(key, MAPPER.writeValueAsString(payload));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void publishRaw(String key, String value) {
        try {
            producer.send(new ProducerRecord<>(TestConfig.requestsTopic(), key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Publish failed", e);
        }
    }

    /** Waits until an event for the given email id and status appears on the events topic. */
    public JsonNode awaitEvent(String emailId, String status) {
        return awaitEvent(n -> emailId.equals(n.path("id").asText()) && status.equals(n.path("status").asText()),
                "id=" + emailId + " status=" + status);
    }

    public JsonNode awaitEvent(Predicate<JsonNode> matcher, String description) {
        Optional<JsonNode> found = Optional.empty();
        long deadline = System.nanoTime() + TestConfig.awaitTimeout().toNanos();
        while (System.nanoTime() < deadline) {
            found = seen.stream().filter(matcher).findFirst();
            if (found.isPresent()) return found.get();
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                try {
                    seen.add(MAPPER.readTree(r.value()));
                } catch (Exception ignored) {
                    // non-JSON noise on the topic is not interesting for tests
                }
            }
        }
        throw new AssertionError("No Kafka event within " + TestConfig.awaitTimeout() + " matching " + description);
    }

    /** Asserts that no matching event arrives within the given window (negative checks). */
    public boolean noEventWithin(Duration window, Predicate<JsonNode> matcher) {
        long deadline = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(250))) {
                try {
                    seen.add(MAPPER.readTree(r.value()));
                } catch (Exception ignored) {
                    // ignore non-JSON
                }
            }
            if (seen.stream().anyMatch(matcher)) return false;
        }
        return true;
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(2));
        consumer.close(Duration.ofSeconds(2));
    }
}
