package com.nuvei.emailtests.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.nuvei.emailtests.api.model.Email;
import com.nuvei.emailtests.base.BaseTest;
import com.nuvei.emailtests.data.EmailFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

/** Tests that cross the REST <-> Kafka boundary. */
@Tag("kafka")
class EmailKafkaFlowTest extends BaseTest {

    @Test
    @Tag("e2e")
    @DisplayName("REST request publishes a QUEUED event to Kafka")
    void apiRequestProducesQueuedEvent() {
        Email email = EmailFactory.valid();
        String id = api.send(email).then().statusCode(202).extract().path("id");

        JsonNode event = kafka.awaitEvent(id, "QUEUED");

        assertThat(event.path("to").asText()).isEqualTo(email.to);
        assertThat(event.path("timestamp").asText()).isNotBlank();
    }

    @Test
    @Tag("e2e")
    @DisplayName("Kafka send request is processed and visible via REST as SENT")
    void kafkaRequestIsProcessed() {
        String id = UUID.randomUUID().toString();
        Email email = EmailFactory.valid().withId(id);

        kafka.publishSendRequest(id, email);

        kafka.awaitEvent(id, "SENT");
        api.get(id).then().statusCode(200).body("status", equalTo("SENT"));
    }

    @Test
    @Tag("e2e")
    @DisplayName("Cancelling via REST publishes a CANCELLED event")
    void cancelProducesEvent() {
        String id = api.send(EmailFactory.valid()).then().statusCode(202).extract().path("id");

        api.cancel(id).then().statusCode(204);

        kafka.awaitEvent(id, "CANCELLED");
    }

    @Test
    @Tag("negative")
    @DisplayName("Invalid Kafka message is ignored and produces no event")
    void invalidKafkaMessageIsDropped() {
        String id = UUID.randomUUID().toString();
        kafka.publishSendRequest(id, EmailFactory.valid().withId(id).withTo("not-an-email"));

        assertThat(kafka.noEventWithin(Duration.ofSeconds(5), n -> id.equals(n.path("id").asText()))).isTrue();
        api.get(id).then().statusCode(404);
    }
}
