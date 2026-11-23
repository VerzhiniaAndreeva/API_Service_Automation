package com.nuvei.emailtests.tests;

import com.nuvei.emailtests.api.model.Email;
import com.nuvei.emailtests.base.BaseTest;
import com.nuvei.emailtests.data.EmailFactory;
import io.restassured.module.jsv.JsonSchemaValidator;
import io.restassured.response.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

@Tag("api")
class EmailApiTest extends BaseTest {

    @Test
    @Tag("smoke")
    @DisplayName("Service is healthy")
    void healthCheck() {
        assertThat(api.health().statusCode()).isEqualTo(200);
    }

    @Test
    @Tag("smoke")
    @DisplayName("Valid email is accepted and queued")
    void sendValidEmail() {
        Email email = EmailFactory.valid();

        api.send(email).then()
                .statusCode(202)
                .body(JsonSchemaValidator.matchesJsonSchemaInClasspath("schemas/send-email-response.json"))
                .body("status", equalTo("QUEUED"));
    }

    @Test
    @DisplayName("Sent email can be retrieved by id")
    void getEmailById() {
        Email email = EmailFactory.valid();
        String id = api.send(email).then().statusCode(202).extract().path("id");

        api.get(id).then()
                .statusCode(200)
                .body("id", equalTo(id))
                .body("to", equalTo(email.to))
                .body("subject", equalTo(email.subject));
    }

    @Test
    @DisplayName("Queued email can be cancelled")
    void cancelQueuedEmail() {
        String id = api.send(EmailFactory.valid()).then().statusCode(202).extract().path("id");

        api.cancel(id).then().statusCode(204);
        api.get(id).then().statusCode(200).body("status", equalTo("CANCELLED"));
    }

    @Test
    @Tag("negative")
    @DisplayName("Unknown id returns 404")
    void getUnknownEmail() {
        api.get("00000000-0000-0000-0000-000000000000").then().statusCode(404);
    }

    @Test
    @Tag("negative")
    @DisplayName("Missing or wrong API key returns 401")
    void unauthorized() {
        assertThat(api.sendWithApiKey(EmailFactory.valid(), null).statusCode()).isEqualTo(401);
        assertThat(api.sendWithApiKey(EmailFactory.valid(), "wrong").statusCode()).isEqualTo(401);
    }

    @Test
    @Tag("negative")
    @DisplayName("Malformed JSON returns 400")
    void malformedJson() {
        api.sendRaw("{not-json").then().statusCode(400);
    }

    @ParameterizedTest(name = "invalid recipient [{0}] -> 400")
    @Tag("negative")
    @NullAndEmptySource
    @ValueSource(strings = {"plain-text", "missing@tld", "@no-local.com", "spaces in@mail.com"})
    void invalidRecipient(String to) {
        Response r = api.send(EmailFactory.valid().withTo(to));
        r.then().statusCode(400).body("details", hasItem(containsString("to")));
    }

    @ParameterizedTest(name = "blank subject [{0}] -> 400")
    @Tag("negative")
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void blankSubject(String subject) {
        api.send(EmailFactory.valid().withSubject(subject)).then()
                .statusCode(400).body("details", hasItem(containsString("subject")));
    }

    @Test
    @Tag("negative")
    @DisplayName("Subject longer than 200 chars is rejected")
    void subjectTooLong() {
        api.send(EmailFactory.valid().withSubject("a".repeat(201))).then().statusCode(400);
    }
}
