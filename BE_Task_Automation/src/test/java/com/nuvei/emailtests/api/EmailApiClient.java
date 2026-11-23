package com.nuvei.emailtests.api;

import com.nuvei.emailtests.api.model.Email;
import com.nuvei.emailtests.config.TestConfig;
import io.qameta.allure.restassured.AllureRestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

/**
 * Thin client over the email REST API. Returns raw {@link Response} so tests own the assertions;
 * no assertions live here.
 */
public class EmailApiClient {
    private static final String EMAILS = "/api/v1/emails";

    private final RequestSpecification spec = new RequestSpecBuilder()
            .setBaseUri(TestConfig.baseUrl())
            .setContentType(ContentType.JSON)
            .setAccept(ContentType.JSON)
            .addFilter(new AllureRestAssured())
            .build();

    public Response send(Email email) {
        return io.restassured.RestAssured.given().spec(spec)
                .header("X-API-Key", TestConfig.apiKey())
                .body(email).post(EMAILS);
    }

    public Response sendWithApiKey(Email email, String apiKey) {
        var req = io.restassured.RestAssured.given().spec(spec).body(email);
        if (apiKey != null) req.header("X-API-Key", apiKey);
        return req.post(EMAILS);
    }

    public Response sendRaw(String rawBody) {
        return io.restassured.RestAssured.given().spec(spec)
                .header("X-API-Key", TestConfig.apiKey())
                .body(rawBody).post(EMAILS);
    }

    public Response get(String id) {
        return io.restassured.RestAssured.given().spec(spec).get(EMAILS + "/{id}", id);
    }

    public Response cancel(String id) {
        return io.restassured.RestAssured.given().spec(spec).delete(EMAILS + "/{id}", id);
    }

    public Response health() {
        return io.restassured.RestAssured.given().spec(spec).get("/health");
    }
}
