package com.nuvei.emailtests.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/** Request/response DTO of the email API. Null fields are omitted so negative tests can drop them. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Email {
    public String id;
    public String to;
    public String subject;
    public String body;
    public String status;

    public Email() {}

    public Email(String to, String subject, String body) {
        this.to = to;
        this.subject = subject;
        this.body = body;
    }

    public Email withId(String id)           { this.id = id; return this; }
    public Email withTo(String to)           { this.to = to; return this; }
    public Email withSubject(String subject) { this.subject = subject; return this; }
    public Email withBody(String body)       { this.body = body; return this; }
}
