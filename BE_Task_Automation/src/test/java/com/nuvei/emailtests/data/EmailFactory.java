package com.nuvei.emailtests.data;

import com.nuvei.emailtests.api.model.Email;

import java.util.UUID;

/** Builders for valid test data; tests override only what they care about. */
public final class EmailFactory {
    private EmailFactory() {}

    public static Email valid() {
        return new Email(uniqueRecipient(), "Subject " + UUID.randomUUID(), "Hello from automation");
    }

    public static String uniqueRecipient() {
        return "user-" + UUID.randomUUID() + "@example.com";
    }
}
