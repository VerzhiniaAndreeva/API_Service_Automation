package com.nuvei.emailtests.base;

import com.nuvei.emailtests.api.EmailApiClient;
import com.nuvei.emailtests.infra.DockerEnvironment;
import com.nuvei.emailtests.kafka.KafkaEmailClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseTest {
    protected EmailApiClient api;
    protected KafkaEmailClient kafka;

    @BeforeAll
    static void startEnvironment() {
        DockerEnvironment.ensureStarted();
    }

    @BeforeEach
    void initClients() {
        api = new EmailApiClient();
        kafka = new KafkaEmailClient();
    }

    @AfterEach
    void closeClients() {
        kafka.close();
    }
}
