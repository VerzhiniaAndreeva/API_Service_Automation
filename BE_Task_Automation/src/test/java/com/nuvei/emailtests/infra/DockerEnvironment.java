package com.nuvei.emailtests.infra;

import com.nuvei.emailtests.config.TestConfig;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;

/**
 * Starts the docker-compose environment once per JVM (lazy singleton) and stops it on JVM exit.
 * With env.manage=false it does nothing, so the same tests run against an already-deployed environment.
 */
public final class DockerEnvironment {
    private static ComposeContainer compose;

    private DockerEnvironment() {}

    public static synchronized void ensureStarted() {
        if (!TestConfig.manageEnv() || compose != null) return;
        ComposeContainer container = new ComposeContainer(new File(TestConfig.composeFile()))
                .withBuild(true)
                .waitingFor("email-service", Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(5)))
                .waitingFor("kafka", Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(5)));
        container.start();
        compose = container;
        Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
    }
}
