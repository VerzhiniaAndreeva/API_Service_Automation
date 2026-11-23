package com.nuvei.emailtests.config;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

/** Single source of configuration: system property > environment variable > config.properties. */
public final class TestConfig {
    private static final Properties FILE = new Properties();

    static {
        try (InputStream in = TestConfig.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in != null) FILE.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load config.properties", e);
        }
    }

    private TestConfig() {}

    public static String get(String key) {
        String value = System.getProperty(key);
        if (value == null) value = System.getenv(key.replace('.', '_').toUpperCase());
        if (value == null) value = FILE.getProperty(key);
        if (value == null) throw new IllegalStateException("Missing config key: " + key);
        return value;
    }

    public static String baseUrl()          { return get("service.base.url"); }
    public static String apiKey()           { return get("service.api.key"); }
    public static String kafkaBootstrap()   { return get("kafka.bootstrap.servers"); }
    public static String requestsTopic()    { return get("kafka.topic.requests"); }
    public static String eventsTopic()      { return get("kafka.topic.events"); }
    public static boolean manageEnv()       { return Boolean.parseBoolean(get("env.manage")); }
    public static String composeFile()      { return get("env.compose.file"); }
    public static Duration awaitTimeout()   { return Duration.ofSeconds(Long.parseLong(get("await.timeout.seconds"))); }
}
