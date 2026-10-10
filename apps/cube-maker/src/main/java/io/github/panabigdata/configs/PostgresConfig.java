package io.github.panabigdata.configs;

import io.github.panabigdata.ConfigSupport;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;

@Getter
public class PostgresConfig {

  private static final String POSTGRES_URL_ENV = "POSTGRES_URL";
  private static final String POSTGRES_USERNAME_ENV = "POSTGRES_USERNAME";
  private static final String POSTGRES_PASSWORD_ENV = "POSTGRES_PASSWORD";
  public static final String POSTGRES_CONNECTION_RETRY_DELAY_MS =
      "POSTGRES_CONNECTION_RETRY_DELAY_MS";
  public static final String POSTGRES_CONNECTION_RETRY_COUNT = "POSTGRES_CONNECTION_RETRY_COUNT";

  private final String url;
  private final String username;
  private final String password;
  private final int connectionRetryCount;
  private final long connectionRetryDelayMs;

  public PostgresConfig() {
    this(System.getenv());
  }

  PostgresConfig(Map<String, String> environment) {
    this(runtimeProperties(environment));
  }

  PostgresConfig(Properties properties) {
    this(
        ConfigSupport.requiredProperty(properties, "postgres.url"),
        ConfigSupport.requiredProperty(properties, "postgres.username"),
        ConfigSupport.requiredProperty(properties, "postgres.password"),
        ConfigSupport.requiredInteger(properties, "postgres.connection.retry.count"),
        ConfigSupport.nonNegativeLong(properties, "postgres.connection.retry.delay.ms"));
  }

  public PostgresConfig(
      String url,
      String username,
      String password,
      int connectionRetryCount,
      long connectionRetryDelayMs) {
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException("url must not be empty");
    }

    if (username == null || username.isBlank()) {
      throw new IllegalArgumentException("username must not be empty");
    }

    if (password == null || password.isBlank()) {
      throw new IllegalArgumentException("password must not be empty");
    }
    if (connectionRetryCount < 0) {
      throw new IllegalArgumentException("postgres.connection.retry.count must not be negative");
    }
    if (connectionRetryDelayMs < 0) {
      throw new IllegalArgumentException("postgres.connection.retry.delay.ms must not be negative");
    }

    this.url = url;
    this.username = username;
    this.password = password;
    this.connectionRetryCount = connectionRetryCount;
    this.connectionRetryDelayMs = connectionRetryDelayMs;
  }

  private static Properties runtimeProperties(Map<String, String> environment) {
    Properties runtime = new Properties();
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "postgres.url", environment, POSTGRES_URL_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "postgres.username", environment, POSTGRES_USERNAME_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "postgres.password", environment, POSTGRES_PASSWORD_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "postgres.connection.retry.count", environment, POSTGRES_CONNECTION_RETRY_COUNT);
    ConfigSupport.overrideRequiredEnvironment(
        runtime,
        "postgres.connection.retry.delay.ms",
        environment,
        POSTGRES_CONNECTION_RETRY_DELAY_MS);
    return runtime;
  }
}
