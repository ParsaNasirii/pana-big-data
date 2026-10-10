package io.github.panabigdata.configs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class PostgresConfigTest {
  private static final String VALID_URL = "jdbc:postgresql://localhost:5432/testdb";
  private static final String VALID_USERNAME = "postgres_user";
  private static final String VALID_PASSWORD = "secure_password";

  @Test
  void shouldCreateConfigWithExplicitParameters() {
    PostgresConfig config = new PostgresConfig(VALID_URL, VALID_USERNAME, VALID_PASSWORD, 4, 1000);

    assertThat(config.getUrl()).isEqualTo(VALID_URL);
    assertThat(config.getUsername()).isEqualTo(VALID_USERNAME);
    assertThat(config.getPassword()).isEqualTo(VALID_PASSWORD);
    assertThat(config.getConnectionRetryCount()).isEqualTo(4);
    assertThat(config.getConnectionRetryDelayMs()).isEqualTo(1000);
  }

  @Test
  void shouldCreateConfigFromEnvironment() {
    PostgresConfig config = new PostgresConfig(validEnvironment());

    assertThat(config.getUrl()).isEqualTo(VALID_URL);
    assertThat(config.getUsername()).isEqualTo(VALID_USERNAME);
    assertThat(config.getPassword()).isEqualTo(VALID_PASSWORD);
    assertThat(config.getConnectionRetryCount()).isEqualTo(4);
    assertThat(config.getConnectionRetryDelayMs()).isEqualTo(1000);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void shouldConfigureConnectionRetries(int retryCount) {
    Map<String, String> environment = new HashMap<>(validEnvironment());
    environment.put("POSTGRES_CONNECTION_RETRY_COUNT", String.valueOf(retryCount));
    environment.put("POSTGRES_CONNECTION_RETRY_DELAY_MS", "250");

    PostgresConfig config = new PostgresConfig(environment);

    assertThat(config.getConnectionRetryCount()).isEqualTo(retryCount);
    assertThat(config.getConnectionRetryDelayMs()).isEqualTo(250);
  }

  @ParameterizedTest
  @ValueSource(strings = {"POSTGRES_CONNECTION_RETRY_COUNT", "POSTGRES_CONNECTION_RETRY_DELAY_MS"})
  void shouldRejectInvalidRetrySettings(String variableName) {
    Map<String, String> environment = new HashMap<>(validEnvironment());
    for (String value : new String[] {"-1", "invalid", "9223372036854775808"}) {
      environment.put(variableName, value);
      assertThatThrownBy(() -> new PostgresConfig(environment))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  ", "\t", "\n"})
  void shouldRejectInvalidUrl(String invalidUrl) {
    assertThatThrownBy(
            () -> new PostgresConfig(invalidUrl, VALID_USERNAME, VALID_PASSWORD, 4, 1000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("url must not be empty");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  ", "\t", "\n"})
  void shouldRejectInvalidUsername(String invalidUsername) {
    assertThatThrownBy(
            () -> new PostgresConfig(VALID_URL, invalidUsername, VALID_PASSWORD, 4, 1000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("username must not be empty");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  ", "\t", "\n"})
  void shouldRejectInvalidPassword(String invalidPassword) {
    assertThatThrownBy(
            () -> new PostgresConfig(VALID_URL, VALID_USERNAME, invalidPassword, 4, 1000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("password must not be empty");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "POSTGRES_URL",
        "POSTGRES_USERNAME",
        "POSTGRES_PASSWORD",
        "POSTGRES_CONNECTION_RETRY_COUNT",
        "POSTGRES_CONNECTION_RETRY_DELAY_MS"
      })
  void shouldRejectMissingEnvironmentVariable(String variableName) {
    Map<String, String> environment = new HashMap<>(validEnvironment());
    environment.remove(variableName);

    assertThatThrownBy(() -> new PostgresConfig(environment))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(variableName);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "POSTGRES_URL",
        "POSTGRES_USERNAME",
        "POSTGRES_PASSWORD",
        "POSTGRES_CONNECTION_RETRY_COUNT",
        "POSTGRES_CONNECTION_RETRY_DELAY_MS"
      })
  void shouldRejectBlankEnvironmentVariable(String variableName) {
    Map<String, String> environment = new HashMap<>(validEnvironment());
    environment.put(variableName, "   ");

    assertThatThrownBy(() -> new PostgresConfig(environment))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(variableName);
  }

  private Map<String, String> validEnvironment() {
    return Map.of(
        "POSTGRES_URL", VALID_URL,
        "POSTGRES_USERNAME", VALID_USERNAME,
        "POSTGRES_PASSWORD", VALID_PASSWORD,
        "POSTGRES_CONNECTION_RETRY_COUNT", "4",
        "POSTGRES_CONNECTION_RETRY_DELAY_MS", "1000");
  }
}
