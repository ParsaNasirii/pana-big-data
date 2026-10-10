package io.github.panabigdata.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class StarRocksConfigTest {

  @Test
  void shouldOverrideCredentialsFromEnvironment() {
    Properties properties = validProperties();

    StarRocksConfig config =
        new StarRocksConfig(
            properties,
            Map.of(
                "STARROCKS_USERNAME", "environment-user",
                "STARROCKS_PASSWORD", "environment-password",
                "METRICS_PORT", "9090"));

    assertThat(config.getUsername()).isEqualTo("environment-user");
    assertThat(config.getPassword()).isEqualTo("environment-password");
    assertThat(config.getMetricsPort()).isEqualTo(9090);
    assertThat(properties.getProperty("starrocks.username")).isEqualTo("test-user");
    assertThat(properties.getProperty("starrocks.password")).isEqualTo("test-password");
  }

  @Test
  void shouldReadValidConfiguration() {
    StarRocksConfig config = new StarRocksConfig(validProperties());

    assertThat(config.getFeHosts()).containsExactly("192.0.2.14");
    assertThat(config.getHttpPort()).isEqualTo(8030);
    assertThat(config.getDatabase()).isEqualTo("panabigdata");
    assertThat(config.getTable()).isEqualTo("netflow_raw");
    assertThat(config.getBatchMaxRecords()).isEqualTo(1000);
    assertThat(config.getConnectTimeoutMillis()).isEqualTo(3000);
    assertThat(config.getRequestTimeoutMillis()).isEqualTo(10000);
    assertThat(config.getMetricsPort()).isEqualTo(8380);
    assertThat(config.getUsername()).isEqualTo("test-user");
    assertThat(config.getPassword()).isEqualTo("test-password");
  }

  @Test
  void shouldReadMultipleFeHosts() {
    Properties properties = validProperties();
    properties.remove("starrocks.fe.host");
    properties.setProperty("starrocks.fe.hosts", "192.0.2.14, 192.0.2.12,192.0.2.13,192.0.2.11");

    StarRocksConfig config = new StarRocksConfig(properties);
    assertThat(config.getFeHosts())
        .containsExactly("192.0.2.14", "192.0.2.12", "192.0.2.13", "192.0.2.11");
  }

  @Test
  void shouldTrimAndDeduplicateFeHosts() {
    Properties properties = validProperties();
    properties.remove("starrocks.fe.host");
    properties.setProperty("starrocks.fe.hosts", " 192.0.2.14 ,192.0.2.12,192.0.2.14,  ");

    StarRocksConfig config = new StarRocksConfig(properties);
    assertThat(config.getFeHosts()).containsExactly("192.0.2.14", "192.0.2.12");
  }

  @Test
  void shouldRejectNullUsername() {
    assertThatThrownBy(
            () -> new StarRocksConfig(validProperties(), Map.of("STARROCKS_PASSWORD", "password")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("STARROCKS_USERNAME");
  }

  @Test
  void shouldRejectBlankUsername() {
    assertThatThrownBy(
            () ->
                new StarRocksConfig(
                    validProperties(),
                    Map.of("STARROCKS_USERNAME", "  ", "STARROCKS_PASSWORD", "password")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("STARROCKS_USERNAME");
  }

  @Test
  void shouldRejectNullPassword() {
    assertThatThrownBy(
            () -> new StarRocksConfig(validProperties(), Map.of("STARROCKS_USERNAME", "user")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("STARROCKS_PASSWORD");
  }

  @Test
  void shouldRejectBlankPassword() {
    assertThatThrownBy(
            () ->
                new StarRocksConfig(
                    validProperties(),
                    Map.of("STARROCKS_USERNAME", "user", "STARROCKS_PASSWORD", "  ")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("STARROCKS_PASSWORD");
  }

  @Test
  void shouldRejectMissingMetricsPort() {
    assertThatThrownBy(
            () ->
                new StarRocksConfig(
                    validProperties(),
                    Map.of("STARROCKS_USERNAME", "user", "STARROCKS_PASSWORD", "password")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("METRICS_PORT");
  }

  @Test
  void shouldRejectNonPositiveMetricsPort() {
    Properties properties = validProperties();
    properties.setProperty("metrics.port", "0");

    assertThatThrownBy(() -> new StarRocksConfig(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("metrics.port");
  }

  @Test
  void shouldRejectInvalidMetricsPort() {
    Properties properties = validProperties();
    properties.setProperty("metrics.port", "65536");

    assertThatThrownBy(() -> new StarRocksConfig(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("metrics.port");
  }

  @Test
  void shouldRejectInvalidHttpPort() {
    Properties properties = validProperties();
    properties.setProperty("starrocks.http.port", "70000");

    assertThatThrownBy(() -> new StarRocksConfig(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("starrocks.http.port must be between 1 and 65535");
  }

  @Test
  void shouldRejectNonPositiveBatchSize() {
    Properties properties = validProperties();
    properties.setProperty("starrocks.batch.max.records", "0");

    assertThatThrownBy(() -> new StarRocksConfig(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("starrocks.batch.max.records must be greater than zero");
  }

  @Test
  void shouldTrimPropertyValuesBeforeParsing() {
    Properties properties = validProperties();
    properties.setProperty("starrocks.http.port", " 8030 ");
    properties.setProperty("starrocks.database", " panabigdata ");
    properties.setProperty("starrocks.table", " netflow_raw ");
    properties.setProperty("starrocks.batch.max.records", " 1000 ");
    properties.setProperty("starrocks.connect.timeout.ms", "3000 ");
    properties.setProperty("starrocks.request.timeout.ms", " 10000");

    StarRocksConfig config = new StarRocksConfig(properties);
    assertThat(config.getHttpPort()).isEqualTo(8030);
    assertThat(config.getDatabase()).isEqualTo("panabigdata");
    assertThat(config.getTable()).isEqualTo("netflow_raw");
    assertThat(config.getBatchMaxRecords()).isEqualTo(1000);
    assertThat(config.getConnectTimeoutMillis()).isEqualTo(3000);
    assertThat(config.getRequestTimeoutMillis()).isEqualTo(10000);
  }

  private static Properties validProperties() {
    Properties properties = new Properties();
    properties.setProperty("starrocks.fe.host", "192.0.2.14");
    properties.setProperty("starrocks.http.port", "8030");
    properties.setProperty("starrocks.database", "panabigdata");
    properties.setProperty("starrocks.table", "netflow_raw");
    properties.setProperty("starrocks.batch.max.records", "1000");
    properties.setProperty("starrocks.connect.timeout.ms", "3000");
    properties.setProperty("starrocks.request.timeout.ms", "10000");
    properties.setProperty("metrics.port", "8380");
    properties.setProperty("starrocks.username", "test-user");
    properties.setProperty("starrocks.password", "test-password");
    return properties;
  }
}
