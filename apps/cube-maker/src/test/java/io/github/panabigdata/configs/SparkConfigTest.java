package io.github.panabigdata.configs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SparkConfigTest {

  @TempDir Path tempDir;

  private static final String VALID_METRICS_PORT = "8380";
  private static final String VALID_PUSHGATEWAY_ADDRESS = "pushgateway:9091";
  private static final String VALID_APP_NAME = "test_app";
  private static final String VALID_MASTER = "local[*]";
  private static final String VALID_DRIVER_HOST = "127.0.0.1";
  private static final String VALID_DRIVER_PORT = "7078";
  private static final String VALID_DRIVER_BLOCK_MANAGER_PORT = "7079";
  private static final String VALID_HDFS_PATH = "/tmp/parquet";
  private static final String VALID_HDFS_FS = "hdfs://localhost:9000";
  private static final String VALID_HDFS_TIMEZONE = "Asia/Tehran";
  private static final String VALID_HADOOP_NAMESERVICE = "test-cluster";

  @Test
  void shouldCreateConfigFromPropertiesAndEnvironment() {
    SparkConfig config = new SparkConfig(validProperties(), validEnvironment());

    assertThat(config.getAppName()).isEqualTo(VALID_APP_NAME);
    assertThat(config.getMaster()).isEqualTo(VALID_MASTER);
    assertThat(config.getDriverHost()).isEqualTo(VALID_DRIVER_HOST);
    assertThat(config.getDriverPort()).isEqualTo(7078);
    assertThat(config.getDriverBlockManagerPort()).isEqualTo(7079);
    assertThat(config.getPushGatewayAddress()).isEqualTo(VALID_PUSHGATEWAY_ADDRESS);
    assertThat(config.getBaseHdfsPath()).isEqualTo(VALID_HDFS_PATH);
    assertThat(config.getHdfsFsDefault()).isEqualTo(VALID_HDFS_FS);
    assertThat(config.getTimezone()).isEqualTo(VALID_HDFS_TIMEZONE);
    assertThat(config.getDirectorySettlingTime()).isEqualTo(Duration.ofHours(2));
    assertThat(config.getSparkConfiguration())
        .containsEntry("spark.app.name", VALID_APP_NAME)
        .containsEntry("spark.cores.max", "4")
        .containsEntry("spark.executor.cores", "2")
        .containsEntry("spark.executor.memory", "1g")
        .containsEntry("spark.dynamicAllocation.enabled", "false");
    assertThat(config.getHadoopConfiguration())
        .containsEntry("dfs.nameservices", VALID_HADOOP_NAMESERVICE);
  }

  @Test
  void shouldUseConfiguredDirectorySettlingTime() {
    Properties properties = validProperties();
    properties.setProperty("cube.directory.settling.hours", "3");

    assertThat(new SparkConfig(properties, validEnvironment()).getDirectorySettlingTime())
        .isEqualTo(Duration.ofHours(3));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "", "invalid"})
  void shouldRejectInvalidDirectorySettlingTime(String value) {
    Properties properties = validProperties();
    properties.setProperty("cube.directory.settling.hours", value);

    assertThatThrownBy(() -> new SparkConfig(properties, validEnvironment()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldConfigureEventLoggingWhenEnabled() {
    Properties properties = validProperties();
    properties.setProperty("spark.eventLog.enabled", "true");
    String eventLogDir = tempDir.resolve("spark-event-logs").toUri().toString();
    properties.setProperty("spark.eventLog.dir", eventLogDir);

    SparkConfig config = new SparkConfig(properties, validEnvironment());

    assertThat(config.isEventLogEnabled()).isTrue();
    assertThat(config.getEventLogDir()).isEqualTo(eventLogDir);
  }

  @Test
  void shouldAllowMissingEventLogDirectoryWhenDisabled() {
    SparkConfig config = new SparkConfig(validProperties(), validEnvironment());

    assertThat(config.isEventLogEnabled()).isFalse();
    assertThat(config.getEventLogDir()).isNull();
  }

  @Test
  void shouldRejectInvalidEventLogEnabled() {
    Properties properties = validProperties();
    properties.setProperty("spark.eventLog.enabled", "maybe");

    assertThatThrownBy(() -> new SparkConfig(properties, validEnvironment()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("spark.eventLog.enabled");
  }

  @Test
  void shouldRequireEventLogEnabled() {
    Properties properties = validProperties();
    properties.remove("spark.eventLog.enabled");

    assertThatThrownBy(() -> new SparkConfig(properties, validEnvironment()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("spark.eventLog.enabled");
  }

  @Test
  void shouldRequireEventLogDirectoryWhenEnabled() {
    Properties properties = validProperties();
    properties.setProperty("spark.eventLog.enabled", "true");

    assertThatThrownBy(() -> new SparkConfig(properties, validEnvironment()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("spark.eventLog.dir");
  }

  @Test
  void shouldRejectMissingAppName() {
    assertThatThrownBy(() -> new SparkConfig(new Properties(), validEnvironment()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("spark.app.name");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "METRICS_PORT",
        "PUSHGATEWAY_ADDRESS",
        "SPARK_MASTER",
        "SPARK_DRIVER_HOST",
        "SPARK_DRIVER_PORT",
        "SPARK_DRIVER_BLOCK_MANAGER_PORT",
        "PARQUET_BASE_PATH",
        "HDFS_DEFAULT_FS",
        "HDFS_TIMEZONE"
      })
  void shouldRejectMissingRuntimeEnvironmentVariable(String variableName) {
    Map<String, String> environment = new HashMap<>(validEnvironment());
    environment.remove(variableName);

    assertThatThrownBy(() -> new SparkConfig(validProperties(), environment))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(variableName);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "METRICS_PORT",
        "PUSHGATEWAY_ADDRESS",
        "SPARK_MASTER",
        "SPARK_DRIVER_HOST",
        "SPARK_DRIVER_PORT",
        "SPARK_DRIVER_BLOCK_MANAGER_PORT",
        "PARQUET_BASE_PATH",
        "HDFS_DEFAULT_FS",
        "HDFS_TIMEZONE"
      })
  void shouldRejectBlankRuntimeEnvironmentVariable(String variableName) {
    Map<String, String> environment = new HashMap<>(validEnvironment());
    environment.put(variableName, "   ");

    assertThatThrownBy(() -> new SparkConfig(validProperties(), environment))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(variableName);
  }

  private Properties validProperties() {
    Properties properties = new Properties();
    properties.setProperty("spark.app.name", VALID_APP_NAME);
    properties.setProperty("cube.directory.settling.hours", "2");
    properties.setProperty("spark.cores.max", "4");
    properties.setProperty("spark.executor.cores", "2");
    properties.setProperty("spark.executor.memory", "1g");
    properties.setProperty("spark.dynamicAllocation.enabled", "false");
    properties.setProperty("spark.eventLog.enabled", "false");
    return properties;
  }

  private Map<String, String> validEnvironment() {
    return Map.ofEntries(
        Map.entry("METRICS_PORT", VALID_METRICS_PORT),
        Map.entry("PUSHGATEWAY_ADDRESS", VALID_PUSHGATEWAY_ADDRESS),
        Map.entry("SPARK_MASTER", VALID_MASTER),
        Map.entry("SPARK_DRIVER_HOST", VALID_DRIVER_HOST),
        Map.entry("SPARK_DRIVER_PORT", VALID_DRIVER_PORT),
        Map.entry("SPARK_DRIVER_BLOCK_MANAGER_PORT", VALID_DRIVER_BLOCK_MANAGER_PORT),
        Map.entry("PARQUET_BASE_PATH", VALID_HDFS_PATH),
        Map.entry("HDFS_DEFAULT_FS", VALID_HDFS_FS),
        Map.entry("HDFS_TIMEZONE", VALID_HDFS_TIMEZONE),
        Map.entry("SPARK_HADOOP_dfs.nameservices", VALID_HADOOP_NAMESERVICE));
  }
}
