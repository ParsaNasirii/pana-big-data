package io.github.panabigdata.configs;

import io.github.panabigdata.ConfigSupport;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;

@Getter
public class SparkConfig {

  private static final String METRICS_PORT_ENV = "METRICS_PORT";
  private static final String PUSHGATEWAY_ADDRESS_ENV = "PUSHGATEWAY_ADDRESS";
  private static final String SPARK_MASTER_ENV = "SPARK_MASTER";
  private static final String SPARK_DRIVER_HOST_ENV = "SPARK_DRIVER_HOST";
  private static final String SPARK_DRIVER_PORT_ENV = "SPARK_DRIVER_PORT";
  private static final String SPARK_DRIVER_BLOCK_MANAGER_PORT_ENV =
      "SPARK_DRIVER_BLOCK_MANAGER_PORT";
  private static final String PARQUET_BASE_PATH_ENV = "PARQUET_BASE_PATH";
  private static final String HDFS_DEFAULT_FS_ENV = "HDFS_DEFAULT_FS";
  private static final String HDFS_TIMEZONE_ENV = "HDFS_TIMEZONE";
  private static final String SPARK_HADOOP_ENV_PREFIX = "SPARK_HADOOP_";

  private final int metricsPort;
  private final String pushGatewayAddress;
  private final String appName;
  private final String master;
  private final String driverHost;
  private final int driverPort;
  private final int driverBlockManagerPort;
  private final String baseHdfsPath;
  private final String hdfsFsDefault;
  private final String timezone;
  private final Duration directorySettlingTime;
  private final boolean eventLogEnabled;
  private final String eventLogDir;
  private final Map<String, String> sparkConfiguration;
  private final Map<String, String> hadoopConfiguration;

  public SparkConfig() {
    this(loadProperties(), System.getenv());
  }

  SparkConfig(Properties properties, Map<String, String> environment) {
    appName = ConfigSupport.requiredProperty(properties, "spark.app.name");
    Properties runtime = runtimeProperties(environment);
    master = ConfigSupport.requiredProperty(runtime, "spark.master");
    driverHost = ConfigSupport.requiredProperty(runtime, "spark.driver.host");
    driverPort = ConfigSupport.positiveInt(runtime, "spark.driver.port");
    metricsPort = ConfigSupport.requiredPort(runtime, "metrics.port");
    pushGatewayAddress = ConfigSupport.requiredProperty(runtime, "pushgateway.address");
    driverBlockManagerPort = ConfigSupport.positiveInt(runtime, "spark.driver.blockManager.port");
    baseHdfsPath = ConfigSupport.requiredProperty(runtime, "parquet.base.path");
    hdfsFsDefault = ConfigSupport.requiredProperty(runtime, "hdfs.fs.defaultFS");
    timezone = ConfigSupport.requiredProperty(runtime, "hdfs.timezone");
    directorySettlingTime =
        Duration.ofHours(ConfigSupport.positiveInt(properties, "cube.directory.settling.hours"));
    eventLogEnabled = ConfigSupport.requiredBoolean(properties, "spark.eventLog.enabled");
    eventLogDir =
        eventLogEnabled
            ? ConfigSupport.requiredProperty(properties, "spark.eventLog.dir")
            : ConfigSupport.optionalProperty(properties, "spark.eventLog.dir");
    sparkConfiguration = sparkConfiguration(properties);
    hadoopConfiguration = hadoopConfiguration(environment);
  }

  private static Properties loadProperties() {
    return ConfigSupport.loadApplicationProperties(SparkConfig.class);
  }

  private static Properties runtimeProperties(Map<String, String> environment) {
    Properties runtime = new Properties();
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "metrics.port", environment, METRICS_PORT_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "pushgateway.address", environment, PUSHGATEWAY_ADDRESS_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "spark.master", environment, SPARK_MASTER_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "spark.driver.host", environment, SPARK_DRIVER_HOST_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "spark.driver.port", environment, SPARK_DRIVER_PORT_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime,
        "spark.driver.blockManager.port",
        environment,
        SPARK_DRIVER_BLOCK_MANAGER_PORT_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "parquet.base.path", environment, PARQUET_BASE_PATH_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "hdfs.fs.defaultFS", environment, HDFS_DEFAULT_FS_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "hdfs.timezone", environment, HDFS_TIMEZONE_ENV);
    return runtime;
  }

  private static Map<String, String> hadoopConfiguration(Map<String, String> environment) {
    Map<String, String> configuration = new HashMap<>();
    environment.forEach(
        (key, value) -> {
          if (key.startsWith(SPARK_HADOOP_ENV_PREFIX)) {
            String hadoopKey = key.substring(SPARK_HADOOP_ENV_PREFIX.length());
            if (hadoopKey.isBlank() || value == null || value.isBlank()) {
              throw new IllegalStateException(
                  "Invalid Hadoop configuration environment variable: " + key);
            }
            configuration.put(hadoopKey, value);
          }
        });
    return Map.copyOf(configuration);
  }

  private static Map<String, String> sparkConfiguration(Properties properties) {
    Map<String, String> configuration = new HashMap<>();
    properties.stringPropertyNames().stream()
        .filter(key -> key.startsWith("spark."))
        .forEach(key -> configuration.put(key, ConfigSupport.requiredProperty(properties, key)));
    return Map.copyOf(configuration);
  }
}
