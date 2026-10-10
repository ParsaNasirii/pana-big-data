package io.github.panabigdata.config;

import io.github.panabigdata.ConfigSupport;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;

@Getter
public class StarRocksConfig {

  private static final String STARROCKS_USERNAME_ENV = "STARROCKS_USERNAME";
  private static final String STARROCKS_PASSWORD_ENV = "STARROCKS_PASSWORD";
  private static final String METRICS_PORT_ENV = "METRICS_PORT";

  private final List<String> feHosts;
  private final int httpPort;
  private final String database;
  private final String table;
  private final int batchMaxRecords;
  private final long connectTimeoutMillis;
  private final long requestTimeoutMillis;
  private final int metricsPort;
  private final String username;
  private final String password;

  public StarRocksConfig() {
    this(ConfigSupport.loadApplicationProperties(StarRocksConfig.class), System.getenv());
  }

  StarRocksConfig(Properties properties, Map<String, String> environment) {
    this(withRuntimeEnvironment(properties, environment));
  }

  public StarRocksConfig(Properties properties) {
    this.username = ConfigSupport.requiredProperty(properties, "starrocks.username");
    this.password = ConfigSupport.requiredProperty(properties, "starrocks.password");
    this.feHosts =
        ConfigSupport.requiredArrayOrSingle(properties, "starrocks.fe.host", "starrocks.fe.hosts");
    this.httpPort = ConfigSupport.requiredPort(properties, "starrocks.http.port");
    this.database = ConfigSupport.requiredProperty(properties, "starrocks.database").trim();
    this.table = ConfigSupport.requiredProperty(properties, "starrocks.table").trim();
    this.batchMaxRecords = ConfigSupport.positiveInt(properties, "starrocks.batch.max.records");
    this.connectTimeoutMillis =
        ConfigSupport.positiveLong(properties, "starrocks.connect.timeout.ms");
    this.requestTimeoutMillis =
        ConfigSupport.positiveLong(properties, "starrocks.request.timeout.ms");
    this.metricsPort = ConfigSupport.requiredPort(properties, "metrics.port");
  }

  private static Properties withRuntimeEnvironment(
      Properties properties, Map<String, String> environment) {
    Properties merged = ConfigSupport.copyProperties(properties);
    ConfigSupport.overrideRequiredEnvironment(
        merged, "starrocks.username", environment, STARROCKS_USERNAME_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        merged, "starrocks.password", environment, STARROCKS_PASSWORD_ENV);
    ConfigSupport.overrideRequiredEnvironment(
        merged, "metrics.port", environment, METRICS_PORT_ENV);
    return merged;
  }
}
