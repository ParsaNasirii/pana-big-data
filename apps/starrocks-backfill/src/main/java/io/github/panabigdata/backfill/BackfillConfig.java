package io.github.panabigdata.backfill;

import io.github.panabigdata.ConfigSupport;
import io.github.panabigdata.hdfs.HadoopConfigSupport;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

@Getter
public final class BackfillConfig {
  private final long start;
  private final long end;
  private final String jdbcUrl;
  private final Properties credentials;
  private final String parquetBasePath;
  private final Configuration hadoop;
  private final String hdfsUser;
  private final Map<String, String> sessionSettings;
  private final int queryTimeout;

  public BackfillConfig() {
    this(ConfigSupport.loadApplicationProperties(BackfillConfig.class), System.getenv());
  }

  BackfillConfig(Properties properties, Map<String, String> environment) {
    start = timestamp(ConfigSupport.requiredProperty(properties, "backfill.start"));
    end = timestamp(ConfigSupport.requiredProperty(properties, "backfill.end"));
    if (start >= end) {
      throw new IllegalArgumentException("backfill.start must be before backfill.end");
    }
    jdbcUrl = ConfigSupport.requiredProperty(properties, "starrocks.jdbc.url");
    parquetBasePath = ConfigSupport.requiredProperty(properties, "backfill.path");
    queryTimeout = ConfigSupport.positiveInt(properties, "session.query_timeout");

    credentials = new Properties();
    for (String name : List.of("connectTimeout", "socketTimeout", "sslMode")) {
      credentials.setProperty(name, ConfigSupport.requiredProperty(properties, "jdbc." + name));
    }
    ConfigSupport.overrideRequiredEnvironment(
        credentials, "user", environment, "STARROCKS_USERNAME");
    ConfigSupport.overrideRequiredEnvironment(
        credentials, "password", environment, "STARROCKS_PASSWORD");
    Properties runtime = ConfigSupport.copyProperties(properties);
    ConfigSupport.overrideRequiredEnvironment(
        runtime, "hadoop.user", environment, "HADOOP_USER_NAME");
    hdfsUser = ConfigSupport.requiredProperty(runtime, "hadoop.user");

    sessionSettings = new HashMap<>();
    for (String name :
        List.of(
            "time_zone", "query_timeout", "insert_timeout", "query_mem_limit", "load_mem_limit")) {
      sessionSettings.put(name, ConfigSupport.requiredProperty(properties, "session." + name));
    }
    hadoop = new HdfsConfiguration();
    HadoopConfigSupport.addSiteResources(
        hadoop, ConfigSupport.requiredProperty(properties, "hadoop.config.directory"));
  }

  static long timestamp(String value) {
    Instant time = OffsetDateTime.parse(value).toInstant();
    if (time.getNano() % 1_000_000 != 0) {
      throw new IllegalArgumentException(
          "Backfill timestamps support at most millisecond precision");
    }
    return time.toEpochMilli();
  }
}
