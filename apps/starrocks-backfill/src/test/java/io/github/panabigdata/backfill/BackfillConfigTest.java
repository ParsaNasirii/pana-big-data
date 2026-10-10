package io.github.panabigdata.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringReader;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class BackfillConfigTest {
  @Test
  void loadsConfigMapPropertiesAndEnvironmentCredentials() throws IOException {
    Properties properties = properties();
    Map<String, String> environment =
        Map.of(
            "STARROCKS_USERNAME", "backfill-user",
            "STARROCKS_PASSWORD", "test-password",
            "HADOOP_USER_NAME", "hdfs-user");

    BackfillConfig config = new BackfillConfig(properties, environment);

    assertEquals(1789865340123L, config.getStart());
    assertEquals(1_000L, config.getEnd() - config.getStart());
    assertEquals("backfill-user", config.getCredentials().getProperty("user"));
    assertEquals("test-password", config.getCredentials().getProperty("password"));
    assertEquals("hdfs-user", config.getHdfsUser());
    assertEquals("/netflow", config.getParquetBasePath());
  }

  @Test
  void rejectsEmptyOrReversedWindows() throws IOException {
    Properties properties = properties();
    properties.setProperty("backfill.end", properties.getProperty("backfill.start"));
    assertThrows(IllegalArgumentException.class, () -> new BackfillConfig(properties, Map.of()));
    properties.setProperty("backfill.end", "2026-09-19T00:49:00.123Z");
    assertThrows(IllegalArgumentException.class, () -> new BackfillConfig(properties, Map.of()));
  }

  @Test
  void convertsOffsetsWithoutLosingMilliseconds() {
    long utc = BackfillConfig.timestamp("2026-09-20T00:49:00.123Z");
    assertEquals(1789865340123L, utc);
    assertEquals(utc, BackfillConfig.timestamp("2026-09-20T04:19:00.123+03:30"));
  }

  @Test
  void rejectsTimestampsWithoutTimezoneOrWithSubMillisecondPrecision() {
    assertThrows(
        DateTimeParseException.class, () -> BackfillConfig.timestamp("2026-09-20T00:49:00"));
    assertThrows(
        IllegalArgumentException.class,
        () -> BackfillConfig.timestamp("2026-09-20T00:49:00.123001Z"));
  }

  private Properties properties() throws IOException {
    String settings =
        """
        backfill.start=2026-09-20T00:49:00.123Z
        backfill.end=2026-09-20T00:49:01.123Z
        backfill.path=/netflow
        hadoop.config.directory=/etc/hadoop/conf
        starrocks.jdbc.url=jdbc:mysql://localhost:9030/panabigdata
        jdbc.connectTimeout=10000
        jdbc.socketTimeout=1860000
        jdbc.sslMode=DISABLED
        session.time_zone=Asia/Tehran
        session.query_timeout=1800
        session.insert_timeout=1800
        session.query_mem_limit=536870912
        session.load_mem_limit=536870912
        """;
    Properties properties = new Properties();
    properties.load(new StringReader(settings));
    return properties;
  }
}
