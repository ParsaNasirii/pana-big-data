package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.config.StarRocksConfig;
import io.github.panabigdata.kafka.RandomRecordGenerator;
import io.github.panabigdata.metrics.StarRocksWriterMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

class StarRocksIntegrationTest {

  @RegisterExtension
  static final StarRocksContainerExtension starrocks = new StarRocksContainerExtension();

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final StarRocksWriterMetrics metrics = new StarRocksWriterMetrics(registry);

  @AfterEach
  void closeRegistry() {
    registry.close();
  }

  @Test
  @Timeout(30)
  void frontendAndBackendAreRegisteredAndAlive() throws Exception {
    try (Connection connection = starrocks.openConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet frontends = statement.executeQuery("SHOW FRONTENDS")) {
        assertThat(frontends.next()).isTrue();
        assertThat(frontends.getBoolean("Alive")).isTrue();
        assertThat(frontends.next()).isFalse();
      }
      try (ResultSet backends = statement.executeQuery("SHOW BACKENDS")) {
        assertThat(backends.next()).isTrue();
        assertThat(backends.getBoolean("Alive")).isTrue();
        assertThat(backends.getInt("HeartbeatPort")).isEqualTo(9050);
        assertThat(backends.next()).isFalse();
      }
    }
  }

  @Test
  @Timeout(90)
  void csvRowsAreStoredOnBackendAndQueriedThroughFrontend() throws Exception {
    LocalDate today = LocalDate.now(ZoneId.of("Asia/Tehran"));
    long afterMidnight = today.atStartOfDay(ZoneId.of("Asia/Tehran")).toInstant().toEpochMilli();
    long beforeMidnight = afterMidnight - 1;
    var record = StarRocksTestData.record();
    StarRocksNetFlowRow first =
        StarRocksNetFlowRow.from(record, beforeMidnight, StarRocksTestData.COLUMNS);
    var javaStrings =
        RandomRecordGenerator.withSchema(
            record.value(), RandomRecordGenerator.schemaWithJavaStrings());
    javaStrings.put("IPV4_SRC_ADDR", javaStrings.get("IPV4_SRC_ADDR").toString());
    javaStrings.put("IPV4_DST_ADDR", javaStrings.get("IPV4_DST_ADDR").toString());
    StarRocksNetFlowRow second =
        StarRocksNetFlowRow.from(
            new ConsumerRecord<>(
                record.topic(), record.partition(), record.offset(), null, javaStrings),
            afterMidnight,
            StarRocksTestData.COLUMNS);

    try (Connection connection = starrocks.openConnection();
        Statement statement = connection.createStatement()) {
      initializeTable(statement);

      StreamLoadResult load =
          new StarRocksStreamLoadClient(streamLoadConfig(), metrics)
              .load("integration-csv", List.of(first, first, second), StarRocksTestData.COLUMNS);
      assertThat(load.status()).as("Stream Load response: %s", load).isEqualTo("Success");
      assertThat(load.numberLoadedRows()).isEqualTo(3L);
      assertThat(load.numberFilteredRows()).isZero();
      assertThat(load.numberUnselectedRows()).isZero();

      try (ResultSet rows =
          statement.executeQuery(
              "SELECT * FROM panabigdata.netflow_raw ORDER BY KAFKA_TIMESTAMP_MS")) {
        for (long timestamp : new long[] {beforeMidnight, beforeMidnight, afterMidnight}) {
          assertThat(rows.next()).isTrue();
          assertThat(rows.getLong("KAFKA_TIMESTAMP_MS")).isEqualTo(timestamp);
          assertThat(rows.getString("KAFKA_DAY"))
              .isEqualTo((timestamp == beforeMidnight ? today.minusDays(1) : today).toString());
          assertThat(rows.getString("IPV4_SRC_ADDR")).isEqualTo("10.1.2.3");
          assertThat(rows.getString("IPV4_DST_ADDR")).isEqualTo("10.9.8.7");
          assertThat(rows.getInt("L4_SRC_PORT"))
              .isEqualTo(((Integer) record.value().get("L4_SRC_PORT")));
          assertThat(rows.getInt("PROTOCOL")).isEqualTo(((Integer) record.value().get("PROTOCOL")));
          assertThat(rows.getDouble("L7_PROTO"))
              .isEqualTo(((Double) record.value().get("L7_PROTO")));
          assertThat(rows.getLong("IN_BYTES")).isEqualTo(((Long) record.value().get("IN_BYTES")));
          assertThat(rows.getLong("OUT_BYTES")).isEqualTo(((Long) record.value().get("OUT_BYTES")));
        }
        assertThat(rows.next()).isFalse();
      }

      String query =
          "SELECT SUM(IN_BYTES) FROM panabigdata.netflow_raw "
              + "WHERE KAFKA_DAY = '"
              + today
              + "' "
              + "AND KAFKA_TIMESTAMP_MS = "
              + afterMidnight;
      try (ResultSet result = statement.executeQuery(query)) {
        assertThat(result.next()).isTrue();
        assertThat(result.getLong(1)).isEqualTo(((Long) record.value().get("IN_BYTES")));
      }
      try (ResultSet explain = statement.executeQuery("EXPLAIN " + query)) {
        StringBuilder plan = new StringBuilder();
        while (explain.next()) {
          plan.append(explain.getString(1)).append('\n');
        }
        assertThat(plan.toString()).contains("partitions=1/2");
      }
    }
  }

  private void initializeTable(Statement statement) throws Exception {
    Path ddlPath = Path.of(System.getProperty("basedir", "."), "init.sql");
    if (!Files.exists(ddlPath)) {
      ddlPath = Path.of("apps/starrocks-writer/init.sql");
    }
    String ddl =
        Files.readString(ddlPath)
            .replace("\"replication_num\" = \"3\"", "\"replication_num\" = \"1\"");
    for (String sql : ddl.split(";")) {
      if (!sql.isBlank()) {
        statement.execute(sql);
      }
    }
  }

  private StarRocksConfig streamLoadConfig() {
    Properties properties = new Properties();
    properties.setProperty("starrocks.fe.host", starrocks.getBackendHost());
    properties.setProperty("starrocks.http.port", Integer.toString(starrocks.getBackendHttpPort()));
    properties.setProperty("starrocks.database", "panabigdata");
    properties.setProperty("starrocks.table", "netflow_raw");
    properties.setProperty("starrocks.batch.max.records", "1000");
    properties.setProperty("starrocks.connect.timeout.ms", "5000");
    properties.setProperty("starrocks.request.timeout.ms", "30000");
    properties.setProperty("metrics.port", "8380");
    properties.setProperty("starrocks.username", "root");
    properties.setProperty("starrocks.password", StarRocksContainerExtension.PASSWORD);
    return new StarRocksConfig(properties);
  }
}
