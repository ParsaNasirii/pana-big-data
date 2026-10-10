package io.github.panabigdata.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Files;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.parquet.avro.AvroParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(parallel = true)
class BackfillIntegrationTest {
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
  @TempDir private java.nio.file.Path directory;
  private Configuration hadoop;
  private MiniDFSCluster hdfs;
  private SingleConnectionDataSource connection;
  private JdbcTemplate jdbc;
  private String url;

  @Container
  private final GenericContainer<?> fe =
      new GenericContainer<>("docker.io/starrocks/fe-ubuntu:4.0.12")
          // Test services use IP addresses; avoid external DNS lookups in Hadoop.
          .withEnv("JAVA_TOOL_OPTIONS", "-Djdk.net.hosts.file=/etc/hosts")
          .withCommand(
              "bash",
              "-ec",
              "sed -i 's/-Xmx8192m/-Xmx1024m/' /opt/starrocks/fe/conf/fe.conf; "
                  + "exec /opt/starrocks/fe/bin/start_fe.sh --logconsole")
          .withExposedPorts(9030)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));

  // The destination table requires two replicas.
  @Container private final GenericContainer<?> firstBackend = backend();
  @Container private final GenericContainer<?> secondBackend = backend();

  @BeforeEach
  void prepareServices() throws Exception {
    url = "jdbc:mysql://" + fe.getHost() + ":" + fe.getMappedPort(9030) + "/";
    hadoop = new HdfsConfiguration();
    hadoop.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, directory.resolve("hdfs").toString());
    hadoop.set("dfs.namenode.rpc-bind-host", "0.0.0.0");
    hadoop.set("dfs.datanode.address", "0.0.0.0:0");
    // HTTP authentication setup must not depend on the machine's hostname.
    hadoop.set("bind.address", "127.0.0.1");

    hdfs = new MiniDFSCluster.Builder(hadoop).numDataNodes(0).waitSafeMode(false).build();
    connection =
        new SingleConnectionDataSource(
            url + "?connectTimeout=5000&socketTimeout=120000", "root", "", false);
    jdbc = new JdbcTemplate(connection);
    jdbc.setQueryTimeout(120);
    prepareStarRocks();
    prepareHdfs();
    jdbc.execute(
        """
          CREATE TABLE netflow_raw (
            KAFKA_TIMESTAMP_MS BIGINT NOT NULL,
            IPV4_SRC_ADDR VARCHAR(15) NOT NULL,
            IN_BYTES BIGINT NOT NULL,
            OUT_BYTES BIGINT NOT NULL,
            OPTIONAL_FIELD BIGINT NULL,
            KAFKA_DAY DATE AS (CAST(FROM_UNIXTIME(KAFKA_TIMESTAMP_MS DIV 1000) AS DATE))
          )
          DUPLICATE KEY(KAFKA_TIMESTAMP_MS)
          PARTITION BY date_trunc('day', KAFKA_DAY)
          DISTRIBUTED BY HASH(IPV4_SRC_ADDR) BUCKETS 4
          PROPERTIES ("replication_num" = "2")
        """);
    jdbc.update(
        """
          INSERT INTO netflow_raw (KAFKA_TIMESTAMP_MS, IPV4_SRC_ADDR, IN_BYTES, OUT_BYTES)
          VALUES (?, '10.9.9.9', 1, 2)
        """,
        Instant.parse("2026-09-20T06:30:00Z").toEpochMilli());
  }

  @AfterEach
  void closeServices() throws Exception {
    try {
      if (connection != null) connection.close();
    } finally {
      try {
        if (hdfs != null) hdfs.close();
      } finally {
        DefaultMetricsSystem.shutdown();
      }
    }
  }

  @Test
  void restoresWindowWithoutDuplicatingOrDeletingOtherRows() throws Exception {
    writeArchive(
        "2026-09-30/23",
        "2026-09-30T20:14:59.999Z",
        "2026-09-30T20:15:00Z",
        "2026-09-30T20:15:00Z");
    writeArchive(
        "2026-10-01/00",
        "2026-09-30T20:30:00Z",
        "2026-09-30T20:44:59.999Z",
        "2026-09-30T20:45:00Z");
    insertRawRow("2026-09-30T20:14:59.999Z", "10.9.9.1");
    var surroundingRows =
        jdbc.queryForList("SELECT * FROM netflow_raw ORDER BY KAFKA_TIMESTAMP_MS");
    // This partial old result must be replaced, not appended to.
    insertRawRow("2026-09-30T20:15:00Z", "10.9.9.3");

    new Backfill(config()).run();

    var rows =
        jdbc.queryForList(
            "SELECT * FROM netflow_raw WHERE IPV4_SRC_ADDR = '10.0.0.1' ORDER BY KAFKA_TIMESTAMP_MS");
    var timestamps =
        List.of(
            "2026-09-30T20:15:00Z",
            "2026-09-30T20:15:00Z",
            "2026-09-30T20:30:00Z",
            "2026-09-30T20:44:59.999Z");
    assertEquals(4, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      var row = rows.get(i);
      assertEquals(Instant.parse(timestamps.get(i)).toEpochMilli(), row.get("KAFKA_TIMESTAMP_MS"));
      assertEquals(Date.valueOf(i < 2 ? "2026-09-30" : "2026-10-01"), row.get("KAFKA_DAY"));
      assertEquals(3_000_000_000L, row.get("IN_BYTES"));
      assertEquals(100L, row.get("OUT_BYTES"));
      assertNull(row.get("OPTIONAL_FIELD"));
    }
    assertEquals(
        surroundingRows,
        jdbc.queryForList(
            "SELECT * FROM netflow_raw WHERE IPV4_SRC_ADDR <> '10.0.0.1' ORDER BY KAFKA_TIMESTAMP_MS"));
    insertRawRow("2026-09-30T20:45:00Z", "10.9.9.2");
    var restoredRows = jdbc.queryForList("SELECT * FROM netflow_raw ORDER BY KAFKA_TIMESTAMP_MS");

    new Backfill(config()).run();
    assertEquals(
        restoredRows, jdbc.queryForList("SELECT * FROM netflow_raw ORDER BY KAFKA_TIMESTAMP_MS"));

    // An overlapping rerun also replaces just its own window.
    new Backfill(config("2026-10-01T00:00:00+03:30", "2026-10-01T00:10:00+03:30")).run();
    assertEquals(
        restoredRows, jdbc.queryForList("SELECT * FROM netflow_raw ORDER BY KAFKA_TIMESTAMP_MS"));
    assertTrue(jdbc.queryForList("SHOW TABLES LIKE 'netflow_backfill%'").isEmpty());
  }

  @Test
  void emptyWindowKeepsMainTableUnchanged() throws Exception {
    writeArchive("2026-09-30/23", "2026-09-30T20:14:59.999Z");
    writeArchive("2026-10-01/00", "2026-09-30T20:45:00Z");
    var previousRows = jdbc.queryForList("SELECT * FROM netflow_raw");

    assertThrows(IllegalStateException.class, () -> new Backfill(config()).run());

    assertEquals(previousRows, jdbc.queryForList("SELECT * FROM netflow_raw"));
    assertTrue(jdbc.queryForList("SHOW TABLES LIKE 'netflow_backfill%'").isEmpty());
  }

  @Test
  void invalidTimestampKeepsMainTableUnchanged() throws Exception {
    writeArchive("2026-09-30/23", "2026-09-30T20:15:00Z", "invalid-timestamp");
    var previousRows = jdbc.queryForList("SELECT * FROM netflow_raw");

    var error = assertThrows(DataAccessException.class, () -> new Backfill(config()).run());
    assertTrue(error.getMostSpecificCause().getMessage().contains("filtered data"));

    assertEquals(previousRows, jdbc.queryForList("SELECT * FROM netflow_raw"));
    assertTrue(jdbc.queryForList("SHOW TABLES LIKE 'netflow_backfill%'").isEmpty());
  }

  private void insertRawRow(String timestamp, String ip) {
    jdbc.update(
        "INSERT INTO netflow_raw (KAFKA_TIMESTAMP_MS, IPV4_SRC_ADDR, IN_BYTES, OUT_BYTES) VALUES (?, ?, 1, 2)",
        Instant.parse(timestamp).toEpochMilli(),
        ip);
  }

  private void prepareStarRocks() throws InterruptedException {
    for (var backend : List.of(firstBackend, secondBackend)) {
      String ip = containerAddress(backend);
      jdbc.execute("ALTER SYSTEM ADD BACKEND '" + ip + ":9050'");
    }
    long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
    boolean ready = false;
    while (System.nanoTime() < deadline) {
      var backends =
          jdbc.query(
              "SHOW BACKENDS",
              (row, index) ->
                  row.getBoolean("Alive") && !row.getString("TotalCapacity").startsWith("0.000 "));
      ready = backends.equals(List.of(true, true));
      if (ready) {
        break;
      }
      Thread.sleep(500);
    }
    if (!ready) {
      throw new IllegalStateException("StarRocks backends did not become ready");
    }
    // Allow slower tablet creation on the CI host.
    jdbc.execute("ADMIN SET FRONTEND CONFIG ('tablet_create_timeout_second' = '20')");
    jdbc.execute("SET PASSWORD FOR 'root' = PASSWORD('test-password')");
    jdbc.execute("CREATE DATABASE backfill_test");
    jdbc.execute("USE backfill_test");
    jdbc.execute("SET time_zone = 'Asia/Tehran'");
    jdbc.execute("SET new_planner_optimize_timeout = 30000");
  }

  private void prepareHdfs() throws Exception {
    // Select the host address reachable from Docker without resolving the machine's hostname.
    String address;
    try (var route = new DatagramSocket()) {
      route.connect(InetAddress.getByName(containerAddress(firstBackend)), 9050);
      address = route.getLocalAddress().getHostAddress() + ":" + hdfs.getNameNodePort();
    }
    hadoop.set("fs.defaultFS", "hdfs://" + address);
    hadoop.set("dfs.namenode.rpc-address", address);
    hadoop.setInt("dfs.replication", 1);
    hdfs.startDataNodes(hadoop, 1, true, null, null, null, null, false, true);
    hdfs.waitDatanodeFullyStarted(hdfs.getDataNodes().get(0), 60_000);
    try (var output = Files.newOutputStream(directory.resolve("core-site.xml"))) {
      hadoop.writeXml(output);
    }
  }

  private static String containerAddress(GenericContainer<?> container) {
    return container
        .getContainerInfo()
        .getNetworkSettings()
        .getNetworks()
        .values()
        .iterator()
        .next()
        .getIpAddress();
  }

  private void writeArchive(String hour, String... timestamps) throws Exception {
    var schema =
        SchemaBuilder.record("Netflow")
            .fields()
            .requiredString("KAFKA_TIMESTAMP")
            .requiredString("IPV4_SRC_ADDR")
            .requiredLong("IN_BYTES")
            .requiredLong("OUT_BYTES")
            .endRecord();
    Path path = new Path("/netflow/" + hour + "/records.parquet");
    try (var files = FileSystem.get(hadoop)) {
      files.mkdirs(path.getParent());
      try (var writer =
          AvroParquetWriter.<GenericRecord>builder(path)
              .withConf(hadoop)
              .withSchema(schema)
              .build()) {
        for (String timestamp : timestamps) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("KAFKA_TIMESTAMP", timestamp);
          record.put("IPV4_SRC_ADDR", "10.0.0.1");
          record.put("IN_BYTES", 3_000_000_000L);
          record.put("OUT_BYTES", 100L);
          writer.write(record);
        }
      }
    }
  }

  private BackfillConfig config() {
    return config("2026-09-30T23:45:00+03:30", "2026-10-01T00:15:00+03:30");
  }

  private BackfillConfig config(String start, String end) {
    Properties properties = new Properties();
    properties.setProperty("backfill.start", start);
    properties.setProperty("backfill.end", end);
    properties.setProperty("backfill.path", "/netflow");
    properties.setProperty("hadoop.config.directory", directory.toString());
    properties.setProperty("starrocks.jdbc.url", url + "backfill_test");
    properties.setProperty("jdbc.connectTimeout", "5000");
    properties.setProperty("jdbc.socketTimeout", "120000");
    properties.setProperty("jdbc.sslMode", "DISABLED");
    properties.setProperty("session.time_zone", "Asia/Tehran");
    properties.setProperty("session.query_timeout", "120");
    properties.setProperty("session.insert_timeout", "120");
    properties.setProperty("session.query_mem_limit", "536870912");
    properties.setProperty("session.load_mem_limit", "536870912");
    return new BackfillConfig(
        properties,
        Map.of(
            "STARROCKS_USERNAME", "root",
            "STARROCKS_PASSWORD", "test-password",
            "HADOOP_USER_NAME", System.getProperty("user.name")));
  }

  private static GenericContainer<?> backend() {
    return new GenericContainer<>("docker.io/starrocks/be-ubuntu:4.0.12")
        // mem_limit does not configure the embedded JVM's heap.
        .withEnv("JAVA_TOOL_OPTIONS", "-Xms64m -Xmx256m -Djdk.net.hosts.file=/etc/hosts")
        .withCommand(
            "bash",
            "-ec",
            "mkdir -p /opt/starrocks/be/storage; "
                + "printf '\nmem_limit = 1G\nnum_cores = 2\n' >> /opt/starrocks/be/conf/be.conf; "
                + "exec /opt/starrocks/be/bin/start_be.sh --logconsole")
        .withExposedPorts(8040)
        .waitingFor(Wait.forHttp("/api/health").forPort(8040).withStartupTimeout(STARTUP_TIMEOUT));
  }
}
