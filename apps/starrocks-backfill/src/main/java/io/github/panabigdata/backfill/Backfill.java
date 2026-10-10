package io.github.panabigdata.backfill;

import com.mysql.cj.jdbc.JdbcStatement;
import io.github.panabigdata.hdfs.HourlyDirectoryService;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

@Slf4j
public final class Backfill {
  private final BackfillConfig config;

  public Backfill(BackfillConfig config) {
    this.config = config;
  }

  // Keep starrocks-writer paused until the restore finishes to avoid racing live inserts.
  public void run() throws IOException, InterruptedException {
    try (var dataSource = new SingleConnectionDataSource(config.getJdbcUrl(), false)) {
      dataSource.setConnectionProperties(config.getCredentials());
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      jdbc.setQueryTimeout(config.getQueryTimeout());
      NamedParameterJdbcTemplate sql = new NamedParameterJdbcTemplate(jdbc);
      configureSession(sql);

      Map<String, Object> archiveLoadParameters = createArchiveLoadParameters();
      String stagingTable = createStagingTable(jdbc);
      try {
        long loadedRows = loadParquetIntoStagingTable(sql, stagingTable, archiveLoadParameters);
        if (loadedRows == 0) {
          throw new IllegalStateException("Requested window is empty; destination was not changed");
        }
        replaceWindowInMainTable(sql, stagingTable);
        log.info("Backfill complete: {} rows", loadedRows);
      } finally {
        dropStagingTable(jdbc, stagingTable);
      }
    }
  }

  private Map<String, Object> createArchiveLoadParameters()
      throws IOException, InterruptedException {
    List<String> paths =
        UserGroupInformation.createRemoteUser(config.getHdfsUser())
            .doAs((PrivilegedExceptionAction<List<String>>) this::findParquetFileUrls);
    return Map.of(
        "paths", String.join(",", paths),
        "username", config.getHdfsUser(),
        "sampleFiles", Integer.toString(paths.size()),
        "start", config.getStart(),
        "end", config.getEnd());
  }

  private void replaceWindowInMainTable(NamedParameterJdbcTemplate sql, String stagingTable) {
    ZoneId zone = ZoneId.of(config.getSessionSettings().get("time_zone"));
    List<LocalDate> days =
        partitionDays(
            Instant.ofEpochMilli(config.getStart()), Instant.ofEpochMilli(config.getEnd()), zone);
    List<String> partitions = getOrCreateDailyPartitions(sql.getJdbcOperations(), days);
    Map<String, Object> windowParameters =
        Map.of(
            "start", config.getStart(),
            "end", config.getEnd(),
            "firstDay", days.get(0).toString(),
            "dayAfterLast", days.get(days.size() - 1).plusDays(1).toString());
    // INSERT OVERWRITE builds shadow partitions and swaps them atomically. Preserve
    // the surrounding rows because the requested window can cover only part of a day.
    String restore =
        """
        INSERT OVERWRITE netflow_raw PARTITION (%s) BY NAME
        SELECT * EXCLUDE (KAFKA_DAY) FROM %s
        UNION ALL
        SELECT * EXCLUDE (KAFKA_DAY) FROM netflow_raw
        WHERE KAFKA_DAY >= :firstDay AND KAFKA_DAY < :dayAfterLast
          AND (KAFKA_TIMESTAMP_MS < :start OR KAFKA_TIMESTAMP_MS >= :end)
        """
            .formatted(String.join(", ", partitions), stagingTable);
    sql.update(restore, windowParameters);
  }

  private static List<String> getOrCreateDailyPartitions(
      JdbcOperations jdbc, List<LocalDate> days) {
    List<String> existingPartitions =
        jdbc.query(
            "SHOW PARTITIONS FROM netflow_raw", (result, row) -> result.getString("PartitionName"));
    List<String> partitions = new ArrayList<>();
    for (LocalDate day : days) {
      String partition = "p" + day.format(DateTimeFormatter.BASIC_ISO_DATE);
      if (!existingPartitions.contains(partition)) {
        jdbc.execute(
            "ALTER TABLE netflow_raw ADD PARTITIONS START ('%s') END ('%s') EVERY (INTERVAL 1 DAY)"
                .formatted(day, day.plusDays(1)));
      }
      partitions.add(partition);
    }
    return partitions;
  }

  static List<LocalDate> partitionDays(Instant start, Instant end, ZoneId timeZone) {
    LocalDate firstDay = start.atZone(timeZone).toLocalDate();
    // The end of the requested window is exclusive.
    LocalDate lastDay = end.minusMillis(1).atZone(timeZone).toLocalDate();
    List<LocalDate> days = new ArrayList<>();
    for (LocalDate day = firstDay; !day.isAfter(lastDay); day = day.plusDays(1)) {
      days.add(day);
    }
    return days;
  }

  private void configureSession(NamedParameterJdbcTemplate sql) {
    sql.update(
        """
        SET time_zone = :time_zone, query_timeout = :query_timeout,
            insert_timeout = :insert_timeout, query_mem_limit = :query_mem_limit,
            load_mem_limit = :load_mem_limit,
            enable_insert_strict = true, insert_max_filter_ratio = 0
        """,
        config.getSessionSettings());
  }

  private List<String> findParquetFileUrls() throws IOException, InterruptedException {
    Instant start = Instant.ofEpochMilli(config.getStart());
    Instant end = Instant.ofEpochMilli(config.getEnd());
    ZoneId timeZone = ZoneId.of(config.getSessionSettings().get("time_zone"));
    try (var hdfs =
        FileSystem.newInstance(
            FileSystem.getDefaultUri(config.getHadoop()),
            config.getHadoop(),
            config.getHdfsUser())) {
      List<Path> files =
          new HourlyDirectoryService(hdfs)
              .findParquetFiles(new Path(config.getParquetBasePath()), start, end, timeZone);
      if (files.isEmpty()) {
        throw new IllegalStateException("No Parquet files found in the requested window");
      }

      // FILES() must connect to the active NameNode.
      var address = HAUtil.getAddressOfActive(hdfs);
      if (address == null) {
        throw new IllegalStateException("No active NameNode");
      }
      String namenode = address.getAddress().getHostAddress() + ":" + address.getPort();
      List<String> paths =
          files.stream().map(file -> "hdfs://" + namenode + file.toUri().getPath()).toList();
      log.info("Found {} Parquet files through {}", paths.size(), namenode);
      return paths;
    }
  }

  private String createStagingTable(JdbcTemplate jdbc) {
    String stagingTable =
        "netflow_backfill_staging_" + UUID.randomUUID().toString().replace("-", "");
    log.info(
        "Creating staging table {} for archive window [{}, {})",
        stagingTable,
        Instant.ofEpochMilli(config.getStart()),
        Instant.ofEpochMilli(config.getEnd()));
    try {
      jdbc.execute(
          """
          CREATE TABLE %s
          DISTRIBUTED BY HASH(IPV4_SRC_ADDR) BUCKETS 4
          PROPERTIES ("partition_retention_condition" = "")
          LIKE netflow_raw
          """
              .formatted(stagingTable));
      return stagingTable;
    } catch (DataAccessException creationFailure) {
      dropStagingTable(jdbc, stagingTable);
      throw creationFailure;
    }
  }

  private static void dropStagingTable(JdbcTemplate jdbc, String stagingTable) {
    try {
      jdbc.execute("DROP TABLE IF EXISTS " + stagingTable + " FORCE");
    } catch (DataAccessException cleanupFailure) {
      log.warn("Could not remove staging table {}", stagingTable, cleanupFailure);
    }
  }

  private static long loadParquetIntoStagingTable(
      NamedParameterJdbcTemplate sql,
      String stagingTable,
      Map<String, Object> archiveLoadParameters) {
    // Keep malformed timestamps in the load: the NOT NULL column and strict mode reject them.
    String insert =
        """
        INSERT INTO %s WITH LABEL %s BY NAME
        SELECT * EXCLUDE (KAFKA_TIMESTAMP) FROM (
          SELECT *,
                 milliseconds_diff(CAST(KAFKA_TIMESTAMP AS DATETIME), '1970-01-01 00:00:00')
                     AS KAFKA_TIMESTAMP_MS
          FROM FILES('path' = :paths, 'format' = 'parquet',
                     'hadoop.security.authentication' = 'simple', 'username' = :username,
                     'fill_mismatch_column_with' = 'null', 'auto_detect_sample_files' = :sampleFiles)
        ) AS archive
        WHERE (KAFKA_TIMESTAMP_MS >= :start AND KAFKA_TIMESTAMP_MS < :end)
           OR KAFKA_TIMESTAMP_MS IS NULL
        """
            .formatted(stagingTable, stagingTable);
    // The INSERT response includes loaded rows; no separate COUNT query is needed.
    return sql.execute(insert, archiveLoadParameters, Backfill::executeInsertAndCheckVisibility);
  }

  private static long executeInsertAndCheckVisibility(PreparedStatement statement)
      throws SQLException {
    long loadedRows = statement.executeLargeUpdate();
    if (loadedRows == 0) {
      return loadedRows;
    }
    String response = statement.unwrap(JdbcStatement.class).getResultSetInternal().getServerInfo();
    // COMMITTED can still mean publish timed out; only publish a VISIBLE load.
    if (response == null || !response.contains("'status':'VISIBLE'")) {
      throw new IllegalStateException("Archive load is not visible: " + response);
    }
    return loadedRows;
  }
}
