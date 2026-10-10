package io.github.panabigdata.storage;

import io.github.panabigdata.configs.PostgresConfig;
import io.github.panabigdata.cube.maker.BasicCubeMaker;
import io.github.panabigdata.metrics.CubeMakerMetrics;
import io.github.panabigdata.metrics.CubeMakerMetrics.Outcome;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

@Slf4j
public final class PostgresCubeWriter implements CubeWriter, DirectoryTrackingStore {
  private final Connection connection;
  private final String jdbcUrl;
  private final Properties properties;
  private final CubeMakerMetrics metrics;
  private final AtomicBoolean closed = new AtomicBoolean();

  public PostgresCubeWriter(Connection connection, PostgresConfig config, CubeMakerMetrics metrics)
      throws SQLException {
    this.connection = connection;
    this.jdbcUrl = config.getUrl();
    this.properties = new Properties();
    this.properties.setProperty("user", config.getUsername());
    this.properties.setProperty("password", config.getPassword());
    this.metrics = metrics;
    connection.setAutoCommit(false);
  }

  @Override
  public Set<String> getProcessedDirectories() throws SQLException {
    return inTransaction(
        () -> {
          Set<String> directories = new HashSet<>();
          try (Statement statement = connection.createStatement();
              ResultSet rows = statement.executeQuery("SELECT path_name FROM batch_tracking")) {
            while (rows.next()) directories.add(rows.getString("path_name"));
          }
          return directories;
        });
  }

  @Override
  public void markDirectoryAsProcessed(String path) throws SQLException {
    inTransaction(
        () -> {
          try (PreparedStatement statement =
              connection.prepareStatement(
                  "INSERT INTO batch_tracking (path_name) VALUES (?) ON CONFLICT DO NOTHING")) {
            statement.setString(1, path);
            statement.executeUpdate();
            return 0;
          }
        });
    log.debug("Batch directory marked as processed: {}", path);
  }

  @Override
  public void writeCube(Dataset<Row> dataframe, BasicCubeMaker producer) throws SQLException {
    String targetTable = producer.getTableName();
    String stagingTable = targetTable + "_staging";
    String cubeName = producer.toString();
    writeStagingTable(dataframe, cubeName, stagingTable);
    mergeStagingTable(cubeName, stagingTable, buildUpsertQuery(dataframe, producer, stagingTable));
  }

  private void writeStagingTable(Dataset<Row> dataframe, String cubeName, String stagingTable) {
    log.info("Writing {} data to staging table {}", cubeName, stagingTable);
    long started = System.nanoTime();
    Outcome outcome = Outcome.FAILED;
    try {
      dataframe.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, stagingTable, properties);
      outcome = Outcome.SUCCEEDED;
    } finally {
      metrics.recordStagingWrite(cubeName, outcome, System.nanoTime() - started);
    }
  }

  void mergeStagingTable(String cubeName, String stagingTable, String query) throws SQLException {
    long started = System.nanoTime();
    Outcome outcome = Outcome.FAILED;
    try {
      long rows =
          inTransaction(
              () -> {
                try (Statement statement = connection.createStatement()) {
                  long affectedRows = statement.executeLargeUpdate(query);
                  statement.execute("DROP TABLE " + stagingTable);
                  return affectedRows;
                }
              });
      metrics.recordRowsUpserted(cubeName, rows);
      outcome = Outcome.SUCCEEDED;
      log.info("Upserted {} rows into table {}", rows, cubeName);
    } finally {
      metrics.recordUpsert(cubeName, outcome, System.nanoTime() - started);
    }
  }

  private static String buildUpsertQuery(
      Dataset<Row> dataframe, BasicCubeMaker producer, String stagingTable) {
    String targetTable = producer.getTableName();
    String[] columns = dataframe.columns();
    List<String> primaryKeys = producer.primaryKeys();

    String primaryKeysString = String.join(", ", primaryKeys);
    List<String> updateSets = new ArrayList<>();
    for (String col : columns) {
      if (!primaryKeys.contains(col)) {
        updateSets.add(col + " = EXCLUDED." + col);
      }
    }

    List<String> selectColumnsList = new ArrayList<>();
    for (String col : columns) {
      if (col.equalsIgnoreCase("src_ip")
          || col.equalsIgnoreCase("dst_ip")
          || col.equalsIgnoreCase("ip_address")) {
        selectColumnsList.add("CAST(" + col + " AS inet)");
      } else {
        selectColumnsList.add(col);
      }
    }

    String updateString = String.join(", ", updateSets);
    String columnNames = String.join(", ", columns);
    String columnSelect = String.join(", ", selectColumnsList);

    return String.format(
        "INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
        targetTable, columnNames, columnSelect, stagingTable, primaryKeysString, updateString);
  }

  private <T> T inTransaction(SqlOperation<T> operation) throws SQLException {
    try {
      T result = operation.execute();
      connection.commit();
      return result;
    } catch (SQLException | RuntimeException exception) {
      log.warn("Operation failed; rolling back");
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        exception.addSuppressed(rollbackFailure);
        try {
          abort();
        } catch (SQLException abortFailure) {
          exception.addSuppressed(abortFailure);
        }
      }
      throw exception;
    }
  }

  public void abort() throws SQLException {
    if (closed.compareAndSet(false, true)) {
      log.warn("Aborting database connection");
      try {
        connection.abort(Runnable::run);
      } catch (SQLException exception) {
        closed.set(false);
        throw exception;
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (closed.compareAndSet(false, true)) {
      log.info("Closing database connection");
      connection.close();
      log.info("Database connection closed");
    }
  }

  @FunctionalInterface
  private interface SqlOperation<T> {
    T execute() throws SQLException;
  }
}
