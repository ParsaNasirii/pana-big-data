package io.github.panabigdata.cube;

import static io.github.panabigdata.cube.CubeColumns.KAFKA_TIMESTAMP_BUCKET;
import static org.apache.spark.sql.functions.lit;

import io.github.panabigdata.BatchDirectoryService;
import io.github.panabigdata.cube.maker.BasicCubeMaker;
import io.github.panabigdata.metrics.CubeMakerMetrics;
import io.github.panabigdata.metrics.CubeMakerMetrics.Outcome;
import io.github.panabigdata.storage.CubeWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

@Slf4j
public class CubeMakerService {
  public static final int NANOS_TO_MILLIS = 1_000_000;
  private final SparkSession spark;
  private final List<BasicCubeMaker> producers;
  private final CubeWriter writer;
  private final BatchDirectoryService directories;
  private final CubeMakerMetrics metrics;
  private final CountDownLatch terminated = new CountDownLatch(1);
  private volatile boolean shutdownRequested;
  private boolean started;

  protected CubeMakerService(
      SparkSession spark,
      List<BasicCubeMaker> producers,
      CubeWriter writer,
      BatchDirectoryService directories,
      CubeMakerMetrics metrics) {
    this.spark = spark;
    this.producers = producers;
    this.writer = writer;
    this.directories = directories;
    this.metrics = metrics;
  }

  public void startProducing() throws SQLException, IOException {
    synchronized (this) {
      if (shutdownRequested) return;
      if (started) throw new IllegalStateException("Cube producer service has already started");
      started = true;
    }
    long start = System.nanoTime();
    Outcome outcome = Outcome.FAILED;
    try {
      log.info("Starting cube processing with {} producers: {}", producers.size(), producers);
      directories.process(this::processDirectory, () -> shutdownRequested);
      checkShutdown();
      outcome = Outcome.SUCCEEDED;
      log.info("Cube processing completed successfully");
    } finally {
      try {
        metrics.recordRun(outcome, System.nanoTime() - start);
      } finally {
        terminated.countDown();
      }
    }
  }

  private boolean processDirectory(String path) throws SQLException {
    checkShutdown();
    Timestamp timestamp;
    try {
      timestamp = extractKafkaTimestamp(path, directories.getTimezone());
    } catch (IllegalArgumentException exception) {
      log.warn("Could not extract date and hour from {}; skipping directory", path);
      return false;
    }
    log.info("Reading hourly data from {} for timestamp bucket {}", path, timestamp);
    Dataset<Row> hourlyData = spark.read().parquet(path);
    for (BasicCubeMaker producer : producers) {
      checkShutdown();
      produceCube(path, timestamp, hourlyData, producer);
    }
    return true;
  }

  private void produceCube(
      String path, Timestamp timestamp, Dataset<Row> hourlyData, BasicCubeMaker producer)
      throws SQLException {
    long startedAt = System.nanoTime();
    Outcome outcome = Outcome.FAILED;
    try {
      log.info("Processing cube {} from directory {}", producer, path);
      Dataset<Row> cube =
          producer
              .processDataFrame(hourlyData)
              .withColumn(KAFKA_TIMESTAMP_BUCKET, lit(timestamp).cast(DataTypes.TimestampType));
      writer.writeCube(cube, producer);
      outcome = Outcome.SUCCEEDED;
    } finally {
      long elapsed = System.nanoTime() - startedAt;
      metrics.recordCube(producer.toString(), outcome, elapsed);
      log.info(
          "Cube attempt: path={}, cube={}, outcome={}, durationMs={}",
          path,
          producer,
          outcome,
          elapsed / NANOS_TO_MILLIS);
    }
  }

  public synchronized void shutdown() {
    if (!shutdownRequested)
      log.info("Shutdown requested; no further directories or cubes will start");
    shutdownRequested = true;
    if (!started) terminated.countDown();
  }

  public boolean awaitTermination(Duration timeout) {
    try {
      return terminated.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public void cancelActiveWork() {
    shutdown();
    log.warn("Cancelling active Spark jobs");
    spark.sparkContext().cancelAllJobs();
  }

  private void checkShutdown() {
    if (shutdownRequested) throw new CancellationException("Cube processing shutdown requested");
  }

  private static Timestamp extractKafkaTimestamp(String path, String timezone) {
    Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})/(\\d{2})$");
    Matcher matcher = pattern.matcher(path);
    if (!matcher.find()) throw new IllegalArgumentException("Invalid hourly directory: " + path);
    Instant instant =
        LocalDate.parse(matcher.group(1))
            .atTime(Integer.parseInt(matcher.group(2)), 0)
            .atZone(ZoneId.of(timezone))
            .toInstant();
    return Timestamp.from(instant);
  }
}
