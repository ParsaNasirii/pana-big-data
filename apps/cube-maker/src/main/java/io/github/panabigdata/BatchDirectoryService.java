package io.github.panabigdata;

import io.github.panabigdata.hdfs.HourlyDirectoryLock;
import io.github.panabigdata.hdfs.HourlyDirectoryService;
import io.github.panabigdata.metrics.CubeMakerMetrics;
import io.github.panabigdata.metrics.CubeMakerMetrics.Outcome;
import io.github.panabigdata.storage.DirectoryTrackingStore;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class BatchDirectoryService {

  public static final int NANOS_TO_MILLIS = 1_000_000;

  private final SparkSession spark;
  private final DirectoryTrackingStore trackingStore;
  private final String baseHdfsPath;
  private final CubeMakerMetrics metrics;
  private final String timezone;
  private final Duration directorySettlingTime;
  private final Clock clock;

  public BatchDirectoryService(
      SparkSession spark,
      DirectoryTrackingStore trackingStore,
      String baseHdfsPath,
      CubeMakerMetrics metrics,
      String timezone,
      Duration directorySettlingTime) {
    this(
        spark,
        trackingStore,
        baseHdfsPath,
        metrics,
        timezone,
        directorySettlingTime,
        Clock.systemUTC());
  }

  BatchDirectoryService(
      SparkSession spark,
      DirectoryTrackingStore trackingStore,
      String baseHdfsPath,
      CubeMakerMetrics metrics,
      String timezone,
      Duration directorySettlingTime,
      Clock clock) {
    this.metrics = metrics;
    this.spark = spark;
    this.trackingStore = trackingStore;
    this.baseHdfsPath = baseHdfsPath;
    this.timezone = timezone;
    this.directorySettlingTime = directorySettlingTime;
    this.clock = clock;
  }

  public void process(DirectoryProcessor processor) throws SQLException, IOException {
    process(processor, () -> false);
  }

  public void process(DirectoryProcessor processor, BooleanSupplier shutdownRequested)
      throws SQLException, IOException {
    checkShutdown(shutdownRequested);
    List<String> unprocessedDirs = getUnprocessedDirectories();
    metrics.setPendingDirectories(unprocessedDirs.size());

    log.info("Found {} batch directories to process", unprocessedDirs.size());
    for (int index = 0; index < unprocessedDirs.size(); index++) {
      checkShutdown(shutdownRequested);
      String dirPath = unprocessedDirs.get(index);
      log.info("Processing batch directory {}/{}: {}", index + 1, unprocessedDirs.size(), dirPath);
      try (Closeable lock = tryAcquireLock(dirPath)) {
        if (lock == null) {
          log.info("Skipping locked batch directory: {}", dirPath);
          continue;
        }
        processDirectory(processor, dirPath);
      }
    }
  }

  private void processDirectory(DirectoryProcessor processor, String dirPath)
      throws SQLException, IOException {
    long started = System.nanoTime();
    long inputBytes = 0;
    Outcome outcome = Outcome.FAILED;
    try {
      inputBytes = getInputBytes(dirPath);
      boolean processed = processor.process(dirPath);
      markDirectoryAsProcessed(dirPath);
      metrics.directoryMarkedProcessed();
      outcome = processed ? Outcome.SUCCEEDED : Outcome.SKIPPED;
      log.info("Completed batch directory: {}", dirPath);
    } catch (SQLException | IOException | RuntimeException exception) {
      log.warn("Batch directory failed processing, leaving directory unmarked: {}", dirPath);
      throw exception;
    } finally {
      long elapsed = System.nanoTime() - started;
      metrics.recordDirectory(outcome, inputBytes, elapsed);
      log.info(
          "Directory attempt: path={}, outcome={}, inputBytes={}, durationMs={}",
          dirPath,
          outcome,
          inputBytes,
          elapsed / NANOS_TO_MILLIS);
    }
  }

  Closeable tryAcquireLock(String dirPath) throws IOException {
    Path path = new Path(dirPath);
    return HourlyDirectoryLock.tryAcquire(
        path.getFileSystem(spark.sparkContext().hadoopConfiguration()), path);
  }

  long getInputBytes(String dirPath) throws IOException {
    Path path = new Path(dirPath);
    FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
    long bytes = 0;
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      if (file.getPath().getName().endsWith(".parquet")) {
        bytes += file.getLen();
      }
    }
    return bytes;
  }

  List<String> getUnprocessedDirectories() throws IOException, SQLException {
    log.info("Scanning HDFS for hourly batch directories under {}", baseHdfsPath);
    Instant cutoff = clock.instant().minus(directorySettlingTime);
    ZoneId zone = ZoneId.of(timezone);
    FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
    List<String> allHourlyDirs =
        new HourlyDirectoryService(fs)
            .findHourlyDirectories(new Path(baseHdfsPath)).stream()
                // Allow time for late writes after the entire hourly bucket has ended.
                .filter(
                    directory ->
                        !directory
                            .date()
                            .atTime(directory.hour(), 0)
                            .atZone(zone)
                            .plusHours(1)
                            .toInstant()
                            .isAfter(cutoff))
                .map(directory -> directory.path().toString())
                .toList();

    Set<String> processedDirs = trackingStore.getProcessedDirectories();
    log.debug("Loaded {} processed batch directory records", processedDirs.size());

    List<String> unprocessedDirs =
        allHourlyDirs.stream()
            .filter(dir -> !processedDirs.contains(dir))
            .sorted()
            .collect(Collectors.toList());
    log.info("Found {} unprocessed hourly batch directories", unprocessedDirs.size());
    return unprocessedDirs;
  }

  void markDirectoryAsProcessed(String path) throws SQLException {
    trackingStore.markDirectoryAsProcessed(path);
  }

  private static void checkShutdown(BooleanSupplier shutdownRequested) {
    if (shutdownRequested.getAsBoolean()) {
      throw new CancellationException("Cube processing shutdown requested");
    }
  }

  public String getTimezone() {
    return timezone;
  }

  @FunctionalInterface
  public interface DirectoryProcessor {
    boolean process(String dirPath) throws SQLException;
  }
}
