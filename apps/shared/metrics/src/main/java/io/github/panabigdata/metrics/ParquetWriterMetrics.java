package io.github.panabigdata.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ParquetWriterMetrics {

  public static final String OPERATION_TAG = "operation";
  public static final String OUTCOME_TAG = "outcome";
  public static final String REASON_TAG = "reason";
  private final MeterRegistry registry;
  private final Counter recordsFinalized;
  private final AtomicLong bufferedRecords = new AtomicLong();
  private final AtomicInteger openFiles = new AtomicInteger();
  private final AtomicLong lastSuccessfulRotationTimestamp = new AtomicLong();

  public enum IoOperation {
    CREATE,
    APPEND,
    CLOSE,
    RENAME,
    STAT;

    private String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public enum Outcome {
    SUCCEEDED,
    FAILED;

    private String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public enum RotationReason {
    SIZE,
    TIME,
    SHUTDOWN;

    private String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public ParquetWriterMetrics(MeterRegistry registry) {
    this.registry = registry;
    recordsFinalized =
        Counter.builder("parquet_writer_records_finalized")
            .description("Records contained in successfully finalized Parquet files")
            .register(registry);
    Gauge.builder("parquet_writer_buffered_records", bufferedRecords, AtomicLong::get)
        .description("Records currently buffered in open Parquet files")
        .register(registry);
    Gauge.builder("parquet_writer_open_files", openFiles, AtomicInteger::get)
        .description("Parquet files currently open for writing")
        .register(registry);
    Gauge.builder(
            "parquet_writer_last_successful_rotation_timestamp_seconds",
            lastSuccessfulRotationTimestamp,
            AtomicLong::get)
        .description("Unix timestamp of the last successful file rotation")
        .register(registry);

    for (RotationReason reason : RotationReason.values()) {
      fileSize(reason);
      for (Outcome outcome : Outcome.values()) {
        rotationCounter(reason, outcome);
        rotationTimer(reason, outcome);
      }
    }
    for (IoOperation operation : IoOperation.values()) {
      ioErrorCounter(operation);
    }
  }

  public void onFileOpened() {
    openFiles.incrementAndGet();
  }

  public void onRecordAppended() {
    bufferedRecords.incrementAndGet();
  }

  public void onFileFinalized(long records) {
    recordsFinalized.increment(records);
    bufferedRecords.addAndGet(-records);
    openFiles.decrementAndGet();
  }

  public void recordFileSize(RotationReason reason, long bytes) {
    fileSize(reason).record(bytes);
  }

  public void recordRotation(RotationReason reason, Outcome outcome, long durationNanoseconds) {
    rotationCounter(reason, outcome).increment();
    rotationTimer(reason, outcome).record(durationNanoseconds, TimeUnit.NANOSECONDS);
    if (outcome == Outcome.SUCCEEDED) {
      lastSuccessfulRotationTimestamp.set(Instant.now().getEpochSecond());
    }
  }

  public void recordIoError(IoOperation operation) {
    ioErrorCounter(operation).increment();
  }

  private DistributionSummary fileSize(RotationReason reason) {
    return DistributionSummary.builder("parquet_writer_file_size_bytes")
        .description("Finalized Parquet file size")
        .tag(REASON_TAG, reason.tag())
        .publishPercentileHistogram()
        .register(registry);
  }

  private Counter rotationCounter(RotationReason reason, Outcome outcome) {
    return Counter.builder("parquet_writer_rotations")
        .description("Parquet file rotation attempts")
        .tag(REASON_TAG, reason.tag())
        .tag(OUTCOME_TAG, outcome.tag())
        .register(registry);
  }

  private Timer rotationTimer(RotationReason reason, Outcome outcome) {
    return Timer.builder("parquet_writer_rotation_duration")
        .description("Parquet file rotation attempt duration")
        .tag(REASON_TAG, reason.tag())
        .tag(OUTCOME_TAG, outcome.tag())
        .publishPercentileHistogram()
        .register(registry);
  }

  private Counter ioErrorCounter(IoOperation operation) {
    return Counter.builder("parquet_writer_io_errors")
        .description("Parquet writer I/O errors")
        .tag(OPERATION_TAG, operation.tag())
        .register(registry);
  }
}
