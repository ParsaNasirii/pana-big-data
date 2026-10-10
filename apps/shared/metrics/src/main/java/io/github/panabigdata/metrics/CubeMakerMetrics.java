package io.github.panabigdata.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class CubeMakerMetrics {

  public static final String OUTCOME_TAG = "outcome";
  public static final String CUBE_TAG = "cube";
  private final MeterRegistry registry;
  private final AtomicInteger pendingDirectories = new AtomicInteger();
  private final AtomicLong lastCompletionTimestamp = new AtomicLong();
  private final AtomicLong lastSuccessTimestamp = new AtomicLong();

  public enum Outcome {
    SUCCEEDED,
    FAILED,
    SKIPPED;

    private String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public CubeMakerMetrics(MeterRegistry registry) {
    this.registry = registry;
    Gauge.builder("cube_maker_pending_directories", pendingDirectories, AtomicInteger::get)
        .description("Discovered directories not yet marked processed in this run")
        .register(registry);
    Gauge.builder(
            "cube_maker_last_completion_timestamp_seconds",
            lastCompletionTimestamp,
            AtomicLong::get)
        .description("Unix timestamp of the last completed cube maker run")
        .register(registry);
    for (Outcome outcome : Outcome.values()) {
      directoryCounter(outcome);
      bytesCounter(outcome);
    }
  }

  public void setPendingDirectories(int count) {
    pendingDirectories.set(count);
  }

  public void directoryMarkedProcessed() {
    pendingDirectories.decrementAndGet();
  }

  /** Bytes are compressed input file sizes, not actual Spark I/O; retries count again. */
  public void recordDirectory(Outcome outcome, long inputBytes, long nanos) {
    directoryCounter(outcome).increment();
    bytesCounter(outcome).increment(inputBytes);
    recordTime(
        "cube_maker_directory_duration",
        "Directory attempt including tracking update",
        outcome,
        nanos);
  }

  public void recordCube(String cube, Outcome outcome, long nanos) {
    recordTime(
        "cube_maker_cube_duration",
        "One cube attempt for one directory through final save",
        outcome,
        nanos,
        CUBE_TAG,
        cube);
  }

  public void recordStagingWrite(String cube, Outcome outcome, long nanos) {
    recordTime(
        "cube_maker_staging_write_duration", "Staging table write", outcome, nanos, CUBE_TAG, cube);
  }

  public void recordUpsert(String cube, Outcome outcome, long nanos) {
    recordTime(
        "cube_maker_upsert_duration",
        "PostgreSQL merge, staging cleanup and commit",
        outcome,
        nanos,
        CUBE_TAG,
        cube);
  }

  public void recordRowsUpserted(String cube, long rows) {
    Counter.builder("cube_maker_rows_upserted")
        .description("Rows inserted or updated by committed cube writes; retries count again")
        .tag(CUBE_TAG, cube)
        .register(registry)
        .increment(rows);
  }

  public void recordRun(Outcome outcome, long nanos) {
    recordTime(
        "cube_maker_run_duration",
        "Cube processing run duration including directory discovery",
        outcome,
        nanos);
    long completedAt = Instant.now().getEpochSecond();
    lastCompletionTimestamp.set(completedAt);
    if (outcome == Outcome.SUCCEEDED) {
      lastSuccessTimestamp.set(completedAt);
      Gauge.builder(
              "cube_maker_last_success_timestamp_seconds", lastSuccessTimestamp, AtomicLong::get)
          .description("Unix timestamp of the last successful cube maker run")
          .register(registry);
    }
  }

  private Counter directoryCounter(Outcome outcome) {
    return Counter.builder("cube_maker_directories_processed")
        .description("Completed directory attempts, including retries")
        .tag(OUTCOME_TAG, outcome.tag())
        .register(registry);
  }

  private Counter bytesCounter(Outcome outcome) {
    return Counter.builder("cube_maker_directory_input_bytes")
        .description(
            "Parquet input bytes per directory attempt, excluding replication; failed attempts include the full known input size")
        .tag(OUTCOME_TAG, outcome.tag())
        .register(registry);
  }

  private void recordTime(
      String name, String description, Outcome outcome, long nanos, String... tags) {
    Timer.builder(name)
        .description(description)
        .tags(tags)
        .tag(OUTCOME_TAG, outcome.tag())
        .publishPercentileHistogram()
        .register(registry)
        .record(nanos, TimeUnit.NANOSECONDS);
  }
}
