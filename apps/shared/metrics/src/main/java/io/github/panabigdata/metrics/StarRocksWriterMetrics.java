package io.github.panabigdata.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class StarRocksWriterMetrics {

  public static final String OUTCOME_TAG = "outcome";

  private final MeterRegistry registry;
  private final Counter rowsWritten;
  private final Counter streamLoadAttempts;
  private final Counter feFailovers;
  private final AtomicLong pendingRows = new AtomicLong();
  private final AtomicLong lastSuccessfulLoadTimestamp = new AtomicLong();

  public enum Outcome {
    SUCCESS,
    RETRY,
    FAILED;

    private String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public StarRocksWriterMetrics(MeterRegistry registry) {
    this.registry = registry;
    rowsWritten =
        Counter.builder("starrocks_writer_rows_written")
            .description("Rows in Stream Loads that are safe for Kafka offset commit")
            .register(registry);
    streamLoadAttempts =
        Counter.builder("starrocks_writer_stream_load_attempts")
            .description("Attempts against configured StarRocks frontends")
            .register(registry);
    feFailovers =
        Counter.builder("starrocks_writer_fe_failovers")
            .description("Stream Load attempts continued on another StarRocks frontend")
            .register(registry);
    Gauge.builder("starrocks_writer_pending_rows", pendingRows, AtomicLong::get)
        .description("Rows retained in the current Stream Load batch for retry")
        .register(registry);
    Gauge.builder(
            "starrocks_writer_last_successful_load_timestamp_seconds",
            lastSuccessfulLoadTimestamp,
            AtomicLong::get)
        .description("Unix timestamp of the last Stream Load safe for Kafka offset commit")
        .register(registry);

    for (Outcome outcome : Outcome.values()) {
      loadCounter(outcome);
      loadTimer(outcome);
    }
  }

  public void setPendingRows(long rows) {
    pendingRows.set(rows);
  }

  public void recordLoad(Outcome outcome, long durationNanoseconds) {
    loadCounter(outcome).increment();
    loadTimer(outcome).record(durationNanoseconds, TimeUnit.NANOSECONDS);
  }

  public void recordSuccessfulLoad(long rows) {
    rowsWritten.increment(rows);
    pendingRows.set(0);
    lastSuccessfulLoadTimestamp.set(Instant.now().getEpochSecond());
  }

  public void recordStreamLoadAttempt() {
    streamLoadAttempts.increment();
  }

  public void recordFeFailover() {
    feFailovers.increment();
  }

  private Counter loadCounter(Outcome outcome) {
    return Counter.builder("starrocks_writer_loads")
        .description("StarRocks Stream Load outcomes")
        .tag(OUTCOME_TAG, outcome.tag())
        .register(registry);
  }

  private Timer loadTimer(Outcome outcome) {
    return Timer.builder("starrocks_writer_load_duration")
        .description("StarRocks Stream Load attempt duration")
        .tag(OUTCOME_TAG, outcome.tag())
        .publishPercentileHistogram()
        .register(registry);
  }
}
