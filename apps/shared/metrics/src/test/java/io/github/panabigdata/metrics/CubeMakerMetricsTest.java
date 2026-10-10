package io.github.panabigdata.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.metrics.CubeMakerMetrics.Outcome;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CubeMakerMetricsTest {
  @Test
  void exportsAttemptsAndBytesByOutcomeWithoutDirectoryTags() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      var metrics = new CubeMakerMetrics(registry);
      metrics.setPendingDirectories(2);
      metrics.recordDirectory(Outcome.FAILED, 100, 2_000_000_000L);
      metrics.recordDirectory(Outcome.SUCCEEDED, 100, 3_000_000_000L);
      metrics.directoryMarkedProcessed();
      metrics.recordDirectory(Outcome.SKIPPED, 20, 1_000_000L);

      assertThat(registry.get("cube_maker_pending_directories").gauge().value()).isEqualTo(1);
      assertThat(
              registry
                  .get("cube_maker_directory_duration")
                  .tag("outcome", "failed")
                  .timer()
                  .totalTime(TimeUnit.SECONDS))
          .isEqualTo(2);
      assertThat(registry.scrape())
          .contains("cube_maker_directories_processed_total{outcome=\"succeeded\"} 1.0")
          .contains("cube_maker_directories_processed_total{outcome=\"failed\"} 1.0")
          .contains("cube_maker_directory_input_bytes_total{outcome=\"failed\"} 100.0")
          .contains("cube_maker_directory_input_bytes_total{outcome=\"succeeded\"} 100.0")
          .contains("cube_maker_directories_processed_total{outcome=\"skipped\"} 1.0")
          .doesNotContain("directory=");
    } finally {
      registry.close();
    }
  }

  @Test
  void recordsEachCubeAttemptAndStageSeparately() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      var metrics = new CubeMakerMetrics(registry);
      metrics.recordCube("agg_ip_traffic", Outcome.SUCCEEDED, 1_000_000_000L);
      metrics.recordCube("agg_ip_traffic", Outcome.SUCCEEDED, 2_000_000_000L);
      metrics.recordCube("agg_protocol_usage", Outcome.FAILED, 3_000_000_000L);
      metrics.recordStagingWrite("agg_protocol_usage", Outcome.FAILED, 1_000_000_000L);
      metrics.recordUpsert("agg_ip_traffic", Outcome.SUCCEEDED, 1_000_000_000L);
      metrics.recordRowsUpserted("agg_ip_traffic", 7);
      metrics.recordRowsUpserted("agg_ip_traffic", 8);
      metrics.recordRun(Outcome.FAILED, 6_000_000_000L);

      var timer =
          registry
              .get("cube_maker_cube_duration")
              .tags("cube", "agg_ip_traffic", "outcome", "succeeded")
              .timer();
      assertThat(timer.count()).isEqualTo(2);
      assertThat(timer.totalTime(TimeUnit.SECONDS)).isEqualTo(3);
      assertThat(
              registry
                  .get("cube_maker_cube_duration")
                  .tags("cube", "agg_protocol_usage", "outcome", "failed")
                  .timer()
                  .count())
          .isEqualTo(1);
      assertThat(
              registry
                  .get("cube_maker_staging_write_duration")
                  .tag("outcome", "failed")
                  .timer()
                  .count())
          .isEqualTo(1);
      assertThat(registry.get("cube_maker_upsert_duration").timer().count()).isEqualTo(1);
      assertThat(registry.get("cube_maker_rows_upserted").counter().count()).isEqualTo(15);
      assertThat(
              registry
                  .get("cube_maker_run_duration")
                  .tag("outcome", "failed")
                  .timer()
                  .totalTime(TimeUnit.SECONDS))
          .isEqualTo(6);
    } finally {
      registry.close();
    }
  }

  @Test
  void recordsCompletionAndOnlyRecordsSuccessForSuccessfulRuns() {
    var failedRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var succeededRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      new CubeMakerMetrics(failedRegistry).recordRun(Outcome.FAILED, 1);
      new CubeMakerMetrics(succeededRegistry).recordRun(Outcome.SUCCEEDED, 1);

      assertThat(failedRegistry.get("cube_maker_last_completion_timestamp_seconds").gauge().value())
          .isPositive();
      assertThat(failedRegistry.find("cube_maker_last_success_timestamp_seconds").gauge()).isNull();
      assertThat(succeededRegistry.get("cube_maker_last_success_timestamp_seconds").gauge().value())
          .isPositive();
    } finally {
      failedRegistry.close();
      succeededRegistry.close();
    }
  }
}
