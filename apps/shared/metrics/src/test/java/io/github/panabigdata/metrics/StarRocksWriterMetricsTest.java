package io.github.panabigdata.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.metrics.StarRocksWriterMetrics.Outcome;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class StarRocksWriterMetricsTest {

  @Test
  void exportsLoadStateAndOutcomes() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      var metrics = new StarRocksWriterMetrics(registry);
      metrics.setPendingRows(3);
      metrics.recordLoad(Outcome.RETRY, 1_000_000_000L);
      metrics.recordLoad(Outcome.SUCCESS, 2_000_000_000L);
      metrics.recordSuccessfulLoad(3);

      assertThat(registry.get("starrocks_writer_pending_rows").gauge().value()).isZero();
      assertThat(registry.get("starrocks_writer_rows_written").counter().count()).isEqualTo(3);
      assertThat(registry.get("starrocks_writer_loads").tag("outcome", "retry").counter().count())
          .isEqualTo(1);
      assertThat(
              registry
                  .get("starrocks_writer_load_duration")
                  .tag("outcome", "success")
                  .timer()
                  .totalTime(TimeUnit.SECONDS))
          .isEqualTo(2);
      assertThat(
              registry
                  .get("starrocks_writer_last_successful_load_timestamp_seconds")
                  .gauge()
                  .value())
          .isPositive();
      assertThat(registry.scrape())
          .contains("starrocks_writer_loads_total{outcome=\"success\"} 1.0")
          .contains("starrocks_writer_rows_written_total 3.0");
    } finally {
      registry.close();
    }
  }

  @Test
  void exportsFrontendAttemptsAndFailovers() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      var metrics = new StarRocksWriterMetrics(registry);
      metrics.recordStreamLoadAttempt();
      metrics.recordStreamLoadAttempt();
      metrics.recordFeFailover();

      assertThat(registry.get("starrocks_writer_stream_load_attempts").counter().count())
          .isEqualTo(2);
      assertThat(registry.get("starrocks_writer_fe_failovers").counter().count()).isEqualTo(1);
      assertThat(registry.scrape())
          .contains("starrocks_writer_stream_load_attempts_total 2.0")
          .contains("starrocks_writer_fe_failovers_total 1.0");
    } finally {
      registry.close();
    }
  }
}
