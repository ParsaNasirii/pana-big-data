package io.github.panabigdata.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.metrics.ParquetWriterMetrics.IoOperation;
import io.github.panabigdata.metrics.ParquetWriterMetrics.Outcome;
import io.github.panabigdata.metrics.ParquetWriterMetrics.RotationReason;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ParquetWriterMetricsTest {

  @Test
  void exportsWriterStateAndFinalizedOutput() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      var metrics = new ParquetWriterMetrics(registry);
      metrics.onFileOpened();
      metrics.onFileOpened();
      metrics.onRecordAppended();
      metrics.onRecordAppended();
      metrics.onRecordAppended();
      metrics.onFileFinalized(2);
      metrics.recordFileSize(RotationReason.SIZE, 512);

      assertThat(registry.get("parquet_writer_buffered_records").gauge().value()).isEqualTo(1);
      assertThat(registry.get("parquet_writer_open_files").gauge().value()).isEqualTo(1);
      assertThat(registry.get("parquet_writer_records_finalized").counter().count()).isEqualTo(2);
      assertThat(
              registry
                  .get("parquet_writer_file_size_bytes")
                  .tag("reason", "size")
                  .summary()
                  .totalAmount())
          .isEqualTo(512);
      assertThat(registry.scrape())
          .contains("parquet_writer_records_finalized_total 2.0")
          .contains("parquet_writer_file_size_bytes_count{reason=\"size\"} 1");
    } finally {
      registry.close();
    }
  }

  @Test
  void exportsRotationAttemptsAndIoErrorsWithBoundedTags() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try {
      var metrics = new ParquetWriterMetrics(registry);
      metrics.recordRotation(RotationReason.TIME, Outcome.SUCCEEDED, 2_000_000_000L);
      metrics.recordRotation(RotationReason.TIME, Outcome.FAILED, 1_000_000_000L);
      metrics.recordIoError(IoOperation.RENAME);

      assertThat(
              registry
                  .get("parquet_writer_rotation_duration")
                  .tags("reason", "time", "outcome", "succeeded")
                  .timer()
                  .totalTime(TimeUnit.SECONDS))
          .isEqualTo(2);
      assertThat(
              registry
                  .get("parquet_writer_rotations")
                  .tags("reason", "time", "outcome", "failed")
                  .counter()
                  .count())
          .isEqualTo(1);
      assertThat(
              registry.get("parquet_writer_io_errors").tag("operation", "rename").counter().count())
          .isEqualTo(1);
      assertThat(
              registry
                  .get("parquet_writer_last_successful_rotation_timestamp_seconds")
                  .gauge()
                  .value())
          .isPositive();
      assertThat(registry.scrape())
          .contains("parquet_writer_rotations_total{outcome=\"failed\",reason=\"time\"} 1.0")
          .contains("parquet_writer_io_errors_total{operation=\"rename\"} 1.0");
    } finally {
      registry.close();
    }
  }
}
