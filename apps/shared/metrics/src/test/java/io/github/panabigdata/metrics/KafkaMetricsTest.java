package io.github.panabigdata.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class KafkaMetricsTest {

  @Test
  void recordsValidAndInvalidRecords() {

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    KafkaMetrics metrics = new KafkaMetrics(registry, "test-consumer");

    metrics.recordValid(10);
    metrics.recordInvalid(3);

    assertThat(registry.counter("kafka_records_valid", "component", "test-consumer").count())
        .isEqualTo(10);

    assertThat(registry.counter("kafka_records_invalid", "component", "test-consumer").count())
        .isEqualTo(3);
  }

  @Test
  void recordsBatchSize() {

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    KafkaMetrics metrics = new KafkaMetrics(registry, "test");

    metrics.recordBatchSize(50);

    assertThat(registry.summary("kafka_batch_size", "component", "test").totalAmount())
        .isEqualTo(50);
  }

  @Test
  void recordsValidationDuration() {

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    KafkaMetrics metrics = new KafkaMetrics(registry, "test");

    metrics.recordValidationTime(100);

    Timer timer = registry.timer("kafka_validation_duration", "component", "test");

    assertThat(timer.count()).isEqualTo(1);

    assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(100);
  }

  @Test
  void measuresValidationSupplier() {

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    KafkaMetrics metrics = new KafkaMetrics(registry, "test");

    AtomicBoolean called = new AtomicBoolean(false);

    boolean result =
        metrics.recordValidation(
            () -> {
              called.set(true);
              return true;
            });

    assertThat(result).isTrue();
    assertThat(called).isTrue();

    Timer timer = registry.timer("kafka_validation_duration", "component", "test");

    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  void recordsInvalidHandlingTime() {

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    KafkaMetrics metrics = new KafkaMetrics(registry, "test");

    metrics.recordInvalidHandlingTime(20);

    assertThat(registry.timer("kafka_invalid_handling_duration", "component", "test").count())
        .isEqualTo(1);
  }
}
