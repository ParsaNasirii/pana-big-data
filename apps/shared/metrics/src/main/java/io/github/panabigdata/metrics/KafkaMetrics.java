package io.github.panabigdata.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public final class KafkaMetrics {

  private static final String COMPONENT_TAG = "component";

  private final Counter validRecords;
  private final Counter invalidRecords;
  private final DistributionSummary batchSize;

  private final Timer validationDuration;
  private final Timer invalidHandlingDuration;
  private final Timer writeDuration;

  public KafkaMetrics(MeterRegistry registry, String component) {

    this.validRecords =
        Counter.builder("kafka_records_valid")
            .description("Number of valid Kafka records processed")
            .tag(COMPONENT_TAG, component)
            .register(registry);

    this.invalidRecords =
        Counter.builder("kafka_records_invalid")
            .description("Number of invalid Kafka records processed")
            .tag(COMPONENT_TAG, component)
            .register(registry);

    this.batchSize =
        DistributionSummary.builder("kafka_batch_size")
            .description("Number of records in a processing batch")
            .tag(COMPONENT_TAG, component)
            .register(registry);

    this.validationDuration = timer(registry, "kafka_validation_duration", component);

    this.invalidHandlingDuration = timer(registry, "kafka_invalid_handling_duration", component);

    this.writeDuration = timer(registry, "kafka_write_duration", component);
  }

  private Timer timer(MeterRegistry registry, String name, String component) {
    return Timer.builder(name)
        .description(name)
        .tag(COMPONENT_TAG, component)
        .publishPercentileHistogram()
        .register(registry);
  }

  public void recordValid(int count) {
    validRecords.increment(count);
  }

  public void recordInvalid(int count) {
    invalidRecords.increment(count);
  }

  public void recordBatchSize(int count) {
    batchSize.record(count);
  }

  public void recordInvalidHandlingTime(long millis) {
    invalidHandlingDuration.record(millis, TimeUnit.MILLISECONDS);
  }

  public void recordWriteTime(long millis) {
    writeDuration.record(millis, TimeUnit.MILLISECONDS);
  }

  public void recordValidationTime(long millis) {
    validationDuration.record(millis, TimeUnit.MILLISECONDS);
  }

  public void recordInvalidHandling(Runnable runnable) {
    invalidHandlingDuration.record(runnable);
  }

  public boolean recordValidation(BooleanSupplier supplier) {
    return validationDuration.record(supplier);
  }
}
