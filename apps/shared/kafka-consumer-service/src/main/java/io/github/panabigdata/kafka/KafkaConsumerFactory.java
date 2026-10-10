package io.github.panabigdata.kafka;

import io.github.panabigdata.config.KafkaConfig;
import io.github.panabigdata.metrics.KafkaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;

public final class KafkaConsumerFactory {

  public static KafkaConsumerService create(
      Consumer<byte[], GenericRecord> consumer,
      BatchWriter batchWriter,
      KafkaConfig config,
      MeterRegistry registry) {

    KafkaMetrics metrics = new KafkaMetrics(registry, config.getGroupId());

    RecordProcessor processor = new RecordProcessor(createInvalidRecordHandler(config), metrics);

    Duration pollTimeout = Duration.ofMillis(config.getPollTimeoutMillis());
    long retryBackoffMillis = config.getRetryBackoffMillis();
    return new KafkaConsumerService(
        consumer, batchWriter, processor, pollTimeout, retryBackoffMillis, metrics);
  }

  private static InvalidRecordHandler createInvalidRecordHandler(KafkaConfig kafkaConfig) {
    if (!kafkaConfig.isDlqEnabled()) {
      return new DropInvalidRecordHandler();
    }
    return new DlqInvalidRecordHandler(
        kafkaConfig.getDlqTopic(), kafkaConfig.getProducerProperties());
  }
}
