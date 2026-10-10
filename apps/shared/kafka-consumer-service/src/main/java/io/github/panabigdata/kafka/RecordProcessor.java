package io.github.panabigdata.kafka;

import io.github.panabigdata.metrics.KafkaMetrics;
import io.github.panabigdata.recordValidator.RecordValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public final class RecordProcessor {

  private final InvalidRecordHandler invalidRecordHandler;
  private final KafkaMetrics metrics;
  private long invalidRecordCount;

  public RecordProcessor(InvalidRecordHandler invalidRecordHandler, KafkaMetrics metrics) {
    this.invalidRecordHandler = Objects.requireNonNull(invalidRecordHandler);
    this.metrics = Objects.requireNonNull(metrics);
  }

  public List<ConsumerRecord<byte[], GenericRecord>> process(
      ConsumerRecords<byte[], GenericRecord> records) {
    metrics.recordBatchSize(records.count());

    List<ConsumerRecord<byte[], GenericRecord>> validRecords = new ArrayList<>(records.count());

    for (ConsumerRecord<byte[], GenericRecord> record : records) {
      boolean isValid = metrics.recordValidation(() -> RecordValidator.isValid(record.value()));

      if (isValid) {
        validRecords.add(record);
        metrics.recordValid(1);
      } else {
        metrics.recordInvalidHandling(() -> invalidRecordHandler.handle(record));
        invalidRecordCount++;
        metrics.recordInvalid(1);
      }
    }

    return validRecords;
  }

  public long getInvalidRecordCount() {
    return invalidRecordCount;
  }

  public void resetInvalidRecordCount() {
    invalidRecordCount = 0;
  }

  public void flushInvalidRecords() {
    invalidRecordHandler.flush();
  }

  public void close() {
    invalidRecordHandler.close();
  }
}
