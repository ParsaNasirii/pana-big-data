package io.github.panabigdata.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class DropInvalidRecordHandler implements InvalidRecordHandler {
  @Override
  public void handle(ConsumerRecord<byte[], GenericRecord> record) {
    // Intentionally dropped.
  }

  @Override
  public void flush() {
    // Nothing to flush.
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
