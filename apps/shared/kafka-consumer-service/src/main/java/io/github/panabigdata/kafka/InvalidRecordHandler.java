package io.github.panabigdata.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface InvalidRecordHandler {
  void handle(ConsumerRecord<byte[], GenericRecord> record);

  void flush();

  void close();
}
