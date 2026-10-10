package io.github.panabigdata.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

public final class DlqInvalidRecordHandler implements InvalidRecordHandler {

  private final String topic;

  private final Producer<byte[], GenericRecord> producer;

  private final List<Future<RecordMetadata>> pendingSends;

  public DlqInvalidRecordHandler(String topic, Properties producerProperties) {

    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("dlqTopic must be configured when DLQ is enabled");
    }

    Objects.requireNonNull(
        producerProperties, "producerProperties must not be null when DLQ is enabled");

    this.topic = topic;
    this.producer = new KafkaProducer<>(producerProperties);
    this.pendingSends = new ArrayList<>();
  }

  @Override
  public void handle(ConsumerRecord<byte[], GenericRecord> record) {

    Objects.requireNonNull(record, "record must not be null");

    pendingSends.add(producer.send(new ProducerRecord<>(topic, record.key(), record.value())));
  }

  @Override
  public void flush() {

    producer.flush();

    for (Future<RecordMetadata> pendingSend : pendingSends) {
      awaitDelivery(pendingSend);
    }

    pendingSends.clear();
  }

  private void awaitDelivery(Future<RecordMetadata> pendingSend) {

    try {

      pendingSend.get();

    } catch (InterruptedException e) {

      Thread.currentThread().interrupt();

      throw new KafkaException("Interrupted while publishing a record to " + topic, e);

    } catch (ExecutionException e) {

      throw new KafkaException("Failed to publish a record to " + topic, e.getCause());
    }
  }

  @Override
  public void close() {
    producer.close();
  }
}
