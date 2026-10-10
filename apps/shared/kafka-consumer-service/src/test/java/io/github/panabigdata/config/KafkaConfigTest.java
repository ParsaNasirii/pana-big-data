package io.github.panabigdata.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaConfigTest {

  @Test
  void shouldSeparateConsumerAndProducerProperties() {
    KafkaConfig config = new KafkaConfig(validProperties());

    assertThat(config.getConsumerProperties())
        .containsKeys("max.poll.records", "max.partition.fetch.bytes", "max.poll.interval.ms")
        .containsEntry("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
        .containsEntry("schema.registry.url", "http://schema-registry:8081")
        .containsEntry("specific.avro.reader", "false")
        .doesNotContainKeys("value.serializer", "kafka.topic", "buffer.flush.interval.ms");

    assertThat(config.getProducerProperties())
        .containsEntry("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
        .containsEntry("schema.registry.url", "http://schema-registry:8081")
        .doesNotContainKeys(
            "value.deserializer",
            "kafka.topic",
            "buffer.flush.interval.ms",
            "specific.avro.reader");
  }

  @Test
  void shouldRejectMissingRequiredProperty() {
    Properties properties = validProperties();
    properties.remove("kafka.topic");

    assertThatThrownBy(() -> new KafkaConfig(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("kafka.topic");
  }

  @Test
  void shouldRejectEnabledAutoCommit() {
    Properties properties = validProperties();
    properties.setProperty("enable.auto.commit", "true");

    assertThatThrownBy(() -> new KafkaConfig(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("enable.auto.commit");
  }

  private Properties validProperties() {
    Properties properties = new Properties();

    properties.setProperty("kafka.topic", "netflow");
    properties.setProperty("dlq.kafka.topic", "netflow-invalid");
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test-group");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("enable.auto.commit", "false");
    properties.setProperty("max.poll.records", "100000");
    properties.setProperty("max.partition.fetch.bytes", "52428800");
    properties.setProperty("max.poll.interval.ms", "900000");

    properties.setProperty(
        "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.setProperty(
        "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    properties.setProperty("schema.registry.url", "http://schema-registry:8081");
    properties.setProperty("specific.avro.reader", "false");
    properties.setProperty(
        "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.setProperty(
        "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

    properties.setProperty("buffer.flush.interval.ms", "60000");
    properties.setProperty("consumer.retry.backoff.ms", "1000");
    properties.setProperty("consumer.poll.timeout.ms", "250");

    return properties;
  }
}
