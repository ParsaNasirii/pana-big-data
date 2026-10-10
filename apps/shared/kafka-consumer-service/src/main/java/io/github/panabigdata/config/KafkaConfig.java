package io.github.panabigdata.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.github.panabigdata.ConfigSupport;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

@Getter
public class KafkaConfig {

  private static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";

  private final Properties properties;
  private final String topic;
  private final String dlqTopic;
  private final String bootstrapServers;
  private final String groupId;
  private final boolean enableAutoCommit;
  private final String keyDeserializer;
  private final String autoOffsetReset;
  private final String valueDeserializer;
  private final int bufferFlushIntervalMillis;
  private final long retryBackoffMillis;
  private final long pollTimeoutMillis;

  public KafkaConfig() {
    this(ConfigSupport.loadApplicationProperties(KafkaConfig.class), System.getenv());
  }

  KafkaConfig(Properties properties, Map<String, String> environment) {
    this(withRuntimeEnvironment(properties, environment));
  }

  public KafkaConfig(Properties properties) {
    this.properties = new Properties();
    this.properties.putAll(properties);

    topic = ConfigSupport.requiredProperty(properties, "kafka.topic");

    dlqTopic = ConfigSupport.optionalProperty(properties, "dlq.kafka.topic");

    bootstrapServers = ConfigSupport.requiredProperty(properties, "bootstrap.servers");
    groupId = ConfigSupport.requiredProperty(properties, "group.id");
    autoOffsetReset = ConfigSupport.requiredProperty(properties, "auto.offset.reset");
    enableAutoCommit = ConfigSupport.requiredBoolean(properties, "enable.auto.commit");
    keyDeserializer = ConfigSupport.requiredProperty(properties, "key.deserializer");
    valueDeserializer = ConfigSupport.requiredProperty(properties, "value.deserializer");

    ConfigSupport.requiredProperty(properties, "key.serializer");
    ConfigSupport.requiredProperty(properties, "value.serializer");

    if (enableAutoCommit) {
      throw new IllegalArgumentException(
          "enable.auto.commit must be false to prevent committing unwritten records");
    }

    bufferFlushIntervalMillis = ConfigSupport.positiveInt(properties, "buffer.flush.interval.ms");

    retryBackoffMillis = ConfigSupport.nonNegativeLong(properties, "consumer.retry.backoff.ms");

    pollTimeoutMillis = ConfigSupport.positiveLong(properties, "consumer.poll.timeout.ms");
  }

  private static Properties withRuntimeEnvironment(
      Properties properties, Map<String, String> environment) {

    Properties merged = ConfigSupport.copyProperties(properties);

    ConfigSupport.overrideRequiredEnvironment(
        merged, "bootstrap.servers", environment, BOOTSTRAP_SERVERS_ENV);

    return merged;
  }

  private Properties filterKafkaProperties(Set<String> allowedNames) {
    Properties filtered = new Properties();
    properties.forEach(
        (key, value) -> {
          if (allowedNames.contains(key.toString())) {
            filtered.put(key, value);
          }
        });
    return filtered;
  }

  public Properties getConsumerProperties() {
    Set<String> allowedNames = new HashSet<>(ConsumerConfig.configNames());
    allowedNames.add(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    allowedNames.add(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
    return filterKafkaProperties(allowedNames);
  }

  public Properties getProducerProperties() {
    Set<String> allowedNames = new HashSet<>(ProducerConfig.configNames());
    allowedNames.add(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    return filterKafkaProperties(allowedNames);
  }

  public boolean isDlqEnabled() {
    return dlqTopic != null;
  }
}
