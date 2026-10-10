package io.github.panabigdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.panabigdata.config.KafkaConfig;
import io.github.panabigdata.recordValidator.RecordValidator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.utility.DockerImageName;

class KafkaIntegrationTest {

  private static final int POLL_SIZE = 10;
  private static final int BATCH_SIZE = 10;
  private static final int MIN_WRITE_SIZE = 5;
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String KAFKA_IMAGE =
      System.getProperty(
          "test.kafka.image",
          System.getenv().getOrDefault("TEST_KAFKA_IMAGE", "apache/kafka-native:3.8.0"));
  private static final String SCHEMA_REGISTRY_IMAGE =
      System.getProperty(
          "test.schema.registry.image",
          System.getenv()
              .getOrDefault("TEST_SCHEMA_REGISTRY_IMAGE", "confluentinc/cp-schema-registry:8.0.2"));

  @RegisterExtension
  private static final KafkaSchemaRegistryExtension KAFKA =
      new KafkaSchemaRegistryExtension(
          DockerImageName.parse(KAFKA_IMAGE), DockerImageName.parse(SCHEMA_REGISTRY_IMAGE));

  private static KafkaConsumerService createService(
      KafkaConsumer<byte[], GenericRecord> consumer, BatchWriter writer, KafkaConfig config) {

    MeterRegistry registry = new SimpleMeterRegistry();

    return KafkaConsumerFactory.create(consumer, writer, config, registry);
  }

  private static void stop(KafkaConsumerService service) {
    service.shutdown();
    service.startConsuming();
    assertThat(service.awaitTermination(TIMEOUT)).isTrue();
  }

  private static KafkaConsumer<byte[], GenericRecord> subscribedConsumer(
      KafkaConfig config, String topic) {
    KafkaConsumer<byte[], GenericRecord> consumer = newConsumer(config);
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  private static KafkaConsumer<byte[], GenericRecord> newConsumer(KafkaConfig config) {
    return new KafkaConsumer<>(config.getConsumerProperties());
  }

  private static Long committedOffset(
      KafkaConsumer<byte[], GenericRecord> consumer, TopicPartition partition) {
    OffsetAndMetadata offset = consumer.committed(partition, TIMEOUT);
    return offset == null ? null : offset.offset();
  }

  private static List<Long> validOffsets(int start, int end) {
    List<Long> offsets = new ArrayList<>();

    for (int offset = start; offset < end; offset++) {
      if (offset % POLL_SIZE != POLL_SIZE - 1) {
        offsets.add((long) offset);
      }
    }

    return offsets;
  }

  private static List<Long> validOffsets(int[] invalidCounts) {
    List<Long> validOffsets = new ArrayList<>();

    for (int block = 0; block < invalidCounts.length; block++) {
      int startOffset = block * POLL_SIZE;
      int validCount = POLL_SIZE - invalidCounts[block];
      validOffsets.addAll(offsets(startOffset, startOffset + validCount));
    }

    return validOffsets;
  }

  private static List<Long> offsets(int start, int end) {
    List<Long> offsets = new ArrayList<>();

    for (int offset = start; offset < end; offset++) {
      offsets.add((long) offset);
    }

    return offsets;
  }

  private static List<Integer> invalidKeys(int[] invalidCounts) {
    List<Integer> keys = new ArrayList<>();

    for (int block = 0; block < invalidCounts.length; block++) {
      int blockEnd = (block + 1) * POLL_SIZE;
      int firstInvalidKey = blockEnd - invalidCounts[block];

      for (int key = firstInvalidKey; key < blockEnd; key++) {
        keys.add(key);
      }
    }

    return keys;
  }

  private static void publishBlock(String topic, int startOffset, int validCount, int invalidCount)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<GenericRecord> records = new ArrayList<>(RandomRecordGenerator.generate(validCount, true));
    records.addAll(RandomRecordGenerator.generate(invalidCount, false));
    List<Future<RecordMetadata>> sends = new ArrayList<>();

    try (KafkaProducer<byte[], GenericRecord> producer =
        new KafkaProducer<>(producerProperties())) {
      for (int index = 0; index < records.size(); index++) {
        int key = startOffset + index;
        sends.add(
            producer.send(
                new ProducerRecord<>(topic, new byte[] {(byte) key}, records.get(index))));
      }

      for (Future<RecordMetadata> send : sends) {
        send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }
    }
  }

  private static List<ConsumerRecord<byte[], GenericRecord>> readRecords(
      String topic, int expectedCount) {
    TopicPartition partition = new TopicPartition(topic, 0);
    List<ConsumerRecord<byte[], GenericRecord>> result = new ArrayList<>();

    try (KafkaConsumer<byte[], GenericRecord> consumer =
        new KafkaConsumer<>(consumerProperties("reader"))) {
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      long deadline = System.nanoTime() + TIMEOUT.toNanos();

      while (result.size() < expectedCount && System.nanoTime() < deadline) {
        ConsumerRecords<byte[], GenericRecord> records = consumer.poll(Duration.ofMillis(200));
        records.forEach(result::add);
      }

      assertThat(consumer.endOffsets(List.of(partition)).get(partition))
          .isEqualTo((long) expectedCount);
    }

    assertThat(result).hasSize(expectedCount);
    return result;
  }

  private static void createTopics(String... names)
      throws ExecutionException, InterruptedException, TimeoutException {
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      List<NewTopic> topics =
          Stream.of(names).map(name -> new NewTopic(name, 1, (short) 1)).toList();
      admin.createTopics(topics).all().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
  }

  private static Properties producerProperties() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    properties.put("schema.registry.url", KAFKA.getSchemaRegistryUrl());
    return properties;
  }

  private static Properties consumerProperties(String groupId) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(POLL_SIZE));
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    properties.setProperty("schema.registry.url", KAFKA.getSchemaRegistryUrl());
    properties.setProperty("specific.avro.reader", "false");
    return properties;
  }

  private static KafkaConfig kafkaConfig(String topic, String dlqTopic, String groupId) {
    Properties properties = consumerProperties(groupId);
    properties.setProperty("kafka.topic", topic);

    if (dlqTopic != null) {
      properties.setProperty("dlq.kafka.topic", dlqTopic);
    }

    properties.setProperty("key.serializer", ByteArraySerializer.class.getName());
    properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
    properties.setProperty("buffer.flush.interval.ms", "1");
    properties.setProperty("consumer.retry.backoff.ms", "0");
    properties.setProperty("consumer.poll.timeout.ms", "1000");
    return new KafkaConfig(properties);
  }

  @Test
  @DisplayName(
      "Retries small polls, alternates commits, routes invalid records to DLQ, and stops safely")
  void flow() throws Exception {
    String sourceTopic = "flow-source";
    String dlqTopic = "flow-dlq";
    String groupId = "flow-group";
    TopicPartition partition = new TopicPartition(sourceTopic, 0);
    createTopics(sourceTopic, dlqTopic);

    KafkaConfig config = kafkaConfig(sourceTopic, dlqTopic, groupId);
    KafkaConsumer<byte[], GenericRecord> consumer = newConsumer(config);
    consumer.assign(List.of(partition));
    consumer.seek(partition, 0L);

    BatchingWriter writer = new BatchingWriter(BATCH_SIZE, true);
    KafkaConsumerService service = createService(consumer, writer, config);

    try {
      List<Long> committedOffsets = new ArrayList<>();
      int[] invalidCounts = {7, 6, 5, 4, 3};

      for (int block = 0; block < invalidCounts.length; block++) {
        int invalidCount = invalidCounts[block];
        publishBlock(sourceTopic, block * POLL_SIZE, POLL_SIZE - invalidCount, invalidCount);
        service.consumeOnce();

        assertThat(consumer.position(partition)).isEqualTo((block + 1L) * POLL_SIZE);
        committedOffsets.add(committedOffset(consumer, partition));
      }
      assertThat(committedOffsets).containsExactly((Long) null, null, 30L, 30L, 50L);
      assertThat(writer.results())
          .containsExactly(
              WriteResult.RETRY,
              WriteResult.SUCCESS_NO_COMMIT,
              WriteResult.RETRY,
              WriteResult.SUCCESS_NO_COMMIT,
              WriteResult.SUCCESS,
              WriteResult.SUCCESS_NO_COMMIT,
              WriteResult.SUCCESS);
      assertThat(writer.attempts().get(0)).isEqualTo(writer.attempts().get(1));
      assertThat(writer.attempts().get(2)).isEqualTo(writer.attempts().get(3));
      assertThat(writer.persistedOffsets()).containsExactlyElementsOf(validOffsets(invalidCounts));
      assertThat(writer.bufferedOffsets()).isEmpty();

      writer.stopOnNextWrite();
      publishBlock(sourceTopic, POLL_SIZE * invalidCounts.length, POLL_SIZE, 0);
      service.consumeOnce();

      assertThat(writer.results()).endsWith(WriteResult.STOP);
      assertThat(writer.attempts()).hasSize(8);
      assertThat(consumer.position(partition)).isEqualTo(60L);
      assertThat(committedOffset(consumer, partition)).isEqualTo(50L);
      assertThat(writer.persistedOffsets()).containsExactlyElementsOf(validOffsets(invalidCounts));
      assertThat(writer.bufferedOffsets()).isEmpty();

      List<ConsumerRecord<byte[], GenericRecord>> dlqRecords =
          readRecords(dlqTopic, Arrays.stream(invalidCounts).sum());
      assertThat(dlqRecords)
          .extracting(record -> (int) record.key()[0])
          .containsExactlyElementsOf(invalidKeys(invalidCounts));
      assertThat(dlqRecords).allMatch(record -> !RecordValidator.isValid(record.value()));
    } finally {
      stop(service);
    }
  }

  @Test
  @DisplayName("Replays an uncommitted batch after the consumer restarts")
  void restart() throws ExecutionException, InterruptedException, TimeoutException {
    String sourceTopic = "restart-source";
    String groupId = "restart-group";
    TopicPartition partition = new TopicPartition(sourceTopic, 0);
    createTopics(sourceTopic);

    KafkaConfig config = kafkaConfig(sourceTopic, null, groupId);
    BatchingWriter firstWriter = new BatchingWriter(BATCH_SIZE, false);
    KafkaConsumerService firstService =
        createService(subscribedConsumer(config, sourceTopic), firstWriter, config);

    publishBlock(sourceTopic, 0, POLL_SIZE - 1, 1);
    firstService.consumeOnce();

    firstWriter.stopOnNextWrite();
    publishBlock(sourceTopic, POLL_SIZE, POLL_SIZE - 1, 1);
    firstService.startConsuming();

    assertThat(firstService.awaitTermination(TIMEOUT)).isTrue();
    assertThat(firstWriter.results())
        .containsExactly(WriteResult.SUCCESS_NO_COMMIT, WriteResult.STOP);
    assertThat(firstWriter.bufferedOffsets()).containsExactlyElementsOf(validOffsets(0, 10));

    BatchingWriter secondWriter = new BatchingWriter(BATCH_SIZE, false);
    KafkaConsumer<byte[], GenericRecord> secondConsumer = subscribedConsumer(config, sourceTopic);
    KafkaConsumerService secondService = createService(secondConsumer, secondWriter, config);

    try {
      secondService.consumeOnce();
      secondService.consumeOnce();

      assertThat(secondWriter.results())
          .containsExactly(WriteResult.SUCCESS_NO_COMMIT, WriteResult.SUCCESS);
      assertThat(secondWriter.persistedOffsets()).containsExactlyElementsOf(validOffsets(0, 20));
      assertThat(secondWriter.bufferedOffsets()).isEmpty();
      assertThat(committedOffset(secondConsumer, partition)).isEqualTo(20L);
    } finally {
      stop(secondService);
    }
  }

  @Test
  @Timeout(30)
  @DisplayName("Shutdown during a successful write flushes invalid records and commits the batch")
  void shutdownDuringSuccessfulWriteCommits() throws Exception {
    String sourceTopic = "shutdown-success-source";
    String dlqTopic = "shutdown-success-dlq";
    TopicPartition partition = new TopicPartition(sourceTopic, 0);
    createTopics(sourceTopic, dlqTopic);
    publishBlock(sourceTopic, 0, POLL_SIZE - 1, 1);

    KafkaConfig config = kafkaConfig(sourceTopic, dlqTopic, "shutdown-success-group");
    KafkaConsumer<byte[], GenericRecord> consumer = new WakeupBeforeCommitConsumer(config);
    consumer.subscribe(List.of(sourceTopic));
    AtomicReference<KafkaConsumerService> serviceRef = new AtomicReference<>();
    BatchWriter writer =
        records -> {
          if (records.isEmpty()) {
            return WriteResult.SUCCESS_NO_COMMIT;
          }
          assertThat(records).hasSize(POLL_SIZE - 1);
          serviceRef.get().shutdown();
          return WriteResult.SUCCESS;
        };
    KafkaConsumerService service = createService(consumer, writer, config);
    serviceRef.set(service);

    service.startConsuming();

    assertThat(service.awaitTermination(TIMEOUT)).isTrue();
    assertThatThrownBy(consumer::assignment).isInstanceOf(IllegalStateException.class);
    try (KafkaConsumer<byte[], GenericRecord> verifier = newConsumer(config)) {
      assertThat(committedOffset(verifier, partition)).isEqualTo((long) POLL_SIZE);
    }
    assertThat(readRecords(dlqTopic, 1))
        .allMatch(record -> !RecordValidator.isValid(record.value()));
  }

  @ParameterizedTest
  @Timeout(30)
  @EnumSource(
      value = WriteResult.class,
      names = {"RETRY", "STOP", "SUCCESS_NO_COMMIT"})
  @DisplayName("Shutdown does not commit a batch that the writer has not accepted for commit")
  void shutdownDoesNotCommitUnacceptedBatch(WriteResult result) throws Exception {
    String sourceTopic = "shutdown-" + result.name().toLowerCase(Locale.ROOT);
    TopicPartition partition = new TopicPartition(sourceTopic, 0);
    createTopics(sourceTopic);
    publishBlock(sourceTopic, 0, POLL_SIZE, 0);

    KafkaConfig config = kafkaConfig(sourceTopic, null, sourceTopic + "-group");
    KafkaConsumer<byte[], GenericRecord> consumer = subscribedConsumer(config, sourceTopic);
    AtomicReference<KafkaConsumerService> serviceRef = new AtomicReference<>();
    BatchWriter writer =
        records -> {
          if (records.isEmpty()) {
            return WriteResult.SUCCESS_NO_COMMIT;
          }
          serviceRef.get().shutdown();
          return result;
        };
    KafkaConsumerService service = createService(consumer, writer, config);
    serviceRef.set(service);

    service.startConsuming();

    assertThat(service.awaitTermination(TIMEOUT)).isTrue();
    assertThatThrownBy(consumer::assignment).isInstanceOf(IllegalStateException.class);
    try (KafkaConsumer<byte[], GenericRecord> verifier = newConsumer(config)) {
      assertThat(committedOffset(verifier, partition)).isNull();
    }
  }

  @Test
  @Timeout(30)
  @DisplayName("A commit wakeup while still running propagates and leaves the batch uncommitted")
  void unexpectedCommitWakeupPropagates() throws Exception {
    String sourceTopic = "unexpected-wakeup-source";
    TopicPartition partition = new TopicPartition(sourceTopic, 0);
    createTopics(sourceTopic);
    publishBlock(sourceTopic, 0, POLL_SIZE, 0);

    KafkaConfig config = kafkaConfig(sourceTopic, null, "unexpected-wakeup-group");
    KafkaConsumer<byte[], GenericRecord> consumer = new WakeupBeforeCommitConsumer(config);
    consumer.subscribe(List.of(sourceTopic));
    BatchWriter writer =
        records -> {
          if (records.isEmpty()) {
            return WriteResult.SUCCESS_NO_COMMIT;
          }
          consumer.wakeup();
          return WriteResult.SUCCESS;
        };
    KafkaConsumerService service = createService(consumer, writer, config);

    assertThatThrownBy(service::startConsuming).isInstanceOf(WakeupException.class);

    assertThat(service.awaitTermination(TIMEOUT)).isTrue();
    assertThatThrownBy(consumer::assignment).isInstanceOf(IllegalStateException.class);
    try (KafkaConsumer<byte[], GenericRecord> verifier = newConsumer(config)) {
      assertThat(committedOffset(verifier, partition)).isNull();
    }
  }

  private static final class WakeupBeforeCommitConsumer
      extends KafkaConsumer<byte[], GenericRecord> {
    private volatile boolean pendingWakeup;

    private WakeupBeforeCommitConsumer(KafkaConfig config) {
      super(config.getConsumerProperties());
    }

    @Override
    public void wakeup() {
      pendingWakeup = true;
    }

    @Override
    public void commitSync() {
      if (pendingWakeup) {
        pendingWakeup = false;
        throw new WakeupException();
      }
      super.commitSync();
    }
  }

  private static final class BatchingWriter implements BatchWriter {

    private final int batchSize;
    private final List<List<Long>> attemptsOffsets = new ArrayList<>();
    private final List<Long> bufferedOffsets = new ArrayList<>();
    private final List<Long> commitedOffsets = new ArrayList<>();
    private final List<WriteResult> results = new ArrayList<>();
    private final boolean retrySmallBatches;
    private boolean retryPending;
    private boolean stopNext;

    private BatchingWriter(int batchSize, boolean retrySmallBatches) {
      this.batchSize = batchSize;
      this.retrySmallBatches = retrySmallBatches;
    }

    @Override
    public WriteResult write(List<ConsumerRecord<byte[], GenericRecord>> records)
        throws IOException {
      List<Long> offsets = records.stream().map(ConsumerRecord::offset).toList();
      attemptsOffsets.add(offsets);

      if (stopNext) {
        stopNext = false;
        results.add(WriteResult.STOP);
        return WriteResult.STOP;
      }

      if (retrySmallBatches && offsets.size() < MIN_WRITE_SIZE && !retryPending) {
        retryPending = true;
        results.add(WriteResult.RETRY);
        return WriteResult.RETRY;
      }

      retryPending = false;

      bufferedOffsets.addAll(offsets);

      WriteResult result = WriteResult.SUCCESS_NO_COMMIT;

      if (bufferedOffsets.size() >= batchSize) {
        commitedOffsets.addAll(bufferedOffsets);
        bufferedOffsets.clear();
        result = WriteResult.SUCCESS;
      }

      results.add(result);
      return result;
    }

    private List<List<Long>> attempts() {
      return attemptsOffsets;
    }

    private List<Long> bufferedOffsets() {
      return bufferedOffsets;
    }

    private List<Long> persistedOffsets() {
      return commitedOffsets;
    }

    private List<WriteResult> results() {
      return results;
    }

    private void stopOnNextWrite() {
      stopNext = true;
    }
  }
}
