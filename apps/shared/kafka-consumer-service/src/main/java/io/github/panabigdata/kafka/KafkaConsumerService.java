package io.github.panabigdata.kafka;

import io.github.panabigdata.metrics.KafkaMetrics;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
public class KafkaConsumerService {

  private final Consumer<byte[], GenericRecord> consumer;
  private final RecordProcessor recordProcessor;
  private final BatchWriter batchWriter;
  private final Duration pollTimeout;
  private final long retryBackoffMillis;

  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private final KafkaMetrics metrics;

  private volatile boolean running = true;

  protected KafkaConsumerService(
      Consumer<byte[], GenericRecord> consumer,
      BatchWriter batchWriter,
      RecordProcessor recordProcessor,
      Duration pollTimeout,
      long retryBackoffMillis,
      KafkaMetrics metrics) {
    this.consumer = Objects.requireNonNull(consumer);
    this.recordProcessor = Objects.requireNonNull(recordProcessor);
    this.batchWriter = Objects.requireNonNull(batchWriter);
    this.pollTimeout = Objects.requireNonNull(pollTimeout);
    this.retryBackoffMillis = retryBackoffMillis;
    this.metrics = Objects.requireNonNull(metrics);
  }

  public void startConsuming() {
    try {
      while (running) {
        consumeOnce();
      }
    } catch (WakeupException e) {
      if (running) {
        throw e;
      }
    } finally {
      try {
        closeResources();
      } finally {
        terminationLatch.countDown();
      }
    }
  }

  void consumeOnce() {
    ConsumerRecords<byte[], GenericRecord> records = consumer.poll(pollTimeout);
    List<ConsumerRecord<byte[], GenericRecord>> validRecords = recordProcessor.process(records);

    boolean retryNeeded = true;

    while (retryNeeded && running) {
      try {
        WriteResult result;
        long writeStartNanos = System.nanoTime();
        try {
          result =
              Objects.requireNonNull(
                  batchWriter.write(validRecords), "bqatchWriter must not return null");
        } finally {
          metrics.recordWriteTime(
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeStartNanos));
        }

        switch (result) {
          case SUCCESS -> {
            commitOffsets();
            retryNeeded = false;
          }

          case SUCCESS_NO_COMMIT -> retryNeeded = false;

          case RETRY -> pauseBeforeRetry();

          case STOP -> {
            log.error("Batch writer requested stop; Kafka offsets were not committed");
            running = false;
            retryNeeded = false;
          }
        }

      } catch (IOException e) {
        log.error("Batch writer failed; retrying the same polled records", e);
        pauseBeforeRetry();
      }
    }
  }

  private void commitOffsets() {
    recordProcessor.flushInvalidRecords();
    try {
      consumer.commitSync();
    } catch (WakeupException e) {
      if (running) {
        throw e;
      }
      consumer.commitSync();
    }
    logInvalidRecords();
    recordProcessor.resetInvalidRecordCount();
  }

  private void logInvalidRecords() {
    long count = recordProcessor.getInvalidRecordCount();
    if (count > 0) {
      log.info("{} invalid records processed", count);
    }
  }

  private void pauseBeforeRetry() {
    try {
      Thread.sleep(retryBackoffMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running = false;
    }
  }

  public void shutdown() {
    running = false;
    consumer.wakeup();
  }

  public boolean awaitTermination(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout must not be null");
    try {
      return terminationLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void closeResources() {
    try {
      recordProcessor.close();
    } finally {
      consumer.close();
    }
  }
}
