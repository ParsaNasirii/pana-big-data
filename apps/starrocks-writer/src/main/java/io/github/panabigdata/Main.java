package io.github.panabigdata;

import io.github.panabigdata.config.KafkaConfig;
import io.github.panabigdata.config.StarRocksConfig;
import io.github.panabigdata.kafka.KafkaConsumerFactory;
import io.github.panabigdata.kafka.KafkaConsumerService;
import io.github.panabigdata.metrics.StarRocksWriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class Main {

  public static void main(String[] args) throws IOException {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    KafkaConfig kafkaConfig = new KafkaConfig();
    StarRocksConfig starRocksConfig = new StarRocksConfig();

    KafkaConsumer<byte[], GenericRecord> kafkaConsumer =
        new KafkaConsumer<>(kafkaConfig.getConsumerProperties());
    kafkaConsumer.subscribe(List.of(kafkaConfig.getTopic()));

    StarRocksWriterMetrics metrics = new StarRocksWriterMetrics(registry);
    StarRocksStreamLoadClient streamLoadClient =
        new StarRocksStreamLoadClient(starRocksConfig, metrics);
    StarRocksWriterService writerService = new StarRocksWriterService(streamLoadClient, metrics);

    KafkaConsumerService consumerService =
        KafkaConsumerFactory.create(kafkaConsumer, writerService, kafkaConfig, registry);

    MetricsHttpServer metricsServer =
        new MetricsHttpServer(registry, starRocksConfig.getMetricsPort());

    Runtime runtime = Runtime.getRuntime();
    Thread shutdownHook =
        new Thread(
            () -> {
              consumerService.shutdown();
              if (!consumerService.awaitTermination(Duration.ofSeconds(240))) {
                log.warn("StarRocks writer did not finish shutting down within 240 seconds");
              }
            },
            "starrocks-writer-shutdown");
    runtime.addShutdownHook(shutdownHook);

    try {
      metricsServer.start();
      consumerService.startConsuming();
    } finally {
      metricsServer.stop();
      try {
        runtime.removeShutdownHook(shutdownHook);
      } catch (IllegalStateException ignored) {
      }
    }
  }
}
