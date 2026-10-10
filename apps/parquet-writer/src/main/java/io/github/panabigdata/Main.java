package io.github.panabigdata;

import io.github.panabigdata.config.KafkaConfig;
import io.github.panabigdata.config.ParquetWriterConfig;
import io.github.panabigdata.hdfs.HadoopConfigSupport;
import io.github.panabigdata.kafka.KafkaConsumerFactory;
import io.github.panabigdata.kafka.KafkaConsumerService;
import io.github.panabigdata.metrics.ParquetWriterMetrics;
import io.github.panabigdata.parquet.HadoopParquetFileWriter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class Main {

  public static void main(String[] args) throws IOException {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    MetricsHttpServer metricsServer = new MetricsHttpServer(registry, 8380);
    metricsServer.start();

    KafkaConfig kafkaConfig = new KafkaConfig();
    ParquetWriterConfig parquetWriterConfig = new ParquetWriterConfig();

    System.setProperty("HADOOP_USER_NAME", parquetWriterConfig.getHdfsUserName());

    Configuration hadoopConf = new Configuration();

    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (hadoopConfDir != null) {
      HadoopConfigSupport.addSiteResources(hadoopConf, hadoopConfDir);
      log.info("Loaded Hadoop config from {}", hadoopConfDir);
    } else {
      log.warn("HADOOP_CONF_DIR not set; falling back to classpath defaults");
    }

    HadoopParquetFileWriter parquetWriter =
        new HadoopParquetFileWriter(
            parquetWriterConfig, hadoopConf, new ParquetWriterMetrics(registry));

    KafkaConsumer<byte[], GenericRecord> kafkaConsumer =
        new KafkaConsumer<>(kafkaConfig.getConsumerProperties());
    kafkaConsumer.subscribe(List.of(kafkaConfig.getTopic()));

    KafkaConsumerService kafkaConsumerService =
        KafkaConsumerFactory.create(kafkaConsumer, parquetWriter, kafkaConfig, registry);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutdown signal received. Initiating graceful shutdown...");

                  kafkaConsumerService.shutdown();

                  boolean terminatedCleanly =
                      kafkaConsumerService.awaitTermination(Duration.ofSeconds(20));

                  if (terminatedCleanly) {
                    log.info("KafkaConsumerService shut down cleanly.");
                  } else {
                    log.warn(
                        "KafkaConsumerService shutdown timed out. Proceeding to close HDFS...");
                  }

                  parquetWriter.close();

                  metricsServer.stop();

                  log.info("Application completely shut down.");
                }));

    log.info("Starting Kafka Consumer Service...");
    kafkaConsumerService.startConsuming();
  }
}
