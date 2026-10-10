package io.github.panabigdata.parquet;

import io.github.panabigdata.config.ParquetWriterConfig;
import io.github.panabigdata.kafka.BatchWriter;
import io.github.panabigdata.kafka.WriteResult;
import io.github.panabigdata.metrics.ParquetWriterMetrics;
import io.github.panabigdata.metrics.ParquetWriterMetrics.IoOperation;
import io.github.panabigdata.metrics.ParquetWriterMetrics.Outcome;
import io.github.panabigdata.metrics.ParquetWriterMetrics.RotationReason;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

@Slf4j
public class HadoopParquetFileWriter implements BatchWriter, AutoCloseable {

  private static final DateTimeFormatter PARTITION_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd/HH");

  private Schema schema;
  private String sourceSchemaForm;
  private final Set<Schema> acceptedSchemas = new HashSet<>();
  private final ParquetWriterConfig config;
  private final Configuration hadoopConf;
  private final FileSystem fs;
  private final Clock clock;
  private final ParquetWriterMetrics metrics;
  private final Duration rotationInterval;
  private final ZoneId partitionTimeZone;
  private final Map<String, OpenFile> openFiles = new LinkedHashMap<>();

  private Instant lastRotationTime;

  public HadoopParquetFileWriter(
      ParquetWriterConfig config, Configuration hadoopConf, ParquetWriterMetrics metrics) {
    this(config, hadoopConf, null, metrics);
  }

  public HadoopParquetFileWriter(
      ParquetWriterConfig config,
      Configuration hadoopConf,
      Clock clock,
      ParquetWriterMetrics metrics) {
    this.config = config;
    this.hadoopConf = hadoopConf;
    this.metrics = metrics;
    this.rotationInterval = Duration.ofMillis(config.getRotationIntervalMillis());
    this.partitionTimeZone = config.getTimeZone();
    this.clock = clock == null ? Clock.system(partitionTimeZone) : Objects.requireNonNull(clock);
    this.lastRotationTime = this.clock.instant();
    try {
      this.fs = FileSystem.get(hadoopConf);
      log.info("Successfully initialized HDFS FileSystem connection.");
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize HDFS FileSystem.", e);
    }
  }

  @Override
  public WriteResult write(List<ConsumerRecord<byte[], GenericRecord>> records) throws IOException {

    if (records == null || records.isEmpty()) {
      return rotateIfNeeded(false);
    }

    if (!acceptSchemas(records)) {
      return WriteResult.STOP;
    }

    boolean sizeThresholdReached = false;
    for (ConsumerRecord<byte[], GenericRecord> record : records) {
      String partitionPath = partitionPath(record.timestamp());
      OpenFile openFile = openFiles.get(partitionPath);
      if (openFile == null) {
        try {
          openFile = initNewWriter(partitionPath);
        } catch (IOException e) {
          metrics.recordIoError(IoOperation.CREATE);
          throw e;
        }
        openFiles.put(partitionPath, openFile);
        metrics.onFileOpened();
      }

      String recordKafkaTime = Instant.ofEpochMilli(record.timestamp()).toString();
      try {
        openFile.writer.write(createParquetRecord(record.value(), recordKafkaTime));
      } catch (IOException e) {
        metrics.recordIoError(IoOperation.APPEND);
        throw e;
      }
      openFile.recordCount++;
      metrics.onRecordAppended();
      sizeThresholdReached =
          sizeThresholdReached || openFile.writer.getDataSize() >= config.getTargetFileSizeBytes();
    }

    return rotateIfNeeded(sizeThresholdReached);
  }

  private boolean acceptSchemas(List<ConsumerRecord<byte[], GenericRecord>> records) {
    if (openFiles.isEmpty()) {
      Schema source = records.get(0).value().getSchema();
      schema = NetFlowParquetSchema.createSchema(source);
      sourceSchemaForm = SchemaNormalization.toParsingForm(source);
      acceptedSchemas.clear();
      acceptedSchemas.add(source);
    }
    for (ConsumerRecord<byte[], GenericRecord> record : records) {
      Schema source = record.value().getSchema();
      if (!acceptedSchemas.contains(source)) {
        if (!sourceSchemaForm.equals(SchemaNormalization.toParsingForm(source))) {
          log.error("NetFlow schema changed structurally; stopping without writing this batch.");
          return false;
        }
        acceptedSchemas.add(source);
      }
    }
    return true;
  }

  private OpenFile initNewWriter(String partitionPath) throws IOException {
    String fileName = config.generateFileName();

    Path finalPath = new Path(config.getBasePath(), partitionPath + "/" + fileName);
    Path tempPath =
        new Path(config.getBasePath(), "_temporary/" + partitionPath + "/" + fileName + ".tmp");

    fs.mkdirs(tempPath.getParent());
    OutputFile outputFile = HadoopOutputFile.fromPath(tempPath, hadoopConf);
    CompressionCodecName codec = CompressionCodecName.valueOf(config.getCompressionCodec());

    if (codec == CompressionCodecName.ZSTD) {
      hadoopConf.setLong("parquet.compression.codec.zstd.level", config.getCompressionLevel());
    }

    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(outputFile)
            .withSchema(schema)
            .withDataModel(GenericData.get())
            .withCompressionCodec(codec)
            .withConf(hadoopConf)
            .build();

    log.debug("Opened new temporary file for partition: {}", tempPath);
    return new OpenFile(writer, tempPath, finalPath);
  }

  private WriteResult rotateIfNeeded(boolean sizeThresholdReached) throws IOException {
    if (openFiles.isEmpty()) {
      return WriteResult.SUCCESS_NO_COMMIT;
    }

    boolean timeThresholdReached =
        !clock.instant().isBefore(lastRotationTime.plus(rotationInterval));
    if (!sizeThresholdReached && !timeThresholdReached) {
      return WriteResult.SUCCESS_NO_COMMIT;
    }

    log.info(
        "Rotating {} open Parquet file(s). Reason: {}",
        openFiles.size(),
        sizeThresholdReached ? "size threshold" : "configured rotation interval");
    rotateAllFiles(sizeThresholdReached ? RotationReason.SIZE : RotationReason.TIME);
    return WriteResult.SUCCESS;
  }

  private String partitionPath(long timestamp) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), partitionTimeZone)
        .format(PARTITION_FORMATTER);
  }

  private void rotateAllFiles(RotationReason reason) throws IOException {
    long startedAt = System.nanoTime();
    Outcome outcome = Outcome.FAILED;
    try {
      for (OpenFile openFile : openFiles.values()) {
        try {
          openFile.writer.close();
        } catch (IOException e) {
          metrics.recordIoError(IoOperation.CLOSE);
          throw e;
        }

        try {
          fs.mkdirs(openFile.finalPath.getParent());
        } catch (IOException e) {
          metrics.recordIoError(IoOperation.CREATE);
          throw e;
        }

        boolean renamed;
        try {
          renamed = fs.rename(openFile.tempPath, openFile.finalPath);
        } catch (IOException e) {
          metrics.recordIoError(IoOperation.RENAME);
          throw e;
        }
        if (!renamed) {
          metrics.recordIoError(IoOperation.RENAME);
          throw new IOException(
              "Failed to rename temporary file to final path: " + openFile.finalPath);
        }

        metrics.onFileFinalized(openFile.recordCount);
        try {
          metrics.recordFileSize(reason, fs.getFileStatus(openFile.finalPath).getLen());
        } catch (IOException e) {
          metrics.recordIoError(IoOperation.STAT);
          log.warn("Could not read finalized Parquet file size: {}", openFile.finalPath, e);
        }
        log.info("Successfully finalized Parquet file: {}", openFile.finalPath);
      }

      openFiles.clear();
      lastRotationTime = clock.instant();
      outcome = Outcome.SUCCEEDED;
    } finally {
      metrics.recordRotation(reason, outcome, System.nanoTime() - startedAt);
    }
  }

  private GenericRecord createParquetRecord(GenericRecord sourceRecord, String kafkaTimestamp) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : sourceRecord.getSchema().getFields()) {
      Schema.Field targetField = schema.getField(field.name());
      if (targetField != null) {
        builder.set(targetField, sourceRecord.get(field.name()));
      }
    }
    builder.set(NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD, kafkaTimestamp);
    return builder.build();
  }

  @Override
  public void close() {
    try {
      if (!openFiles.isEmpty()) {
        rotateAllFiles(RotationReason.SHUTDOWN);
      }
    } catch (IOException e) {
      log.error("Error finalizing files during shutdown.", e);
    }
  }

  private static final class OpenFile {
    private final ParquetWriter<GenericRecord> writer;
    private final Path tempPath;
    private final Path finalPath;
    private long recordCount;

    private OpenFile(ParquetWriter<GenericRecord> writer, Path tempPath, Path finalPath) {
      this.writer = writer;
      this.tempPath = tempPath;
      this.finalPath = finalPath;
    }
  }
}
