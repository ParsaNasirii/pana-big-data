package io.github.panabigdata.parquet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.panabigdata.config.ParquetWriterConfig;
import io.github.panabigdata.kafka.RandomRecordGenerator;
import io.github.panabigdata.kafka.WriteResult;
import io.github.panabigdata.metrics.ParquetWriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HadoopParquetFileWriterTest {

  @TempDir java.nio.file.Path tempDir;

  private ParquetWriterConfig config;
  private Configuration hadoopConf;
  private HadoopParquetFileWriter writer;
  private MutableClock clock;
  private PrometheusMeterRegistry registry;
  private ParquetWriterMetrics metrics;

  @BeforeEach
  void setUp() {
    config = new ParquetWriterConfig(validProperties("UNCOMPRESSED"), mockEnvironment());

    hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    clock = new MutableClock(Instant.parse("2026-09-29T00:00:00Z"));
    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new ParquetWriterMetrics(registry);
    writer = new HadoopParquetFileWriter(config, hadoopConf, clock, metrics);
  }

  @AfterEach
  void tearDown() {
    if (writer != null) {
      writer.close();
    }
    registry.close();
  }

  @Test
  void testWriteEmptyBuffer() throws Exception {
    WriteResult result = writer.write(List.of());

    assertEquals(WriteResult.SUCCESS_NO_COMMIT, result);
    try (var paths = Files.walk(tempDir)) {
      assertEquals(0, paths.filter(Files::isRegularFile).count());
    }
  }

  @Test
  void testWriteAndGracefulClose() throws Exception {
    GenericRecord sourceRecord = RandomRecordGenerator.generate(1, true).get(0);
    sourceRecord.put("IPV4_SRC_ADDR", "10.0.0.1");
    sourceRecord.put("IPV4_DST_ADDR", "10.0.0.2");
    ConsumerRecord<byte[], GenericRecord> consumerRecord =
        new ConsumerRecord<>("netflow", 0, 0L, null, sourceRecord);

    WriteResult result = writer.write(List.of(consumerRecord));

    assertEquals(WriteResult.SUCCESS_NO_COMMIT, result);

    try (var paths = Files.walk(tempDir)) {
      List<java.nio.file.Path> files = paths.filter(Files::isRegularFile).toList();
      assertEquals(1, files.stream().filter(path -> path.toString().endsWith(".tmp")).count());
      assertEquals(0, files.stream().filter(path -> path.toString().endsWith(".parquet")).count());
    }

    writer.close();

    assertEquals(1, registry.get("parquet_writer_records_finalized").counter().count());
    assertEquals(0, registry.get("parquet_writer_buffered_records").gauge().value());
    assertEquals(0, registry.get("parquet_writer_open_files").gauge().value());
    assertEquals(
        1,
        registry
            .get("parquet_writer_rotations")
            .tags("reason", "shutdown", "outcome", "succeeded")
            .counter()
            .count());
    assertEquals(
        1,
        registry.get("parquet_writer_file_size_bytes").tag("reason", "shutdown").summary().count());
    assertTrue(
        registry.get("parquet_writer_last_successful_rotation_timestamp_seconds").gauge().value()
            > 0);

    List<java.nio.file.Path> finalizedFiles;
    try (var paths = Files.walk(tempDir)) {
      finalizedFiles = paths.filter(Files::isRegularFile).toList();
    }

    java.nio.file.Path parquetFile =
        finalizedFiles.stream()
            .filter(path -> path.toString().endsWith(".parquet"))
            .findFirst()
            .orElseThrow();

    assertEquals(
        0, finalizedFiles.stream().filter(path -> path.toString().endsWith(".tmp")).count());
    assertEquals(
        1, finalizedFiles.stream().filter(path -> path.toString().endsWith(".parquet")).count());

    ParquetMetadata metadata = readMetadata(parquetFile);
    assertEquals(1, metadata.getBlocks().size());
    assertEquals(1, metadata.getBlocks().get(0).getRowCount());
    assertEquals("10.0.0.1", singleStringValue(metadata, "IPV4_SRC_ADDR"));
    assertEquals("10.0.0.2", singleStringValue(metadata, "IPV4_DST_ADDR"));
    assertDoesNotThrow(
        () ->
            Instant.parse(singleStringValue(metadata, NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void writesBothStringSchemaVersionsIntoTheSameOpenFile(boolean javaStringsFirst)
      throws Exception {
    List<GenericRecord> records = RandomRecordGenerator.generate(2, true);
    GenericRecord ordinary = records.get(0);
    ordinary.put("IPV4_SRC_ADDR", new Utf8(ordinary.get("IPV4_SRC_ADDR").toString()));
    ordinary.put("IPV4_DST_ADDR", new Utf8(ordinary.get("IPV4_DST_ADDR").toString()));
    GenericRecord javaStrings =
        RandomRecordGenerator.withSchema(
            records.get(1), RandomRecordGenerator.schemaWithJavaStrings());
    GenericRecord first = javaStringsFirst ? javaStrings : ordinary;
    GenericRecord second = javaStringsFirst ? ordinary : javaStrings;
    long timestamp = clock.instant().toEpochMilli();

    assertEquals(WriteResult.SUCCESS_NO_COMMIT, writer.write(List.of(recordAt(first, timestamp))));
    assertEquals(WriteResult.SUCCESS_NO_COMMIT, writer.write(List.of(recordAt(second, timestamp))));
    writer.close();

    List<GenericRecord> written = readFinalizedRecords();
    assertEquals(2, written.size());
    for (int index = 0; index < written.size(); index++) {
      GenericRecord expected = index == 0 ? first : second;
      GenericRecord actual = written.get(index);
      for (Schema.Field field : expected.getSchema().getFields()) {
        Object value = expected.get(field.name());
        if (value instanceof CharSequence) {
          assertEquals(value.toString(), actual.get(field.name()).toString());
        } else {
          assertEquals(value, actual.get(field.name()));
        }
      }
      assertEquals(
          clock.instant().toString(),
          actual.get(NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD).toString());
    }
  }

  @Test
  void writesReadableZstdCompressedFileWithConfiguredLevel() throws Exception {
    writer.close();

    Properties props = validProperties("ZSTD");
    props.setProperty("parquet.compression.codec.zstd.level", "7");
    config = new ParquetWriterConfig(props, mockEnvironment());
    writer = new HadoopParquetFileWriter(config, hadoopConf, clock, metrics);

    GenericRecord sourceRecord = RandomRecordGenerator.generate(1, true).get(0);
    sourceRecord.put("IPV4_SRC_ADDR", "10.20.30.40");
    sourceRecord.put("IPV4_DST_ADDR", "10.20.30.41");
    ConsumerRecord<byte[], GenericRecord> consumerRecord =
        new ConsumerRecord<>("netflow", 0, 0L, null, sourceRecord);

    assertEquals(WriteResult.SUCCESS_NO_COMMIT, writer.write(List.of(consumerRecord)));
    writer.close();
    writer = null;

    java.nio.file.Path parquetFile;
    try (var paths = Files.walk(tempDir)) {
      parquetFile =
          paths.filter(path -> path.toString().endsWith(".parquet")).findFirst().orElseThrow();
    }

    assertEquals(7, hadoopConf.getInt("parquet.compression.codec.zstd.level", -1));
    ParquetMetadata metadata = readMetadata(parquetFile);
    assertTrue(
        metadata.getBlocks().stream()
            .flatMap(block -> block.getColumns().stream())
            .allMatch(column -> column.getCodec() == CompressionCodecName.ZSTD));

    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.genericRecordReader(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(parquetFile.toUri()), hadoopConf))) {
      GenericRecord result = reader.read();
      assertEquals("10.20.30.40", result.get("IPV4_SRC_ADDR").toString());
      assertEquals("10.20.30.41", result.get("IPV4_DST_ADDR").toString());
      assertNull(reader.read());
    }
  }

  @Test
  void testFileRotationThreshold() throws Exception {
    GenericRecord sourceRecord = RandomRecordGenerator.generate(1, true).get(0);
    sourceRecord.put("IPV4_SRC_ADDR", "192.168.1.1");
    sourceRecord.put("IPV4_DST_ADDR", "192.168.1.2");
    ConsumerRecord<byte[], GenericRecord> consumerRecord =
        new ConsumerRecord<>("netflow", 0, 0L, null, sourceRecord);
    ConsumerRecord<byte[], GenericRecord> otherHourRecord =
        recordAt(sourceRecord, Instant.parse("2026-09-28T23:30:00Z").toEpochMilli());

    List<ConsumerRecord<byte[], GenericRecord>> batch = List.of(consumerRecord);
    WriteResult result = writer.write(List.of(otherHourRecord));

    int maxSafetyIterations = 50000;
    int iterations = 0;

    while (result == WriteResult.SUCCESS_NO_COMMIT && iterations < maxSafetyIterations) {
      result = writer.write(batch);
      iterations++;
    }

    assertEquals(
        WriteResult.SUCCESS, result, "Writer should return FILE_ROTATED once threshold is reached");

    try (var paths = Files.walk(tempDir)) {
      long finalizedCount = paths.filter(path -> path.toString().endsWith(".parquet")).count();
      assertEquals(2, finalizedCount, "Reaching the threshold must rotate every open file");
    }
  }

  @Test
  void testAllOpenFilesRotateAfterConfiguredInterval() throws Exception {
    GenericRecord sourceRecord = RandomRecordGenerator.generate(1, true).get(0);
    sourceRecord.put("IPV4_SRC_ADDR", "10.0.0.1");
    sourceRecord.put("IPV4_DST_ADDR", "10.0.0.2");
    ConsumerRecord<byte[], GenericRecord> firstHourRecord =
        recordAt(sourceRecord, Instant.parse("2026-09-28T22:30:00Z").toEpochMilli());
    ConsumerRecord<byte[], GenericRecord> secondHourRecord =
        recordAt(sourceRecord, Instant.parse("2026-09-28T23:30:00Z").toEpochMilli());

    assertEquals(
        WriteResult.SUCCESS_NO_COMMIT, writer.write(List.of(firstHourRecord, secondHourRecord)));

    clock.advance(Duration.ofMinutes(1));

    assertEquals(WriteResult.SUCCESS, writer.write(List.of()));

    try (var paths = Files.walk(tempDir)) {
      List<java.nio.file.Path> files = paths.filter(Files::isRegularFile).toList();
      assertEquals(0, files.stream().filter(path -> path.toString().endsWith(".tmp")).count());
      assertEquals(2, files.stream().filter(path -> path.toString().endsWith(".parquet")).count());
    }
  }

  private static ConsumerRecord<byte[], GenericRecord> recordAt(
      GenericRecord record, long timestamp) {
    return new ConsumerRecord<>("netflow", 0, 0L, null, record) {
      @Override
      public long timestamp() {
        return timestamp;
      }
    };
  }

  private List<GenericRecord> readFinalizedRecords() throws IOException {
    List<java.nio.file.Path> files;
    try (var paths = Files.walk(tempDir)) {
      files = paths.filter(path -> path.toString().endsWith(".parquet")).toList();
    }
    assertEquals(1, files.size());
    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.genericRecordReader(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(files.get(0).toUri()), hadoopConf))) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }
    return records;
  }

  private Properties validProperties(String codec) {
    Properties props = new Properties();
    props.setProperty("parquet.compression.codec", codec);
    props.setProperty("parquet.base.path", tempDir.toString());
    props.setProperty("parquet.target.file.size.mib", "1");
    props.setProperty("parquet.rotation.interval.ms", "60000");
    props.setProperty("parquet.time.zone", "UTC");
    props.setProperty("hdfs.fs.defaultFS", "file:///");
    props.setProperty("hdfs.user.name", "test");
    return props;
  }

  private static Map<String, String> mockEnvironment() {
    return Map.of(
        "HADOOP_USER_NAME", "test_user",
        "HDFS_DEFAULT_FS", "file:///");
  }

  private static ParquetMetadata readMetadata(java.nio.file.Path parquetFile) throws IOException {
    byte[] bytes = Files.readAllBytes(parquetFile);
    assertEquals("PAR1", new String(bytes, 0, 4, StandardCharsets.US_ASCII));
    assertEquals("PAR1", new String(bytes, bytes.length - 4, 4, StandardCharsets.US_ASCII));

    int footerLength =
        ByteBuffer.wrap(bytes, bytes.length - 8, Integer.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt();
    int footerOffset = bytes.length - 8 - footerLength;
    assertTrue(footerOffset >= 4);

    try (ByteArrayInputStream footer =
        new ByteArrayInputStream(bytes, footerOffset, footerLength)) {
      return new ParquetMetadataConverter().fromParquetMetadata(Util.readFileMetaData(footer));
    }
  }

  private static String singleStringValue(ParquetMetadata metadata, String fieldName) {
    ColumnChunkMetaData column =
        metadata.getBlocks().get(0).getColumns().stream()
            .filter(candidate -> candidate.getPath().toDotString().equals(fieldName))
            .findFirst()
            .orElseThrow();
    Statistics<?> statistics = column.getStatistics();
    assertTrue(statistics.hasNonNullValue());
    assertEquals(statistics.genericGetMin(), statistics.genericGetMax());
    return ((Binary) statistics.genericGetMin()).toStringUsingUTF8();
  }
}
