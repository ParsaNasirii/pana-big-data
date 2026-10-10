package io.github.panabigdata.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.panabigdata.config.ParquetWriterConfig;
import io.github.panabigdata.kafka.RandomRecordGenerator;
import io.github.panabigdata.kafka.WriteResult;
import io.github.panabigdata.metrics.ParquetWriterMetrics;
import io.github.panabigdata.parquet.HadoopParquetFileWriter;
import io.github.panabigdata.parquet.NetFlowParquetSchema;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InnerHadoopParquetFileWriterIntegrationTest {

  private static final DateTimeFormatter PARTITION_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd/HH");

  private MiniDFSCluster hdfsCluster;
  private Configuration hadoopConf;
  private String testBasePath;
  private HadoopParquetFileWriter writer;
  private SimpleMeterRegistry registry;
  private ParquetWriterMetrics metrics;

  @BeforeEach
  void setUp() throws Exception {
    org.apache.hadoop.util.ExitUtil.resetFirstExitException();

    File baseDir = new File("./target/hdfs-test-data/" + UUID.randomUUID()).getAbsoluteFile();

    if (!baseDir.mkdirs() && !baseDir.isDirectory()) {
      throw new IOException("Could not create MiniDFSCluster base directory: " + baseDir);
    }

    // IMPORTANT:
    // Do not load the machine's real Hadoop configuration.
    hadoopConf = new Configuration(false);

    hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    hadoopConf.set("dfs.replication", "1");

    hadoopConf.setBoolean("dfs.ha.automatic-failover.enabled", false);
    hadoopConf.setBoolean("dfs.client.read.shortcircuit", false);

    hadoopConf.unset("dfs.domain.socket.path");

    hadoopConf.set("fs.defaultFS", "hdfs://localhost:0");

    hdfsCluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(1).build();

    hdfsCluster.waitActive();

    FileSystem fs = hdfsCluster.getFileSystem();

    String hdfsUri = fs.getUri().toString();

    testBasePath = "/test-data/panabigdata-" + UUID.randomUUID();

    Properties props = new Properties();
    props.setProperty("parquet.compression.codec", "SNAPPY");
    props.setProperty("parquet.base.path", testBasePath);
    props.setProperty("parquet.target.file.size.mib", "10");
    props.setProperty("parquet.rotation.interval.ms", "3600000");
    props.setProperty("parquet.time.zone", "UTC");

    Map<String, String> mockEnv =
        Map.of("HADOOP_USER_NAME", System.getProperty("user.name"), "HDFS_DEFAULT_FS", hdfsUri);

    ParquetWriterConfig config = new ParquetWriterConfig(props, mockEnv);

    registry = new SimpleMeterRegistry();
    metrics = new ParquetWriterMetrics(registry);
    writer = new HadoopParquetFileWriter(config, hadoopConf, metrics);
  }

  @AfterEach
  void tearDown() {
    if (writer != null) {
      try {
        writer.close();
      } catch (Exception e) {
      }
    }

    if (hdfsCluster != null) {
      hdfsCluster.shutdown(true);
    }

    registry.close();

    org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.shutdown();
  }

  private List<ConsumerRecord<byte[], GenericRecord>> createRecords(
      int count, boolean valid, long timestamp) {
    List<GenericRecord> avroRecords = RandomRecordGenerator.generate(count, valid);
    List<ConsumerRecord<byte[], GenericRecord>> kafkaRecords = new ArrayList<>(count);

    for (int i = 0; i < avroRecords.size(); i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);

      ConsumerRecord<byte[], GenericRecord> record =
          new ConsumerRecord<>("netflow-topic", 0, i, key, avroRecords.get(i)) {
            @Override
            public long timestamp() {
              return timestamp;
            }
          };
      kafkaRecords.add(record);
    }
    return kafkaRecords;
  }

  private String getExpectedPartition(long timestampMillis) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneId.of("UTC"))
        .format(PARTITION_FORMATTER);
  }

  @Test
  @DisplayName("Verify correct UTC directory structure and temporary path cleanup")
  void shouldCreateCorrectTimePartitionedDirectoryAndCleanupTempFiles() throws IOException {
    long currentTestTime = System.currentTimeMillis();
    List<ConsumerRecord<byte[], GenericRecord>> records = createRecords(5, true, currentTestTime);
    String expectedPartition = getExpectedPartition(currentTestTime);

    WriteResult result = writer.write(records);

    assertEquals(WriteResult.SUCCESS_NO_COMMIT, result, "Write result must be SUCCESS_NO_COMMIT");

    FileSystem fs = hdfsCluster.getFileSystem();
    Path tempPartitionPath = new Path(testBasePath, "_temporary/" + expectedPartition);

    assertTrue(fs.exists(tempPartitionPath), "Temporary directory must exist before file rotation");

    writer.close();

    Path finalPartitionPath = new Path(testBasePath, expectedPartition);
    assertTrue(
        fs.exists(finalPartitionPath), "Final partition directory does not exist after close");

    FileStatus[] tempFiles =
        fs.exists(tempPartitionPath) ? fs.listStatus(tempPartitionPath) : new FileStatus[0];
    assertEquals(0, tempFiles.length, "Temporary directory must be empty after write completes");
  }

  @Test
  @DisplayName("Verify parquet content integrity, record count, and kafka timestamp field")
  void shouldWriteDataCorrectlyToParquetFile() throws IOException {
    long currentTestTime = System.currentTimeMillis();
    int expectedCount = 3;
    List<ConsumerRecord<byte[], GenericRecord>> records =
        createRecords(expectedCount, true, currentTestTime);
    String expectedPartition = getExpectedPartition(currentTestTime);
    Path partitionPath = new Path(testBasePath, expectedPartition);

    writer.write(records);
    writer.close();

    FileSystem fs = hdfsCluster.getFileSystem();

    Path parquetFile =
        Stream.of(fs.listStatus(partitionPath))
            .map(FileStatus::getPath)
            .filter(p -> p.getName().endsWith(".parquet"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No parquet file found in partition"));

    HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetFile, hadoopConf);
    try (ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(inputFile).withConf(hadoopConf).build()) {

      int actualCount = 0;
      GenericRecord readRecord;

      while ((readRecord = parquetReader.read()) != null) {
        assertNotNull(
            readRecord.get(NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD),
            "KAFKA_TIMESTAMP field is missing from the record");
        assertNotNull(readRecord.get("IPV4_SRC_ADDR"), "IPV4_SRC_ADDR must not be null");
        actualCount++;
      }

      assertEquals(
          expectedCount, actualCount, "Persisted record count must match input record count");
    }
  }

  @Test
  @DisplayName("Verify subsequent batches accumulate in the same file until rotation")
  void concurrentBatchesShouldAccumulateInSameFile() throws IOException {
    long currentTestTime = System.currentTimeMillis();
    List<ConsumerRecord<byte[], GenericRecord>> batch1 = createRecords(4, true, currentTestTime);
    List<ConsumerRecord<byte[], GenericRecord>> batch2 = createRecords(6, true, currentTestTime);

    String expectedPartition = getExpectedPartition(currentTestTime);
    Path partitionPath = new Path(testBasePath, expectedPartition);

    WriteResult res1 = writer.write(batch1);
    WriteResult res2 = writer.write(batch2);

    assertEquals(WriteResult.SUCCESS_NO_COMMIT, res1);
    assertEquals(WriteResult.SUCCESS_NO_COMMIT, res2);

    writer.close();

    FileSystem fs = hdfsCluster.getFileSystem();

    long parquetFilesCount =
        Stream.of(fs.listStatus(partitionPath))
            .map(FileStatus::getPath)
            .filter(p -> p.getName().endsWith(".parquet"))
            .count();

    assertEquals(1, parquetFilesCount, "There should be exactly 1 accumulated parquet file");

    Path parquetFile =
        Stream.of(fs.listStatus(partitionPath))
            .map(FileStatus::getPath)
            .filter(p -> p.getName().endsWith(".parquet"))
            .findFirst()
            .orElseThrow();

    HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetFile, hadoopConf);
    try (ParquetReader<GenericRecord> parquetReader =
        AvroParquetReader.<GenericRecord>builder(inputFile).withConf(hadoopConf).build()) {

      int totalCount = 0;
      while (parquetReader.read() != null) {
        totalCount++;
      }

      assertEquals(10, totalCount, "The single file must contain all records from both batches");
    }
  }

  @Test
  @DisplayName("Verify automatic file rotation when volume threshold is reached")
  void shouldRotateFileAutomaticallyWhenThresholdReached() throws IOException {
    writer.close();

    Properties props = new Properties();
    props.setProperty("parquet.compression.codec", "SNAPPY");
    props.setProperty("parquet.base.path", testBasePath);
    props.setProperty("parquet.target.file.size.mib", "1");
    props.setProperty("parquet.rotation.interval.ms", "3600000");
    props.setProperty("parquet.time.zone", "UTC");

    Map<String, String> mockEnv =
        Map.of(
            "HADOOP_USER_NAME", System.getProperty("user.name"),
            "HDFS_DEFAULT_FS", hdfsCluster.getFileSystem().getUri().toString());

    ParquetWriterConfig lowThresholdConfig = new ParquetWriterConfig(props, mockEnv);
    writer = new HadoopParquetFileWriter(lowThresholdConfig, hadoopConf, metrics);

    long currentTestTime = System.currentTimeMillis();
    List<ConsumerRecord<byte[], GenericRecord>> largeBatch =
        createRecords(1000, true, currentTestTime);
    WriteResult result = WriteResult.SUCCESS_NO_COMMIT;

    int iterations = 0;
    int maxIterations = 500;

    while (result == WriteResult.SUCCESS_NO_COMMIT && iterations < maxIterations) {
      result = writer.write(largeBatch);
      iterations++;
    }

    assertEquals(
        WriteResult.SUCCESS,
        result,
        "Writer must return SUCCESS (rotated) when threshold is exceeded");

    FileSystem fs = hdfsCluster.getFileSystem();
    String expectedPartition = getExpectedPartition(currentTestTime);
    Path finalPartitionPath = new Path(testBasePath, expectedPartition);

    long parquetFilesCount =
        Stream.of(fs.listStatus(finalPartitionPath))
            .map(FileStatus::getPath)
            .filter(p -> p.getName().endsWith(".parquet"))
            .count();

    assertEquals(
        1, parquetFilesCount, "One rotated Parquet file should exist in the final directory");
  }

  @Test
  @DisplayName("Verify system stability when receiving an empty batch")
  void shouldHandleEmptyBatchesWithoutError() throws IOException {
    WriteResult result = writer.write(List.of());

    assertEquals(
        WriteResult.SUCCESS_NO_COMMIT, result, "Empty batch should just return SUCCESS_NO_COMMIT");

    FileSystem fs = hdfsCluster.getFileSystem();
    Path partitionPath = new Path(testBasePath);

    assertFalse(fs.exists(partitionPath), "No directories should be created for an empty batch");
  }

  @Test
  @DisplayName("Keep one open file per hour and finalize all files together")
  void shouldWriteDifferentHoursToSeparateOpenFiles() throws IOException {
    long timeHour11Millis =
        ZonedDateTime.of(2026, 9, 6, 11, 59, 59, 0, ZoneId.of("UTC")).toInstant().toEpochMilli();
    long timeHour12Millis =
        ZonedDateTime.of(2026, 9, 6, 12, 0, 1, 0, ZoneId.of("UTC")).toInstant().toEpochMilli();

    writer.write(createRecords(5, true, timeHour11Millis));

    FileSystem fs = hdfsCluster.getFileSystem();
    String partition11 = getExpectedPartition(timeHour11Millis);
    Path tmpPath11 = new Path(testBasePath, "_temporary/" + partition11);

    assertTrue(fs.exists(tmpPath11), "Temporary directory should exist for hour 11");

    WriteResult result = writer.write(createRecords(5, true, timeHour12Millis));

    Path finalPath11 = new Path(testBasePath, partition11);
    assertEquals(WriteResult.SUCCESS_NO_COMMIT, result);
    assertFalse(fs.exists(finalPath11), "Hour change alone must not trigger rotation");
    assertEquals(1, fs.listStatus(tmpPath11).length, "Hour 11 file must remain open");

    String partition12 = getExpectedPartition(timeHour12Millis);
    Path tmpPath12 = new Path(testBasePath, "_temporary/" + partition12);
    assertEquals(1, fs.listStatus(tmpPath12).length, "Hour 12 must have its own open file");

    writer.close();

    Path finalPath12 = new Path(testBasePath, partition12);
    assertEquals(1, fs.listStatus(finalPath11).length, "Hour 11 file must be finalized");
    assertEquals(1, fs.listStatus(finalPath12).length, "Hour 12 file must be finalized");
    assertEquals(0, fs.listStatus(tmpPath11).length, "Hour 11 temp file must be removed");
    assertEquals(0, fs.listStatus(tmpPath12).length, "Hour 12 temp file must be removed");
  }

  @Test
  @DisplayName("Verify internal Parquet metadata for SNAPPY compression")
  void shouldCompressDataUsingSnappyCodec() throws IOException {
    long currentTestTime = System.currentTimeMillis();
    List<ConsumerRecord<byte[], GenericRecord>> records = createRecords(10, true, currentTestTime);
    String expectedPartition = getExpectedPartition(currentTestTime);
    Path partitionPath = new Path(testBasePath, expectedPartition);

    writer.write(records);
    writer.close();

    FileSystem fs = hdfsCluster.getFileSystem();
    Path parquetFile =
        Stream.of(fs.listStatus(partitionPath))
            .map(FileStatus::getPath)
            .filter(p -> p.getName().endsWith(".parquet"))
            .findFirst()
            .orElseThrow();

    ParquetMetadata metadata =
        ParquetFileReader.readFooter(hadoopConf, parquetFile, ParquetMetadataConverter.NO_FILTER);

    CompressionCodecName codec = metadata.getBlocks().get(0).getColumns().get(0).getCodec();

    assertEquals(
        CompressionCodecName.SNAPPY,
        codec,
        "The internal Parquet column chunks must be compressed with SNAPPY");
  }

  @Test
  @DisplayName("Verify fault tolerance and exception propagation on HDFS failure")
  void shouldThrowExceptionWhenDatanodeFails() throws IOException {
    long currentTestTime = System.currentTimeMillis();
    writer.write(createRecords(5, true, currentTestTime));
    hdfsCluster.shutdownDataNodes();

    Exception thrownException =
        assertThrows(
            Exception.class,
            () -> {
              writer.write(createRecords(10, true, currentTestTime));
              writer.close();
            },
            "Writer must throw an exception when the underlying HDFS DataNode becomes unreachable");

    boolean expectedErrorFound = false;
    Throwable current = thrownException;

    while (current != null) {
      if (current instanceof IOException
          || (current instanceof IllegalArgumentException
              && current.getMessage().contains("Self-suppression"))) {
        expectedErrorFound = true;
        break;
      }
      current = current.getCause();
    }

    assertTrue(
        expectedErrorFound,
        "The root cause should be an HDFS node failure or the known Parquet self-suppression bug. Actual error chain: "
            + thrownException);
  }
}
