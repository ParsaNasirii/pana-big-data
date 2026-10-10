package io.github.panabigdata.maintenance.compaction;

import static org.junit.jupiter.api.Assertions.*;

import io.github.panabigdata.config.ParquetWriterConfig;
import io.github.panabigdata.kafka.RandomRecordGenerator;
import io.github.panabigdata.maintenance.config.MaintenanceConfig;
import io.github.panabigdata.metrics.ParquetWriterMetrics;
import io.github.panabigdata.parquet.HadoopParquetFileWriter;
import io.github.panabigdata.parquet.NetFlowParquetSchema;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetCompactorIntegrationTest {
  @TempDir static java.nio.file.Path clusterDirectory;
  static final Schema sourceSchema = RandomRecordGenerator.schema();
  static MiniDFSCluster cluster;
  static FileSystem fs;
  static Configuration hadoop;
  static SparkSession spark;
  static final Instant NOW = Instant.now().plus(Duration.ofDays(2));
  static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  static final String HOUR = "2020-01-01/00";
  String base;

  @BeforeAll
  static void start() throws SQLException, IOException {
    hadoop = new Configuration(false);
    hadoop.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterDirectory.toString());
    hadoop.set("hadoop.tmp.dir", clusterDirectory.resolve("tmp").toString());
    hadoop.set("dfs.namenode.name.dir", clusterDirectory.resolve("name").toUri().toString());
    hadoop.set("dfs.namenode.edits.dir", clusterDirectory.resolve("edits").toUri().toString());
    hadoop.set("dfs.datanode.data.dir", clusterDirectory.resolve("data").toUri().toString());
    hadoop.set("fs.defaultFS", "hdfs://localhost:0");
    hadoop.setInt("dfs.replication", 1);
    hadoop.setBoolean("dfs.namenode.acls.enabled", true);
    hadoop.setBoolean("dfs.permissions.enabled", false);
    hadoop.setInt("dfs.namenode.handler.count", 2);
    hadoop.setInt("dfs.datanode.handler.count", 2);
    hadoop.setInt("dfs.datanode.max.transfer.threads", 8);
    cluster = new MiniDFSCluster.Builder(hadoop).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    hadoop.set("fs.defaultFS", fs.getUri().toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("compactor-hdfs-test")
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 2)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.hadoop.fs.defaultFS", fs.getUri().toString())
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
    org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.shutdown();
  }

  @BeforeEach
  void newBase() throws SQLException, IOException {
    base = "/test-" + UUID.randomUUID();
    fs.mkdirs(new Path(base));
  }

  MaintenanceConfig config(boolean dryRun) {
    return config(base, dryRun);
  }

  MaintenanceConfig config(String path, boolean dryRun) {
    return config(path, dryRun, 256);
  }

  MaintenanceConfig config(String path, boolean dryRun, int targetMiB) {
    return config(path, dryRun, targetMiB, 1);
  }

  MaintenanceConfig config(String path, boolean dryRun, int targetMiB, int zstdLevel) {
    Properties properties = new Properties();
    properties.setProperty("compaction.base.path", path);
    properties.setProperty("compaction.dry.run", Boolean.toString(dryRun));
    properties.setProperty("cleanup.temp.max.age.hours", "168");
    properties.setProperty("cleanup.lock.max.age.hours", "24");
    properties.setProperty("compaction.target.file.size.mib", Integer.toString(targetMiB));
    properties.setProperty("parquet.compression.codec", "ZSTD");
    properties.setProperty("parquet.compression.codec.zstd.level", Integer.toString(zstdLevel));
    properties.setProperty("hadoop.config.directory", "/etc/hadoop/conf");
    properties.setProperty("parquet.time.zone", "UTC");
    return new MaintenanceConfig(properties);
  }

  void write(String hour, int fileCount) throws SQLException, IOException {
    write(hour, fileCount, "10.0.0.1");
  }

  void write(String hour, int fileCount, String sourceAddress) throws SQLException, IOException {
    Properties props = new Properties();
    props.setProperty("parquet.base.path", base);
    props.setProperty("parquet.compression.codec", "SNAPPY");
    props.setProperty("parquet.target.file.size.mib", "512");
    props.setProperty("parquet.rotation.interval.ms", "3600000");
    props.setProperty("parquet.time.zone", "UTC");
    var writerConfig =
        new ParquetWriterConfig(
            props,
            Map.of(
                "HADOOP_USER_NAME",
                System.getProperty("user.name"),
                "HDFS_DEFAULT_FS",
                fs.getUri().toString()));
    long timestamp =
        Instant.parse(hour.substring(0, 10) + "T" + hour.substring(11) + ":00:00Z").toEpochMilli();
    var registry = new SimpleMeterRegistry();
    for (int i = 0; i < fileCount; i++) {
      GenericRecord record =
          new GenericData.Record(
              i % 2 == 0 ? sourceSchema : RandomRecordGenerator.schemaWithJavaStrings());
      for (var field : record.getSchema().getFields()) {
        Object value =
            switch (field.schema().getType()) {
              case STRING -> field.name().equals("IPV4_SRC_ADDR") ? sourceAddress : "10.0.0.1";
              case INT -> 42;
              case LONG -> 9007199254740993L;
              case DOUBLE -> 3.125;
              default -> throw new AssertionError("Unexpected type " + field.schema());
            };
        record.put(field.pos(), value);
      }
      ConsumerRecord<byte[], GenericRecord> kafka =
          new ConsumerRecord<>("netflow", 0, i, null, record) {
            @Override
            public long timestamp() {
              return timestamp;
            }
          };
      try (var writer =
          new HadoopParquetFileWriter(writerConfig, hadoop, new ParquetWriterMetrics(registry))) {
        writer.write(List.of(kafka, kafka)); // Duplicate rows must survive compaction.
      }
    }
    registry.close();
  }

  List<Path> parquetFiles(String directory) throws IOException {
    return Arrays.stream(fs.listStatus(new Path(directory)))
        .map(s -> s.getPath())
        .filter(p -> p.getName().endsWith(".parquet"))
        .toList();
  }

  List<Path> temporaryPaths(String prefix) throws IOException {
    return Arrays.stream(fs.listStatus(new Path(base + "/2020-01-01")))
        .map(status -> status.getPath())
        .filter(path -> path.getName().startsWith(prefix))
        .toList();
  }

  void assertCodec(String directory, CompressionCodecName expected)
      throws SQLException, IOException {
    for (Path path : parquetFiles(directory)) {
      try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoop))) {
        for (var block : reader.getFooter().getBlocks()) {
          for (var column : block.getColumns()) {
            assertEquals(expected, column.getCodec());
          }
        }
      }
    }
  }

  @Test
  void mergesSmallFilesWithinEachHourWithConfiguredCodecAndSameRows()
      throws SQLException, IOException {
    write(HOUR, 3);
    write("2020-01-01/01", 2);
    Dataset<Row> original = spark.read().parquet(base + "/" + HOUR).cache();
    assertEquals(6, original.count());

    var result = new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays();

    assertEquals(2, result.compacted());
    assertEquals(10, result.rows());
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
    assertEquals(1, parquetFiles(base + "/2020-01-01/01").size());
    assertCodec(base + "/" + HOUR, CompressionCodecName.ZSTD);
    Dataset<Row> output = spark.read().parquet(base + "/" + HOUR);
    assertEquals(0, original.exceptAll(output).count());
    assertEquals(0, output.exceptAll(original).count());
    assertEquals(
        0, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().selected());
    original.unpersist();
  }

  @Test
  void passesConfiguredZstdLevelToParquetWriter() throws SQLException, IOException {
    write(HOUR, 2);

    var compactor = new ParquetCompactor(spark, config(base, false, 256, 3), CLOCK);

    assertEquals(1, compactor.compactPreviousDays().compacted());
    assertEquals(
        3,
        spark
            .sparkContext()
            .hadoopConfiguration()
            .getInt("parquet.compression.codec.zstd.level", -1));
    assertCodec(base + "/" + HOUR, CompressionCodecName.ZSTD);
  }

  @Test
  void mergesLateFilesInAnOldHourOnTheNextRun() throws SQLException, IOException {
    write(HOUR, 2);
    new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays();
    write(HOUR, 2);
    assertEquals(
        1, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
    assertEquals(8, spark.read().parquet(base + "/" + HOUR).count());
  }

  @Test
  void mergesRecentFilesFromPreviousDaysButSkipsSingleFilesAndToday()
      throws SQLException, IOException {
    write(HOUR, 1);
    write("2020-01-01/01", 2);
    String today = LocalDate.ofInstant(NOW, ZoneOffset.UTC) + "/00";
    write(today, 2);
    for (Path file : parquetFiles(base + "/2020-01-01/01"))
      fs.setTimes(file, NOW.toEpochMilli(), -1);
    assertEquals(
        1, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertEquals(1, parquetFiles(base + "/2020-01-01/01").size());
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
    assertEquals(2, parquetFiles(base + "/" + today).size());
  }

  @Test
  void leavesLargeFilesAloneWhileMergingSmallOnes() throws SQLException, IOException {
    byte[] random = new byte[2 * 1024 * 1024];
    new java.util.Random(1234).nextBytes(random);
    write(HOUR, 1, java.util.Base64.getEncoder().encodeToString(random));
    Path large = parquetFiles(base + "/" + HOUR).get(0);
    assertTrue(fs.getFileStatus(large).getLen() > 1024 * 1024);
    write(HOUR, 2);

    var settings = config(base, false, 1);
    var result = new ParquetCompactor(spark, settings, CLOCK).compactPreviousDays();

    assertEquals(1, result.compacted());
    assertTrue(fs.exists(large));
    assertEquals(2, parquetFiles(base + "/" + HOUR).size());
    assertEquals(6, spark.read().parquet(base + "/" + HOUR).count());
  }

  @Test
  void skipsLockedHourAndTriesItOnNextRun() throws SQLException, IOException {
    write(HOUR, 2);
    try (var lock =
        io.github.panabigdata.hdfs.HourlyDirectoryLock.tryAcquire(
            fs, new Path(base + "/" + HOUR))) {
      fs.setTimes(new Path(base + "/2020-01-01/.00.maintenance.lock"), NOW.toEpochMilli(), -1);
      var result = new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays();
      assertEquals(1, result.selected());
      assertEquals(0, result.compacted());
      assertEquals(2, parquetFiles(base + "/" + HOUR).size());
    }
    assertEquals(
        1, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
  }

  @Test
  void reclaimsExpiredLockWhenNoOriginalsNeedRecovery() throws SQLException, IOException {
    write(HOUR, 2);
    Path lock = new Path(base + "/2020-01-01/.00.maintenance.lock");
    fs.create(lock).close();
    fs.setTimes(lock, NOW.minus(Duration.ofDays(2)).toEpochMilli(), -1);

    assertEquals(
        1, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertFalse(fs.exists(lock));
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
  }

  @Test
  void restoresOriginalsAfterInterruptedMergeBeforePublication() throws SQLException, IOException {
    write(HOUR, 2);
    Path lock = new Path(base + "/2020-01-01/.00.maintenance.lock");
    Path backup = new Path(base + "/2020-01-01/.00.compacting-interrupted/originals");
    fs.mkdirs(backup);
    Path original = parquetFiles(base + "/" + HOUR).get(0);
    assertTrue(fs.rename(original, new Path(backup, original.getName())));
    fs.create(lock).close();
    fs.setTimes(lock, NOW.minus(Duration.ofDays(2)).toEpochMilli(), -1);

    assertEquals(
        1, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertFalse(fs.exists(lock));
    assertFalse(fs.exists(backup));
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
    assertEquals(4, spark.read().parquet(base + "/" + HOUR).count());
  }

  @Test
  void restoresOriginalsFromUnlockedStageBeforeCompacting() throws SQLException, IOException {
    write(HOUR, 2);
    Path originals = new Path(base + "/2020-01-01/.00.compacting-interrupted/originals");
    fs.mkdirs(originals);
    Path original = parquetFiles(base + "/" + HOUR).get(0);
    assertTrue(fs.rename(original, new Path(originals, original.getName())));

    assertEquals(
        1, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertFalse(fs.exists(originals));
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
    assertEquals(4, spark.read().parquet(base + "/" + HOUR).count());
  }

  @Test
  void finishesInterruptedMergeAfterPublication() throws SQLException, IOException {
    write(HOUR, 2);
    Path hour = new Path(base + "/" + HOUR);
    Path stage = new Path(base + "/2020-01-01/.00.compacting-interrupted");
    Path backup = new Path(stage, "originals");
    spark.read().parquet(hour.toString()).coalesce(1).write().parquet(stage.toString());
    Path merged = new Path(hour, "merged-interrupted.parquet");
    assertTrue(fs.rename(parquetFiles(stage.toString()).get(0), merged));
    fs.mkdirs(backup);
    for (Path original : parquetFiles(hour.toString())) {
      if (!original.getName().equals(merged.getName())) {
        assertTrue(fs.rename(original, new Path(backup, original.getName())));
      }
    }
    Path lock = new Path(base + "/2020-01-01/.00.maintenance.lock");
    fs.create(lock).close();
    fs.setTimes(lock, NOW.minus(Duration.ofDays(2)).toEpochMilli(), -1);

    assertEquals(
        0, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertFalse(fs.exists(lock));
    assertFalse(fs.exists(stage));
    assertEquals(
        List.of(merged.getName()),
        parquetFiles(hour.toString()).stream().map(Path::getName).toList());
    assertEquals(4, spark.read().parquet(hour.toString()).count());
  }

  @Test
  void cleansOnlyExpiredWriterAndAbandonedStagingFiles() throws SQLException, IOException {
    Path oldTemp = new Path(base + "/_temporary/2020-01-01/00/old.tmp");
    Path newTemp = new Path(base + "/_temporary/2020-01-01/00/new.tmp");
    Path oldStage = new Path(base + "/2020-01-01/.00.compacting-old");
    Path backupStage = new Path(base + "/2020-01-01/.00.compacting-backup/originals");
    fs.mkdirs(oldTemp.getParent());
    fs.create(oldTemp).close();
    fs.create(newTemp).close();
    fs.mkdirs(oldStage);
    fs.mkdirs(backupStage);
    long expired = NOW.minus(Duration.ofDays(8)).toEpochMilli();
    fs.setTimes(oldTemp, expired, -1);
    fs.setTimes(oldStage, expired, -1);
    fs.setTimes(backupStage.getParent(), expired, -1);

    new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays();

    assertFalse(fs.exists(oldTemp));
    assertFalse(fs.exists(oldStage));
    assertTrue(fs.exists(newTemp));
    assertFalse(fs.exists(backupStage));
  }

  @Test
  void dryRunDoesNotWriteOrClean() throws SQLException, IOException {
    write(HOUR, 2);
    Path temp = new Path(base + "/_temporary/old.tmp");
    fs.create(temp).close();
    fs.setTimes(temp, NOW.minus(Duration.ofDays(8)).toEpochMilli(), -1);
    var result = new ParquetCompactor(spark, config(true), CLOCK).compactPreviousDays();
    assertEquals(1, result.selected());
    assertEquals(0, result.compacted());
    assertEquals(2, parquetFiles(base + "/" + HOUR).size());
    assertTrue(fs.exists(temp));
  }

  @Test
  void rollsBackWhenPublicationFails() throws SQLException, IOException {
    write(HOUR, 2);
    var compactor =
        new ParquetCompactor(spark, config(false), CLOCK) {
          @Override
          void renameWithoutOverwrite(Path from, Path to) throws IOException {
            super.renameWithoutOverwrite(from, to);
            if (to.getName().startsWith("merged-")) {
              throw new IOException("injected failure");
            }
          }
        };
    assertThrows(IOException.class, compactor::compactPreviousDays);
    assertEquals(2, parquetFiles(base + "/" + HOUR).size());
    assertCodec(base + "/" + HOUR, CompressionCodecName.SNAPPY);
    assertFalse(fs.exists(new Path(base + "/2020-01-01/.00.maintenance.lock")));
  }

  @Test
  void rejectsCorruptInputAndFilesystemRoot() throws SQLException, IOException {
    MaintenanceConfig rootConfig = config("/", true);
    assertThrows(
        IllegalArgumentException.class, () -> new ParquetCompactor(spark, rootConfig, CLOCK));
    write(HOUR, 1);
    try (var stream = fs.create(new Path(base + "/" + HOUR + "/broken.parquet"))) {
      stream.write(42);
    }
    spark.conf().set("spark.sql.files.ignoreCorruptFiles", "true");
    try {
      ParquetCompactor compactor = new ParquetCompactor(spark, config(false), CLOCK);
      assertThrows(IOException.class, compactor::compactPreviousDays);
      assertEquals(2, parquetFiles(base + "/" + HOUR).size());
    } finally {
      spark.conf().set("spark.sql.files.ignoreCorruptFiles", "false");
    }
  }

  @Test
  void skipsHoursWithOnlyOneParquetFileIncludingEmptyFiles() throws SQLException, IOException {
    write(HOUR, 1);
    Path empty = new Path(base + "/2020-01-01/01/empty.parquet");
    fs.mkdirs(empty.getParent());
    try (var writer =
        AvroParquetWriter.builder(HadoopOutputFile.fromPath(empty, hadoop))
            .withSchema(NetFlowParquetSchema.createSchema(sourceSchema))
            .withConf(hadoop)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {}
    assertEquals(
        0, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().compacted());
    assertEquals(0, spark.read().parquet(base + "/2020-01-01/01").count());
    assertEquals(1, parquetFiles(base + "/" + HOUR).size());
    assertCodec(base + "/" + HOUR, CompressionCodecName.SNAPPY);
    assertEquals(
        0, new ParquetCompactor(spark, config(false), CLOCK).compactPreviousDays().selected());
  }

  @Test
  void rejectsFilesystemRootAndUnknownFiles() throws SQLException, IOException {
    MaintenanceConfig rootConfig = config("/", true);
    assertThrows(
        IllegalArgumentException.class, () -> new ParquetCompactor(spark, rootConfig, CLOCK));

    write(HOUR, 2);
    try (var stream = fs.create(new Path(base + "/" + HOUR + "/unexpected.txt"))) {
      stream.write(1);
    }
    ParquetCompactor compactor = new ParquetCompactor(spark, config(false), CLOCK);
    assertThrows(IOException.class, compactor::compactPreviousDays);
  }
}
