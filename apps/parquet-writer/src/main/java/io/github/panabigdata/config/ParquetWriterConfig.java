package io.github.panabigdata.config;

import io.github.panabigdata.ConfigSupport;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class ParquetWriterConfig {

  private static final long BYTES_PER_MIB = 1024L * 1024L;
  private static final String FILE_NAME_PREFIX = "part-";
  private static final String FILE_EXTENSION = ".parquet";

  private final String compressionCodec;
  private final long compressionLevel;
  private final String basePath;
  private final long targetFileSizeBytes;
  private final long rotationIntervalMillis;
  private final ZoneId timeZone;

  private final String hdfsUserName;

  public ParquetWriterConfig() {
    this(ConfigSupport.loadApplicationProperties(ParquetWriterConfig.class), System.getenv());
  }

  public ParquetWriterConfig(Properties properties, Map<String, String> environment) {
    this.compressionCodec =
        ConfigSupport.requiredProperty(properties, "parquet.compression.codec").toUpperCase();
    this.compressionLevel =
        ConfigSupport.optionalLong(properties, "parquet.compression.codec.zstd.level", 1L);
    this.basePath = ConfigSupport.requiredProperty(properties, "parquet.base.path");

    long targetMib = ConfigSupport.positiveLong(properties, "parquet.target.file.size.mib");
    this.targetFileSizeBytes = targetMib * BYTES_PER_MIB;

    this.rotationIntervalMillis =
        ConfigSupport.positiveLong(properties, "parquet.rotation.interval.ms");
    this.timeZone = ConfigSupport.requiredZoneId(properties, "parquet.time.zone");

    Properties mergedEnv = ConfigSupport.copyProperties(properties);
    ConfigSupport.overrideRequiredEnvironment(
        mergedEnv, "hdfs.user.name", environment, "HADOOP_USER_NAME");
    this.hdfsUserName = mergedEnv.getProperty("hdfs.user.name");

    log.info(
        "Parquet Config initialized. Codec: {}, Target Size: {} MiB, Rotation Interval: {} ms, Time Zone: {}",
        compressionCodec,
        targetMib,
        rotationIntervalMillis,
        timeZone);
  }

  public String generateFileName() {
    return FILE_NAME_PREFIX + UUID.randomUUID() + FILE_EXTENSION;
  }
}
