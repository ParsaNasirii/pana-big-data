package io.github.panabigdata.maintenance.config;

import io.github.panabigdata.ConfigSupport;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public final class MaintenanceConfig {
  private static final long BYTES_PER_MEBIBYTE = 1024L * 1024;

  private final String basePath;
  private final String hadoopConfigDirectory;
  private final boolean dryRun;
  private final long targetFileSizeBytes;
  private final String compressionCodec;
  private final long zstdCompressionLevel;
  private final Duration temporaryFileRetention;
  private final Duration staleLockAge;
  private final ZoneId timeZone;

  public MaintenanceConfig() {
    this(ConfigSupport.loadApplicationProperties(MaintenanceConfig.class));
  }

  public MaintenanceConfig(Properties properties) {
    basePath = ConfigSupport.requiredProperty(properties, "compaction.base.path");
    hadoopConfigDirectory = ConfigSupport.requiredProperty(properties, "hadoop.config.directory");
    dryRun = ConfigSupport.requiredBoolean(properties, "compaction.dry.run");
    targetFileSizeBytes =
        Math.multiplyExact(
            ConfigSupport.positiveLong(properties, "compaction.target.file.size.mib"),
            BYTES_PER_MEBIBYTE);
    compressionCodec =
        ConfigSupport.requiredProperty(properties, "parquet.compression.codec")
            .toUpperCase(Locale.ROOT);
    zstdCompressionLevel =
        ConfigSupport.optionalLong(properties, "parquet.compression.codec.zstd.level", 1L);
    temporaryFileRetention =
        Duration.ofHours(ConfigSupport.positiveLong(properties, "cleanup.temp.max.age.hours"));
    staleLockAge =
        Duration.ofHours(ConfigSupport.positiveLong(properties, "cleanup.lock.max.age.hours"));
    timeZone = ConfigSupport.requiredZoneId(properties, "parquet.time.zone");
    log.info(
        "Parquet maintenance: basePath={}, dryRun={}, targetFileSizeBytes={}, "
            + "compressionCodec={}, zstdCompressionLevel={}, "
            + "temporaryFileRetention={}, staleLockAge={}, timeZone={}",
        basePath,
        dryRun,
        targetFileSizeBytes,
        compressionCodec,
        zstdCompressionLevel,
        temporaryFileRetention,
        staleLockAge,
        timeZone);
  }
}
