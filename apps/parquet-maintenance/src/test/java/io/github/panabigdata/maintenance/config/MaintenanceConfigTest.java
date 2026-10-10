package io.github.panabigdata.maintenance.config;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class MaintenanceConfigTest {
  @Test
  void readsMaintenanceSettings() {
    MaintenanceConfig config = new MaintenanceConfig(validProperties());
    assertEquals("/netflow", config.getBasePath());
    assertEquals(256L * 1024 * 1024, config.getTargetFileSizeBytes());
    assertEquals("ZSTD", config.getCompressionCodec());
    assertEquals(1L, config.getZstdCompressionLevel());
    assertEquals(Duration.ofHours(168), config.getTemporaryFileRetention());
    assertEquals(Duration.ofHours(24), config.getStaleLockAge());
    assertFalse(config.isDryRun());
  }

  @Test
  void rejectsInvalidSettings() {
    Properties emptyProperties = new Properties();
    assertThrows(IllegalArgumentException.class, () -> new MaintenanceConfig(emptyProperties));
    for (String key :
        new String[] {
          "compaction.target.file.size.mib",
          "cleanup.temp.max.age.hours",
          "cleanup.lock.max.age.hours"
        }) {
      Properties properties = validProperties();
      properties.setProperty(key, "0");
      assertThrows(IllegalArgumentException.class, () -> new MaintenanceConfig(properties));
    }
  }

  private Properties validProperties() {
    Properties properties = new Properties();
    properties.setProperty("compaction.base.path", "/netflow");
    properties.setProperty("compaction.dry.run", "false");
    properties.setProperty("compaction.target.file.size.mib", "256");
    properties.setProperty("parquet.compression.codec", "zstd");
    properties.setProperty("parquet.compression.codec.zstd.level", "1");
    properties.setProperty("cleanup.temp.max.age.hours", "168");
    properties.setProperty("cleanup.lock.max.age.hours", "24");
    properties.setProperty("hadoop.config.directory", "/etc/hadoop/conf");
    properties.setProperty("parquet.time.zone", "Asia/Tehran");
    return properties;
  }
}
