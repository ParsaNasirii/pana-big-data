package io.github.panabigdata.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ParquetWriterConfigTest {

  @Test
  void shouldSupportTwoGibTargetSize() {
    Properties properties = validProperties();
    properties.setProperty("parquet.target.file.size.mib", "2048");

    ParquetWriterConfig config =
        new ParquetWriterConfig(properties, Map.of("HADOOP_USER_NAME", "test"));

    assertThat(config.getTargetFileSizeBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
  }

  @Test
  void shouldRejectMissingHadoopUserEnvironment() {
    Properties properties = validProperties();

    assertThatThrownBy(() -> new ParquetWriterConfig(properties, Map.of()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("HADOOP_USER_NAME");
  }

  @Test
  void shouldRejectInvalidTimeZone() {
    Properties properties = validProperties();
    properties.setProperty("parquet.time.zone", "not-a-time-zone");

    assertThatThrownBy(
            () -> new ParquetWriterConfig(properties, Map.of("HADOOP_USER_NAME", "test")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("parquet.time.zone");
  }

  private Properties validProperties() {
    Properties properties = new Properties();
    properties.setProperty("parquet.compression.codec", "SNAPPY");
    properties.setProperty("parquet.base.path", "/netflow");
    properties.setProperty("parquet.target.file.size.mib", "512");
    properties.setProperty("parquet.rotation.interval.ms", "3600000");
    properties.setProperty("parquet.time.zone", "UTC");
    properties.setProperty("parquet.estimated.avro-to-parquet.ratio", "2.0");
    properties.setProperty("parquet.compression.codec.zstd.level", "1");

    return properties;
  }
}
