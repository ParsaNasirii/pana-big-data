package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigSupportTest {

  @TempDir Path tempDir;

  @Test
  void shouldLoadApplicationPropertiesFromClasspathForTests() {
    Properties properties =
        ConfigSupport.loadApplicationProperties(
            ConfigSupportTest.class, tempDir.resolve("missing.properties"));

    assertThat(properties.getProperty("sample.key")).isEqualTo("classpath-value");
  }

  @Test
  void shouldPreferMountedApplicationProperties() throws IOException {
    Path configFile = tempDir.resolve("application.properties");
    Files.writeString(configFile, "sample.key=mounted-value");

    Properties properties =
        ConfigSupport.loadApplicationProperties(ConfigSupportTest.class, configFile);

    assertThat(properties.getProperty("sample.key")).isEqualTo("mounted-value");
  }

  @Test
  void shouldCopyAndOverridePropertiesFromEnvironment() {
    Properties source = new Properties();
    source.setProperty("required", "file-value");
    source.setProperty("optional", "file-value");

    Properties copy = ConfigSupport.copyProperties(source);

    ConfigSupport.overrideRequiredEnvironment(
        copy, "required", Map.of("REQUIRED_ENV", "runtime-value"), "REQUIRED_ENV");

    ConfigSupport.overrideOptionalEnvironment(
        copy, "optional", Map.of("OPTIONAL_ENV", "runtime-value"), "OPTIONAL_ENV");

    assertThat(copy.getProperty("required")).isEqualTo("runtime-value");
    assertThat(copy.getProperty("optional")).isEqualTo("runtime-value");
    assertThat(source.getProperty("required")).isEqualTo("file-value");

    ConfigSupport.overrideOptionalEnvironment(copy, "optional", Map.of(), "OPTIONAL_ENV");

    assertThat(copy.getProperty("optional")).isEqualTo("runtime-value");

    assertThatThrownBy(
            () ->
                ConfigSupport.overrideRequiredEnvironment(
                    copy, "required", Map.of(), "REQUIRED_ENV"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("REQUIRED_ENV");
  }

  @Test
  void shouldReadValidValues() {
    Properties properties = new Properties();
    properties.setProperty("text", "value");
    properties.setProperty("blank", " ");
    properties.setProperty("enabled", "true");
    properties.setProperty("int", "10");
    properties.setProperty("long", "20");
    properties.setProperty("zero", "0");
    properties.setProperty("double", "1.5");
    properties.setProperty("time-zone", "Asia/Tehran");

    assertThat(ConfigSupport.requiredProperty(properties, "text")).isEqualTo("value");
    assertThat(ConfigSupport.optionalProperty(properties, "text")).isEqualTo("value");
    assertThat(ConfigSupport.optionalProperty(properties, "missing")).isNull();
    assertThat(ConfigSupport.optionalProperty(properties, "blank")).isNull();
    assertThat(ConfigSupport.requiredBoolean(properties, "enabled")).isTrue();
    properties.setProperty("enabled", "FALSE");
    assertThat(ConfigSupport.requiredBoolean(properties, "enabled")).isFalse();
    assertThat(ConfigSupport.positiveInt(properties, "int")).isEqualTo(10);
    assertThat(ConfigSupport.requiredInteger(properties, "int")).isEqualTo(10);
    assertThat(ConfigSupport.requiredInteger(properties, "zero")).isZero();
    properties.setProperty("integer", " -2 ");
    assertThat(ConfigSupport.requiredInteger(properties, "integer")).isEqualTo(-2);
    assertThat(ConfigSupport.positiveLong(properties, "long")).isEqualTo(20);
    assertThat(ConfigSupport.nonNegativeLong(properties, "zero")).isZero();
    assertThat(ConfigSupport.positiveDouble(properties, "double")).isEqualTo(1.5);
    assertThat(ConfigSupport.requiredZoneId(properties, "time-zone"))
        .isEqualTo(ZoneId.of("Asia/Tehran"));
  }

  @Test
  void shouldRejectInvalidValues() {
    Properties properties = new Properties();
    properties.setProperty("boolean", "maybe");
    properties.setProperty("zero", "0");
    properties.setProperty("negative", "-1");
    properties.setProperty("nan", "NaN");
    properties.setProperty("time-zone", "not-a-time-zone");

    assertThatThrownBy(() -> ConfigSupport.requiredProperty(properties, "missing"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ConfigSupport.requiredBoolean(properties, "boolean"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ConfigSupport.requiredBoolean(properties, "missing"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing");

    properties.setProperty("boolean", " ");
    assertThatThrownBy(() -> ConfigSupport.requiredBoolean(properties, "boolean"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("boolean");

    assertThatThrownBy(() -> ConfigSupport.positiveInt(properties, "zero"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ConfigSupport.requiredInteger(properties, "missing"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing");

    for (String value : new String[] {" ", "invalid", "2147483648"}) {
      properties.setProperty("integer", value);
      assertThatThrownBy(() -> ConfigSupport.requiredInteger(properties, "integer"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    assertThatThrownBy(() -> ConfigSupport.positiveLong(properties, "zero"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ConfigSupport.nonNegativeLong(properties, "negative"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ConfigSupport.positiveDouble(properties, "nan"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ConfigSupport.requiredZoneId(properties, "time-zone"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("time-zone");
  }
}
