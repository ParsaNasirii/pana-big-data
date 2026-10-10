package io.github.panabigdata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class ConfigSupport {
  private static final String APPLICATION_PROPERTIES = "application.properties";
  private static final Path DEFAULT_CONFIG_FILE = Path.of("/app/config", APPLICATION_PROPERTIES);

  private ConfigSupport() {}

  public static Properties loadApplicationProperties(Class<?> owner) {
    return loadApplicationProperties(owner, DEFAULT_CONFIG_FILE);
  }

  static Properties loadApplicationProperties(Class<?> owner, Path configFile) {
    boolean externalFile = Files.isRegularFile(configFile);
    String source = externalFile ? configFile.toString() : APPLICATION_PROPERTIES;
    try (InputStream input =
        externalFile
            ? Files.newInputStream(configFile)
            : owner.getClassLoader().getResourceAsStream(APPLICATION_PROPERTIES)) {
      if (input == null) {
        throw new IllegalStateException(
            APPLICATION_PROPERTIES
                + " not found at "
                + DEFAULT_CONFIG_FILE
                + " or on the classpath");
      }
      Properties properties = new Properties();
      properties.load(input);
      return properties;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load " + source, e);
    }
  }

  public static Properties copyProperties(Properties properties) {
    Properties copy = new Properties();
    copy.putAll(properties);
    return copy;
  }

  public static void overrideRequiredEnvironment(
      Properties properties,
      String propertyName,
      Map<String, String> environment,
      String environmentName) {
    properties.setProperty(propertyName, requiredEnvironment(environment, environmentName));
  }

  public static void overrideOptionalEnvironment(
      Properties properties,
      String propertyName,
      Map<String, String> environment,
      String environmentName) {
    String value = environment.get(environmentName);
    if (value != null && !value.isBlank()) {
      properties.setProperty(propertyName, value);
    }
  }

  public static String requiredEnvironment(Map<String, String> environment, String name) {
    String value = environment.get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " must be configured");
    }
    return value;
  }

  public static String requiredProperty(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(key + " must be configured");
    }
    return value;
  }

  public static String optionalProperty(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }

  public static boolean requiredBoolean(Properties properties, String key) {
    String value = requiredProperty(properties, key);
    if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
      throw new IllegalArgumentException(key + " must be true or false");
    }
    return Boolean.parseBoolean(value);
  }

  public static int requiredInteger(Properties properties, String key) {
    return Integer.parseInt(requiredProperty(properties, key).trim());
  }

  public static int positiveInt(Properties properties, String key) {
    int value = requiredInteger(properties, key);
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be greater than zero");
    }
    return value;
  }

  public static long positiveLong(Properties properties, String key) {
    long value = Long.parseLong(requiredProperty(properties, key).trim());
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be greater than zero");
    }
    return value;
  }

  public static long optionalLong(Properties properties, String key, long defaultValue) {
    String value = optionalProperty(properties, key);
    if (value == null) {
      return defaultValue;
    }
    long result = Long.parseLong(value);
    if (result <= 0) {
      throw new IllegalArgumentException(key + " must be greater than zero");
    }
    return result;
  }

  public static long nonNegativeLong(Properties properties, String key) {
    long value = Long.parseLong(requiredProperty(properties, key));
    if (value < 0) {
      throw new IllegalArgumentException(key + " must not be negative");
    }
    return value;
  }

  public static double positiveDouble(Properties properties, String key) {
    double value = Double.parseDouble(requiredProperty(properties, key));
    if (!Double.isFinite(value) || value <= 0) {
      throw new IllegalArgumentException(key + " must be a positive finite number");
    }
    return value;
  }

  public static ZoneId requiredZoneId(Properties properties, String key) {
    try {
      return ZoneId.of(requiredProperty(properties, key));
    } catch (DateTimeException e) {
      throw new IllegalArgumentException(key + " must be a valid time-zone ID", e);
    }
  }

  public static int requiredPort(Properties properties, String key) {
    int port = positiveInt(properties, key);
    if (port > 65535) {
      throw new IllegalArgumentException(key + " must be between 1 and 65535");
    }
    return port;
  }

  public static List<String> requiredArrayOrSingle(
      Properties properties, String singleKey, String arrayKey) {
    String configuredHosts = ConfigSupport.optionalProperty(properties, arrayKey);
    if (configuredHosts == null) {
      configuredHosts = ConfigSupport.requiredProperty(properties, singleKey);
    }

    List<String> hosts =
        Arrays.stream(configuredHosts.split(","))
            .map(String::trim)
            .filter(host -> !host.isEmpty())
            .distinct()
            .toList();

    if (hosts.isEmpty()) {
      throw new IllegalArgumentException(arrayKey + " must contain at least one FE host");
    }
    return List.copyOf(hosts);
  }
}
