package io.github.panabigdata.kafka;

import java.time.Duration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

final class KafkaSchemaRegistryExtension implements BeforeAllCallback, AfterAllCallback {

  private static final int REGISTRY_PORT = 8081;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private final Network network = Network.newNetwork();
  private final KafkaContainer kafka;
  private final GenericContainer<?> schemaRegistry;

  KafkaSchemaRegistryExtension(DockerImageName kafkaImage, DockerImageName registryImage) {
    kafka =
        new KafkaContainer(kafkaImage)
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");
    schemaRegistry =
        new GenericContainer<>(registryImage)
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("SCHEMA_REGISTRY_HEAP_OPTS", "-Xms128m -Xmx256m")
            .waitingFor(
                Wait.forHttp("/subjects")
                    .forPort(REGISTRY_PORT)
                    .withStartupTimeout(STARTUP_TIMEOUT));
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    try {
      kafka.start();
      schemaRegistry.start();
    } catch (RuntimeException e) {
      stop();
      throw e;
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {
    stop();
  }

  private void stop() {
    try {
      schemaRegistry.stop();
    } finally {
      try {
        kafka.stop();
      } finally {
        network.close();
      }
    }
  }

  String getBootstrapServers() {
    return kafka.getBootstrapServers();
  }

  String getSchemaRegistryUrl() {
    return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(REGISTRY_PORT);
  }
}
