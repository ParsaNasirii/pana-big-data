package io.github.panabigdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

class StarRocksContainerExtension implements BeforeAllCallback, AfterAllCallback {

  static final String PASSWORD = "test-password";
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private final Network network = Network.newNetwork();
  private final GenericContainer<?> fe =
      new GenericContainer<>(
              System.getProperty(
                  "test.starrocks.image",
                  System.getenv()
                      .getOrDefault(
                          "TEST_STARROCKS_IMAGE", "docker.io/starrocks/fe-ubuntu:4.0.12")))
          .withNetwork(network)
          .withCommand(
              "bash",
              "-ec",
              "sed -i 's/-Xmx8192m/-Xmx1024m/' /opt/starrocks/fe/conf/fe.conf; "
                  + "exec /opt/starrocks/fe/bin/start_fe.sh --logconsole")
          .withExposedPorts(9030)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
  private final GenericContainer<?> be =
      new GenericContainer<>(
              System.getProperty(
                  "test.starrocks.be.image",
                  System.getenv()
                      .getOrDefault(
                          "TEST_STARROCKS_BE_IMAGE", "docker.io/starrocks/be-ubuntu:4.0.12")))
          .withNetwork(network)
          .withCommand(
              "bash",
              "-ec",
              "mkdir -p /opt/starrocks/be/storage; "
                  + "printf '\nmem_limit = 1G\nnum_cores = 2\n' >> /opt/starrocks/be/conf/be.conf; "
                  + "exec /opt/starrocks/be/bin/start_be.sh --logconsole")
          .withExposedPorts(8040)
          .waitingFor(
              Wait.forHttp("/api/health").forPort(8040).withStartupTimeout(STARTUP_TIMEOUT));

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    try {
      fe.start();
      be.start();
      String backendIp =
          be.getContainerInfo()
              .getNetworkSettings()
              .getNetworks()
              .values()
              .iterator()
              .next()
              .getIpAddress();
      try (Connection connection = DriverManager.getConnection(getJdbcUrl(), "root", "");
          Statement statement = connection.createStatement()) {
        statement.execute("ALTER SYSTEM ADD BACKEND '" + backendIp + ":9050'");
        waitForBackend(statement);
        statement.execute("SET PASSWORD FOR 'root' = PASSWORD('" + PASSWORD + "')");
      }
    } catch (Exception e) {
      stop();
      throw e;
    }
  }

  private void waitForBackend(Statement statement) throws Exception {
    long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try (ResultSet result = statement.executeQuery("SHOW BACKENDS")) {
        if (result.next()
            && result.getBoolean("Alive")
            && !result.getString("TotalCapacity").startsWith("0.000 ")) {
          return;
        }
      }
      Thread.sleep(500);
    }
    throw new IllegalStateException("StarRocks BE did not become ready within " + STARTUP_TIMEOUT);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    stop();
  }

  private void stop() {
    try {
      be.stop();
    } finally {
      try {
        fe.stop();
      } finally {
        network.close();
      }
    }
  }

  Connection openConnection() throws SQLException {
    return DriverManager.getConnection(getJdbcUrl(), "root", PASSWORD);
  }

  private String getJdbcUrl() {
    return "jdbc:mysql://"
        + fe.getHost()
        + ":"
        + fe.getMappedPort(9030)
        + "/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000&socketTimeout=30000";
  }

  String getBackendHost() {
    return be.getHost();
  }

  int getBackendHttpPort() {
    return be.getMappedPort(8040);
  }
}
