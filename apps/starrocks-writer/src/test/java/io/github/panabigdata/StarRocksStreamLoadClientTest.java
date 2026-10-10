package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.github.panabigdata.config.StarRocksConfig;
import io.github.panabigdata.metrics.StarRocksWriterMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StarRocksStreamLoadClientTest {

  private HttpServer server;
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final StarRocksWriterMetrics metrics = new StarRocksWriterMetrics(registry);

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
    registry.close();
  }

  @Test
  @Timeout(10)
  void shouldFollowRedirectAndSendStreamLoadRequest() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

    AtomicReference<String> firstRequestMethod = new AtomicReference<>();
    AtomicReference<String> redirectedRequestMethod = new AtomicReference<>();
    AtomicReference<String> redirectedRequestPath = new AtomicReference<>();
    AtomicReference<String> authorizationHeader = new AtomicReference<>();
    AtomicReference<String> labelHeader = new AtomicReference<>();
    AtomicReference<String> formatHeader = new AtomicReference<>();
    AtomicReference<String> contentTypeHeader = new AtomicReference<>();
    AtomicReference<String> columnsHeader = new AtomicReference<>();
    AtomicReference<String> separatorHeader = new AtomicReference<>();
    AtomicReference<String> timezoneHeader = new AtomicReference<>();
    AtomicReference<String> stripOuterArrayHeader = new AtomicReference<>();
    AtomicReference<String> strictModeHeader = new AtomicReference<>();
    AtomicReference<String> maxFilterRatioHeader = new AtomicReference<>();
    AtomicReference<String> expectHeader = new AtomicReference<>();
    AtomicReference<String> requestBody = new AtomicReference<>();

    server.createContext(
        "/api/panabigdata/netflow_raw/_stream_load",
        exchange -> {
          firstRequestMethod.set(exchange.getRequestMethod());
          exchange.getRequestBody().close();

          String redirectLocation =
              "http://127.0.0.1:" + server.getAddress().getPort() + "/be-stream-load";
          exchange.getResponseHeaders().add("Location", redirectLocation);
          exchange.sendResponseHeaders(307, -1);
          exchange.close();
        });

    server.createContext(
        "/be-stream-load",
        exchange -> {
          redirectedRequestMethod.set(exchange.getRequestMethod());
          redirectedRequestPath.set(exchange.getRequestURI().getPath());
          authorizationHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
          labelHeader.set(exchange.getRequestHeaders().getFirst("label"));
          formatHeader.set(exchange.getRequestHeaders().getFirst("format"));
          contentTypeHeader.set(exchange.getRequestHeaders().getFirst("Content-Type"));
          columnsHeader.set(exchange.getRequestHeaders().getFirst("columns"));
          separatorHeader.set(exchange.getRequestHeaders().getFirst("column_separator"));
          timezoneHeader.set(exchange.getRequestHeaders().getFirst("timezone"));
          stripOuterArrayHeader.set(exchange.getRequestHeaders().getFirst("strip_outer_array"));
          strictModeHeader.set(exchange.getRequestHeaders().getFirst("strict_mode"));
          maxFilterRatioHeader.set(exchange.getRequestHeaders().getFirst("max_filter_ratio"));
          expectHeader.set(exchange.getRequestHeaders().getFirst("Expect"));

          requestBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

          String responseJson =
              """
                    {
                      "TxnId": 98765,
                      "Label": "batch-2-12345",
                      "Status": "Success",
                      "Message": "OK",
                      "NumberTotalRows": 2,
                      "NumberLoadedRows": 2,
                      "NumberFilteredRows": 0,
                      "NumberUnselectedRows": 0,
                      "LoadBytes": 128,
                      "LoadTimeMs": 5
                    }
                    """;

          byte[] responseBytes = responseJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, responseBytes.length);
          exchange.getResponseBody().write(responseBytes);
          exchange.close();
        });

    server.start();

    StarRocksConfig config = createConfig(server.getAddress().getPort());
    HttpClient httpClient =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    StarRocksStreamLoadClient client =
        new StarRocksStreamLoadClient(config, httpClient, new ObjectMapper(), metrics);

    StreamLoadResult result =
        client.load(
            "batch-2-12345",
            List.of(StarRocksTestData.row(), StarRocksTestData.row()),
            StarRocksTestData.COLUMNS);

    assertThat(firstRequestMethod.get()).isEqualTo("PUT");
    assertThat(redirectedRequestMethod.get()).isEqualTo("PUT");
    assertThat(redirectedRequestPath.get()).isEqualTo("/be-stream-load");

    String expectedAuthorization =
        "Basic "
            + Base64.getEncoder()
                .encodeToString("test-user:test-password".getBytes(StandardCharsets.UTF_8));
    assertThat(authorizationHeader.get()).isEqualTo(expectedAuthorization);
    assertThat(labelHeader.get()).isEqualTo("batch-2-12345");
    assertThat(formatHeader.get()).isEqualTo("csv");
    assertThat(contentTypeHeader.get()).isEqualTo("text/csv; charset=UTF-8");
    assertThat(columnsHeader.get())
        .isEqualTo(String.join(",", StarRocksTestData.COLUMNS) + ",KAFKA_TIMESTAMP_MS");
    assertThat(separatorHeader.get()).isEqualTo(",");
    assertThat(timezoneHeader.get()).isEqualTo("Asia/Tehran");
    assertThat(stripOuterArrayHeader.get()).isNull();
    assertThat(strictModeHeader.get()).isEqualTo("true");
    assertThat(maxFilterRatioHeader.get()).isEqualTo("0");
    assertThat(expectHeader.get()).isEqualToIgnoringCase("100-Continue");

    String[] lines = requestBody.get().split("\n");
    assertThat(lines).hasSize(2);
    for (String line : lines) {
      String[] values = line.split(",", -1);
      assertThat(values).hasSize(StarRocksTestData.COLUMNS.size() + 1);
      assertThat(values[0]).isEqualTo("10.1.2.3");
      assertThat(values[1]).isEqualTo("10.9.8.7");
      assertThat(values[StarRocksTestData.COLUMNS.size()]).isEqualTo("1723456789000");
    }
    assertThat(requestBody.get()).endsWith("\n");

    assertThat(result.status()).isEqualTo("Success");
    assertThat(result.txnId()).isEqualTo(98765L);
    assertThat(result.numberLoadedRows()).isEqualTo(2L);
    assertThat(result.isAccepted()).isTrue();
  }

  @Test
  void shouldFailOverToNextFeWhenFirstFeIsUnreachable() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

    server.createContext(
        "/api/panabigdata/netflow_raw/_stream_load",
        exchange -> {
          exchange.getRequestBody().close();

          String responseJson =
              """
                    {
                      "TxnId": 12345,
                      "Label": "failover-test",
                      "Status": "Success",
                      "Message": "OK",
                      "NumberTotalRows": 1,
                      "NumberLoadedRows": 1,
                      "NumberFilteredRows": 0,
                      "NumberUnselectedRows": 0,
                      "LoadBytes": 128,
                      "LoadTimeMs": 5
                    }
                    """;

          byte[] responseBytes = responseJson.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, responseBytes.length);
          exchange.getResponseBody().write(responseBytes);
          exchange.close();
        });

    server.start();

    StarRocksConfig config =
        createMultiFeConfig(server.getAddress().getPort(), "127.0.0.2,127.0.0.1");
    StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(config, metrics);

    StreamLoadResult result =
        client.load("failover-test", List.of(StarRocksTestData.row()), StarRocksTestData.COLUMNS);
    assertThat(result.status()).isEqualTo("Success");
    assertThat(result.numberLoadedRows()).isEqualTo(1L);
    assertThat(registry.get("starrocks_writer_stream_load_attempts").counter().count())
        .isEqualTo(2);
    assertThat(registry.get("starrocks_writer_fe_failovers").counter().count()).isEqualTo(1);
  }

  @Test
  void shouldNotFailOverWhenFirstFeReturnsUnauthorized() throws Exception {
    AtomicInteger requestCount = new AtomicInteger();

    server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);

    server.createContext(
        "/api/panabigdata/netflow_raw/_stream_load",
        exchange -> {
          requestCount.incrementAndGet();
          exchange.getRequestBody().close();

          byte[] responseBytes =
              """
                    {
                      "status": "FAILED",
                      "message": "Access denied"
                    }
                    """
                  .getBytes(StandardCharsets.UTF_8);

          exchange.sendResponseHeaders(401, responseBytes.length);
          exchange.getResponseBody().write(responseBytes);
          exchange.close();
        });

    server.start();

    StarRocksConfig config =
        createMultiFeConfig(server.getAddress().getPort(), "127.0.0.1,127.0.0.2");
    StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(config, metrics);

    assertThatThrownBy(
            () ->
                client.load(
                    "unauthorized-no-failover",
                    List.of(StarRocksTestData.row()),
                    StarRocksTestData.COLUMNS))
        .isInstanceOf(NonRetryableStreamLoadException.class)
        .hasMessageContaining("non-retryable status 401");

    assertThat(requestCount.get()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403})
  void shouldTreatAuthenticationFailuresAsNonRetryable(int statusCode) throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

    server.createContext(
        "/api/panabigdata/netflow_raw/_stream_load",
        exchange -> {
          exchange.getRequestBody().close();

          byte[] responseBytes =
              """
                    {
                      "status": "FAILED",
                      "message": "Access denied"
                    }
                    """
                  .getBytes(StandardCharsets.UTF_8);

          exchange.sendResponseHeaders(statusCode, responseBytes.length);
          exchange.getResponseBody().write(responseBytes);
          exchange.close();
        });

    server.start();

    StarRocksConfig config = createConfig(server.getAddress().getPort());
    StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(config, metrics);

    assertThatThrownBy(
            () ->
                client.load(
                    "auth-failure-test",
                    List.of(StarRocksTestData.row()),
                    StarRocksTestData.COLUMNS))
        .isInstanceOf(NonRetryableStreamLoadException.class)
        .hasMessageContaining("non-retryable status " + statusCode);
  }

  @Test
  void shouldTreatServerErrorAsRetryableIOException() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

    server.createContext(
        "/api/panabigdata/netflow_raw/_stream_load",
        exchange -> {
          exchange.getRequestBody().close();

          byte[] responseBytes =
              """
                    {
                      "status": "FAILED",
                      "message": "Internal server error"
                    }
                    """
                  .getBytes(StandardCharsets.UTF_8);

          exchange.sendResponseHeaders(500, responseBytes.length);
          exchange.getResponseBody().write(responseBytes);
          exchange.close();
        });

    server.start();

    StarRocksConfig config = createConfig(server.getAddress().getPort());
    StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(config, metrics);

    assertThatThrownBy(
            () ->
                client.load(
                    "server-error-test",
                    List.of(StarRocksTestData.row()),
                    StarRocksTestData.COLUMNS))
        .isExactlyInstanceOf(IOException.class)
        .hasMessageContaining("status 500");
  }

  @Test
  void shouldRejectBlankLabel() {
    StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(createConfig(8030), metrics);

    assertThatThrownBy(
            () -> client.load(" ", List.of(StarRocksTestData.row()), StarRocksTestData.COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("label must not be blank");
  }

  @Test
  void shouldRejectEmptyRows() {
    StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(createConfig(8030), metrics);

    assertThatThrownBy(() -> client.load("batch-test", List.of(), StarRocksTestData.COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("rows must not be empty");
  }

  private static StarRocksConfig createConfig(int httpPort) {
    Properties properties = new Properties();
    properties.setProperty("starrocks.fe.host", "127.0.0.1");
    properties.setProperty("starrocks.http.port", Integer.toString(httpPort));
    properties.setProperty("starrocks.database", "panabigdata");
    properties.setProperty("starrocks.table", "netflow_raw");
    properties.setProperty("starrocks.batch.max.records", "1000");
    properties.setProperty("starrocks.connect.timeout.ms", "3000");
    properties.setProperty("starrocks.request.timeout.ms", "5000");
    properties.setProperty("metrics.port", "8380");
    properties.setProperty("starrocks.username", "test-user");
    properties.setProperty("starrocks.password", "test-password");
    return new StarRocksConfig(properties);
  }

  private static StarRocksConfig createMultiFeConfig(int httpPort, String feHosts) {
    Properties properties = new Properties();
    properties.setProperty("starrocks.fe.hosts", feHosts);
    properties.setProperty("starrocks.http.port", Integer.toString(httpPort));
    properties.setProperty("starrocks.database", "panabigdata");
    properties.setProperty("starrocks.table", "netflow_raw");
    properties.setProperty("starrocks.batch.max.records", "1000");
    properties.setProperty("starrocks.connect.timeout.ms", "1000");
    properties.setProperty("starrocks.request.timeout.ms", "3000");
    properties.setProperty("metrics.port", "8380");
    properties.setProperty("starrocks.username", "test-user");
    properties.setProperty("starrocks.password", "test-password");
    return new StarRocksConfig(properties);
  }
}
