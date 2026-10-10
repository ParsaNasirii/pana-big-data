package io.github.panabigdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.panabigdata.config.StarRocksConfig;
import io.github.panabigdata.metrics.StarRocksWriterMetrics;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StarRocksStreamLoadClient {

  private static final int TEMPORARY_REDIRECT = 307;
  private static final int PERMANENT_REDIRECT = 308;
  private static final int UNAUTHORIZED = 401;
  private static final int FORBIDDEN = 403;

  private final StarRocksConfig config;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final StarRocksWriterMetrics metrics;

  private final List<URI> streamLoadUris;
  private final String authorizationHeader;

  public StarRocksStreamLoadClient(StarRocksConfig config, StarRocksWriterMetrics metrics) {
    this(
        config,
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(config.getConnectTimeoutMillis()))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build(),
        new ObjectMapper(),
        metrics);
  }

  StarRocksStreamLoadClient(
      StarRocksConfig config,
      HttpClient httpClient,
      ObjectMapper objectMapper,
      StarRocksWriterMetrics metrics) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    this.streamLoadUris = buildStreamLoadUris(config);
    this.authorizationHeader = buildAuthorizationHeader(config);
  }

  public StreamLoadResult load(String label, List<StarRocksNetFlowRow> rows, List<String> columns)
      throws IOException, InterruptedException {
    if (label == null || label.isBlank()) {
      throw new IllegalArgumentException("label must not be blank");
    }
    Objects.requireNonNull(rows, "rows must not be null");
    if (rows.isEmpty()) {
      throw new IllegalArgumentException("rows must not be empty");
    }
    Objects.requireNonNull(columns, "columns must not be null");
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("columns must not be empty");
    }

    StringBuilder csv = new StringBuilder();
    for (StarRocksNetFlowRow row : rows) {
      row.appendCsv(csv);
    }
    String csvBody = csv.toString();
    IOException lastConnectionFailure = null;

    for (int index = 0; index < streamLoadUris.size(); index++) {
      URI feUri = streamLoadUris.get(index);
      HttpResponse<String> response;
      metrics.recordStreamLoadAttempt();

      try {
        HttpRequest request = buildRequest(feUri, label, csvBody, columns);
        response = send(request);
      } catch (IOException e) {
        if (index + 1 < streamLoadUris.size() && isFeConnectionFailure(e)) {
          lastConnectionFailure = e;
          metrics.recordFeFailover();
          log.warn("Failed to connect to StarRocks FE {}; trying next configured FE", feUri, e);
          continue;
        }
        throw e;
      }

      if (isRedirect(response.statusCode())) {
        URI redirectUri =
            response
                .headers()
                .firstValue("Location")
                .map(feUri::resolve)
                .orElseThrow(
                    () -> new IOException("Stream Load redirect response has no Location header"));
        HttpRequest redirectedRequest = buildRequest(redirectUri, label, csvBody, columns);
        response = send(redirectedRequest);
      }

      validateHttpStatus(response);
      return objectMapper.readValue(response.body(), StreamLoadResult.class);
    }

    throw new IOException(
        "Unable to connect to any configured StarRocks FE", lastConnectionFailure);
  }

  private HttpRequest buildRequest(URI uri, String label, String csvBody, List<String> columns) {
    return HttpRequest.newBuilder(uri)
        .timeout(Duration.ofMillis(config.getRequestTimeoutMillis()))
        .header("Authorization", authorizationHeader)
        .header("Content-Type", "text/csv; charset=UTF-8")
        .header("label", label)
        .header("format", "csv")
        .header("column_separator", ",")
        .header("columns", String.join(",", columns) + ",KAFKA_TIMESTAMP_MS")
        .header("timezone", "Asia/Tehran")
        .header("strict_mode", "true")
        .header("max_filter_ratio", "0")
        .expectContinue(true)
        .PUT(HttpRequest.BodyPublishers.ofString(csvBody, StandardCharsets.UTF_8))
        .build();
  }

  private HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private static boolean isFeConnectionFailure(IOException exception) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof ConnectException
          || current instanceof HttpConnectTimeoutException
          || current instanceof NoRouteToHostException
          || current instanceof UnknownHostException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean isRedirect(int statusCode) {
    return statusCode == TEMPORARY_REDIRECT || statusCode == PERMANENT_REDIRECT;
  }

  private static void validateHttpStatus(HttpResponse<String> response) throws IOException {
    int statusCode = response.statusCode();
    if (statusCode >= 200 && statusCode < 300) {
      return;
    }
    if (statusCode == UNAUTHORIZED || statusCode == FORBIDDEN) {
      throw new NonRetryableStreamLoadException(statusCode, response.body());
    }
    throw new IOException(
        "Stream Load HTTP request failed with status " + statusCode + ": " + response.body());
  }

  private static List<URI> buildStreamLoadUris(StarRocksConfig config) {
    return config.getFeHosts().stream().map(host -> buildStreamLoadUri(config, host)).toList();
  }

  private static URI buildStreamLoadUri(StarRocksConfig config, String host) {
    String uri =
        "http://"
            + host
            + ":"
            + config.getHttpPort()
            + "/api/"
            + config.getDatabase()
            + "/"
            + config.getTable()
            + "/_stream_load";
    return URI.create(uri);
  }

  private static String buildAuthorizationHeader(StarRocksConfig config) {
    String credentials = config.getUsername() + ":" + config.getPassword();
    String encoded =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    return "Basic " + encoded;
  }
}
