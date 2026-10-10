package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.metrics.KafkaMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class MetricsHttpServerTest {

  @Test
  void exposesPrometheusMetrics() throws Exception {

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    MetricsHttpServer server = new MetricsHttpServer(registry, 3333);

    server.start();

    KafkaMetrics metrics = new KafkaMetrics(registry, "test");

    metrics.recordValid(5);

    int port = server.getPort();

    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/metrics")).build();

    String body =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body();

    assertThat(body).contains("kafka_records_valid");

    server.stop();
  }
}
