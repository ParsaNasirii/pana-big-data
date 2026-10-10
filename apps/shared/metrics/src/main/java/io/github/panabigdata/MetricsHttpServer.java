package io.github.panabigdata;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public final class MetricsHttpServer {

  private final HttpServer server;
  private final PrometheusMeterRegistry registry;
  private final int port;

  public MetricsHttpServer(PrometheusMeterRegistry registry, int port) throws IOException {

    this.registry = registry;
    this.port = port;

    server = HttpServer.create(new InetSocketAddress(port), 0);

    server.createContext(
        "/metrics",
        exchange -> {
          byte[] response = registry.scrape().getBytes(StandardCharsets.UTF_8);

          exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");

          exchange.sendResponseHeaders(200, response.length);

          exchange.getResponseBody().write(response);

          exchange.close();
        });
  }

  public void start() {
    server.start();
  }

  public void stop() {
    server.stop(1);
  }

  public int getPort() {
    return port;
  }

  public PrometheusMeterRegistry getRegistry() {
    return registry;
  }
}
