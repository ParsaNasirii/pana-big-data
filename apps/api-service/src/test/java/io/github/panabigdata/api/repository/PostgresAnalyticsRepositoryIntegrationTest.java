package io.github.panabigdata.api.repository;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.api.entity.postgres.AggIpConnections;
import io.github.panabigdata.api.projection.IpBandwidthProjection;
import io.github.panabigdata.api.projection.IpTrafficSummaryProjection;
import io.github.panabigdata.api.projection.ProtocolUsageProjection;
import io.github.panabigdata.api.repository.postgres.AggIpConnectionsRepository;
import io.github.panabigdata.api.repository.postgres.AggIpTrafficRepository;
import io.github.panabigdata.api.repository.postgres.AggProtocolUsageRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresAnalyticsRepositoryIntegrationTest {
  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15-alpine");

  private AggIpTrafficRepository traffic;
  private AggIpConnectionsRepository connections;
  private AggProtocolUsageRepository protocols;
  private JdbcTemplate jdbc;
  private OffsetDateTime baseTime;

  @BeforeEach
  void setUp() {
    DriverManagerDataSource dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    jdbc = new JdbcTemplate(dataSource);
    NamedParameterJdbcTemplate postgresJdbc = new NamedParameterJdbcTemplate(dataSource);
    traffic = new AggIpTrafficRepository(postgresJdbc);
    connections = new AggIpConnectionsRepository(postgresJdbc);
    protocols = new AggProtocolUsageRepository(postgresJdbc);
    jdbc.execute(
        """
        CREATE TABLE IF NOT EXISTS agg_ip_traffic (
          kafka_timestamp_bucket TIMESTAMPTZ NOT NULL, ip_address INET NOT NULL,
          total_in_bytes BIGINT, total_out_bytes BIGINT, total_bytes BIGINT)
        """);
    jdbc.execute(
        """
        CREATE TABLE IF NOT EXISTS agg_ip_to_ip_connection (
          kafka_timestamp_bucket TIMESTAMPTZ NOT NULL, src_ip INET NOT NULL,
          dst_ip INET NOT NULL, total_flows BIGINT, total_bytes BIGINT)
        """);
    jdbc.execute(
        """
        CREATE TABLE IF NOT EXISTS agg_protocol_usage (
          kafka_timestamp_bucket TIMESTAMPTZ NOT NULL, protocol INT NOT NULL,
          total_bytes BIGINT)
        """);
    jdbc.execute("TRUNCATE agg_ip_traffic, agg_ip_to_ip_connection, agg_protocol_usage");
    baseTime = OffsetDateTime.of(2023, 10, 1, 12, 0, 0, 0, ZoneOffset.UTC);
  }

  @Test
  void summarizesTrafficAndPreservesNullForEmptyRange() {
    insertTraffic("192.168.1.10", baseTime, 100, 200);
    insertTraffic("192.168.1.10", baseTime.plusHours(1), 150, 50);
    insertTraffic("10.0.0.5", baseTime, 1000, 1000);

    IpTrafficSummaryProjection summary =
        traffic.getTrafficSummaryForIp(
            "192.168.1.10", baseTime.minusMinutes(1), baseTime.plusHours(2));
    assertThat(summary.getTotalInBytes()).isEqualTo(250);
    assertThat(summary.getTotalOutBytes()).isEqualTo(250);
    assertThat(summary.getTotalBytes()).isEqualTo(500);
    assertThat(
            traffic
                .getTrafficSummaryForIp("192.168.1.10", baseTime.plusDays(1), baseTime.plusDays(2))
                .getTotalBytes())
        .isNull();
  }

  @Test
  void ranksIpsWithinTimeRangeAndLimit() {
    insertTraffic("192.168.1.10", baseTime, 100, 200);
    insertTraffic("192.168.1.10", baseTime.plusHours(1), 150, 50);
    insertTraffic("10.0.0.5", baseTime, 1000, 1000);
    insertTraffic("172.16.0.1", baseTime.minusDays(2), 5000, 5000);

    List<IpBandwidthProjection> result =
        traffic.getTopIpsByBandwidth(
            baseTime.minusMinutes(1), baseTime.plusHours(2), PageRequest.of(0, 2));
    assertThat(result).hasSize(2);
    assertThat(result.get(0).getIpAddress()).isEqualTo("10.0.0.5");
    assertThat(result.get(0).getTotalBytes()).isEqualTo(2000);
    assertThat(result.get(1).getIpAddress()).isEqualTo("192.168.1.10");
    assertThat(result.get(1).getTotalBytes()).isEqualTo(500);
  }

  @Test
  void pagesConnectionsWithCountAndSort() {
    insertConnection(baseTime, "192.168.1.50", "8.8.8.8", 10, 100);
    insertConnection(baseTime.plusMinutes(10), "1.1.1.1", "192.168.1.50", 20, 200);
    insertConnection(baseTime, "10.0.0.1", "10.0.0.2", 30, 300);
    insertConnection(baseTime.minusDays(5), "192.168.1.50", "4.2.2.4", 40, 400);

    Page<AggIpConnections> first =
        connections.getConnectionsForIp(
            "192.168.1.50",
            baseTime.minusHours(1),
            baseTime.plusHours(1),
            PageRequest.of(0, 1, Sort.by(Sort.Direction.DESC, "totalBytes")));
    assertThat(first.getTotalElements()).isEqualTo(2);
    assertThat(first.getContent()).hasSize(1);
    assertThat(first.getContent().get(0).getId().getSrcIp()).isEqualTo("1.1.1.1");
    assertThat(first.getContent().get(0).getTotalBytes()).isEqualTo(200);

    Page<AggIpConnections> second =
        connections.getConnectionsForIp(
            "192.168.1.50",
            baseTime.minusHours(1),
            baseTime.plusHours(1),
            PageRequest.of(1, 1, Sort.by(Sort.Direction.DESC, "totalBytes")));
    assertThat(second.getTotalElements()).isEqualTo(2);
    assertThat(second.getContent().get(0).getId().getDstIp()).isEqualTo("8.8.8.8");
  }

  @Test
  void aggregatesAndRanksProtocols() {
    insertProtocol(baseTime, 6, 100);
    insertProtocol(baseTime.plusHours(1), 6, 200);
    insertProtocol(baseTime, 17, 1000);
    insertProtocol(baseTime, 1, 50);
    insertProtocol(baseTime.minusDays(2), 1, 10000);

    List<ProtocolUsageProjection> result =
        protocols.getTopUsedProtocols(
            baseTime.minusMinutes(1), baseTime.plusHours(2), PageRequest.of(0, 2));
    assertThat(result).hasSize(2);
    assertThat(result.get(0).getProtocol()).isEqualTo(17);
    assertThat(result.get(0).getTotalBytes()).isEqualTo(1000);
    assertThat(result.get(1).getProtocol()).isEqualTo(6);
    assertThat(result.get(1).getTotalBytes()).isEqualTo(300);
  }

  private void insertTraffic(String ip, OffsetDateTime time, long in, long out) {
    jdbc.update(
        "INSERT INTO agg_ip_traffic VALUES (?, CAST(? AS inet), ?, ?, ?)",
        time,
        ip,
        in,
        out,
        in + out);
  }

  private void insertConnection(
      OffsetDateTime time, String src, String dst, long flows, long bytes) {
    jdbc.update(
        "INSERT INTO agg_ip_to_ip_connection VALUES (?, CAST(? AS inet), CAST(? AS inet), ?, ?)",
        time,
        src,
        dst,
        flows,
        bytes);
  }

  private void insertProtocol(OffsetDateTime time, int protocol, long bytes) {
    jdbc.update("INSERT INTO agg_protocol_usage VALUES (?, ?, ?)", time, protocol, bytes);
  }
}
