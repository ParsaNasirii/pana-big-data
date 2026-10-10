package io.github.panabigdata.api.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.panabigdata.api.dto.starrocks.ConnectionInfo;
import io.github.panabigdata.api.dto.starrocks.IpBandwidth;
import io.github.panabigdata.api.dto.starrocks.ProtocolUsage;
import io.github.panabigdata.api.dto.starrocks.TrafficSummary;
import io.github.panabigdata.api.repository.starrocks.NetflowRawRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class NetflowRawRepositoryTest {

  private static final String TARGET_IP = "192.168.1.10";
  private NetflowRawRepository repository;
  private JdbcTemplate jdbc;

  @BeforeEach
  void setUp() {
    DriverManagerDataSource dataSource =
        new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    jdbc = new JdbcTemplate(dataSource);
    repository = new NetflowRawRepository(dataSource);
    jdbc.execute(
        "CREATE ALIAS from_unixtime FOR "
            + "'io.github.panabigdata.api.repository.NetflowRawRepositoryTest.fromUnixtime'");
    jdbc.execute(
        """
        CREATE TABLE netflow_raw (
          KAFKA_TIMESTAMP_MS BIGINT NOT NULL,
          IPV4_SRC_ADDR VARCHAR(15) NOT NULL,
          IPV4_DST_ADDR VARCHAR(15) NOT NULL,
          IN_BYTES BIGINT NOT NULL,
          OUT_BYTES BIGINT NOT NULL,
          PROTOCOL INT NOT NULL,
          KAFKA_DAY DATE GENERATED ALWAYS AS (
            CAST(FROM_UNIXTIME(KAFKA_TIMESTAMP_MS / 1000) AS DATE)
          )
        )
        """);
  }

  @AfterEach
  void tearDown() {
    jdbc.execute("SHUTDOWN");
  }

  public static LocalDateTime fromUnixtime(BigDecimal seconds) {
    return Instant.ofEpochSecond(seconds.longValue())
        .atZone(ZoneId.of("Asia/Tehran"))
        .toLocalDateTime();
  }

  @Test
  void summarizesBothIpDirectionsAndCountsSelfConnectionOnce() {
    insert(TARGET_IP, "8.8.8.8", 1690000L, 500L, 200L, 6);
    insert("10.0.0.1", TARGET_IP, 1690000L, 1000L, 400L, 6);
    insert(TARGET_IP, TARGET_IP, 1690000L, 50L, 20L, 6);
    insert("172.16.0.5", "1.1.1.1", 1690000L, 9999L, 9999L, 17);

    TrafficSummary summary = repository.getTrafficSummaryForIp(TARGET_IP, 1600000L, 1700000L);

    assertThat(summary.getTotalIn()).isEqualTo(1550L);
    assertThat(summary.getTotalOut()).isEqualTo(620L);
  }

  @Test
  void retainsBothInclusiveMillisecondEndpointsAcrossTehranMidnight() {
    long midnight = midnight();
    long start = midnight - 1;
    long end = midnight + 1;
    insert(TARGET_IP, "1.1.1.1", start - 1, 1000L, 1000L, 6);
    insert(TARGET_IP, "1.1.1.2", start, 1L, 10L, 6);
    insert("1.1.1.3", TARGET_IP, midnight, 2L, 20L, 6);
    insert(TARGET_IP, "1.1.1.4", end, 4L, 40L, 6);
    insert(TARGET_IP, "1.1.1.5", end + 1, 1000L, 1000L, 6);

    TrafficSummary summary = repository.getTrafficSummaryForIp(TARGET_IP, start, end);
    Page<ConnectionInfo> connections =
        repository.getConnectionsForIp(TARGET_IP, start, end, PageRequest.of(0, 2));

    assertThat(summary.getTotalIn()).isEqualTo(7L);
    assertThat(summary.getTotalOut()).isEqualTo(70L);
    assertThat(connections.getContent()).hasSize(2);
    assertThat(connections.getTotalElements()).isEqualTo(3L);
    assertThat(connections.getContent().get(0).getDstIp()).isEqualTo("1.1.1.2");
    assertThat(
            jdbc.queryForList(
                "SELECT DISTINCT KAFKA_DAY FROM netflow_raw ORDER BY KAFKA_DAY", LocalDate.class))
        .containsExactly(LocalDate.of(2026, 9, 20), LocalDate.of(2026, 9, 21));
  }

  @Test
  void includesMidnightWhenItIsTheExactInclusiveEnd() {
    long midnight = midnight();
    insert(TARGET_IP, "1.1.1.1", midnight - 1, 1L, 10L, 6);
    insert("1.1.1.2", TARGET_IP, midnight, 2L, 20L, 6);
    insert(TARGET_IP, "1.1.1.3", midnight + 1, 1000L, 1000L, 6);

    TrafficSummary summary = repository.getTrafficSummaryForIp(TARGET_IP, midnight - 1, midnight);
    Page<ConnectionInfo> page =
        repository.getConnectionsForIp(TARGET_IP, midnight - 1, midnight, PageRequest.of(0, 10));

    assertThat(summary.getTotalIn()).isEqualTo(3L);
    assertThat(summary.getTotalOut()).isEqualTo(30L);
    assertThat(page.getTotalElements()).isEqualTo(2L);
    assertThat(page.getContent())
        .containsExactly(
            new ConnectionInfo(TARGET_IP, "1.1.1.1"), new ConnectionInfo("1.1.1.2", TARGET_IP));
  }

  @Test
  void preservesDuplicateRowsAndUsesSameFilterForPageAndCount() {
    insert(TARGET_IP, "1.1.1.1", 1000L, 1L, 2L, 6);
    insert(TARGET_IP, "1.1.1.1", 1000L, 1L, 2L, 6);
    insert("1.1.1.2", TARGET_IP, 1001L, 1L, 2L, 6);
    insert(TARGET_IP, "1.1.1.3", 1003L, 100L, 200L, 6);
    insert("1.1.1.4", "1.1.1.5", 1001L, 100L, 200L, 6);

    TrafficSummary summary = repository.getTrafficSummaryForIp(TARGET_IP, 1000L, 1002L);
    Page<ConnectionInfo> page =
        repository.getConnectionsForIp(TARGET_IP, 1000L, 1002L, PageRequest.of(1, 2));

    assertThat(summary.getTotalIn()).isEqualTo(3L);
    assertThat(page.getTotalElements()).isEqualTo(3L);
    assertThat(page.getTotalPages()).isEqualTo(2);
    assertThat(page.getContent()).containsExactly(new ConnectionInfo("1.1.1.2", TARGET_IP));
  }

  @Test
  void returnsNullSumsAndEmptyPageOutsideDataRange() {
    insert(TARGET_IP, "1.1.1.1", 1000L, 1L, 2L, 6);

    TrafficSummary summary = repository.getTrafficSummaryForIp(TARGET_IP, 172800000L, 259200000L);
    Page<ConnectionInfo> page =
        repository.getConnectionsForIp(TARGET_IP, 172800000L, 259200000L, PageRequest.of(0, 2));

    assertThat(summary.getTotalIn()).isNull();
    assertThat(summary.getTotalOut()).isNull();
    assertThat(page.getContent()).isEmpty();
    assertThat(page.getTotalElements()).isZero();
  }

  @Test
  void ranksOnlyRequestedTimeRangeAndCountsMatchingGroups() {
    long midnight = midnight();
    long start = midnight - 1;
    long end = midnight + 1;
    insert("10.0.0.1", "8.8.8.8", start, 5000L, 5000L, 6);
    insert("10.0.0.2", "8.8.8.8", midnight, 1000L, 1000L, 17);
    insert("10.0.0.1", "1.1.1.1", end, 5000L, 5000L, 6);
    insert("10.0.0.3", "1.1.1.1", start - 1, 50000L, 50000L, 1);
    insert("10.0.0.4", "1.1.1.1", end + 1, 50000L, 50000L, 2);
    insert("10.0.0.5", "1.1.1.1", midnight - 86400000L, 50000L, 50000L, 3);

    Page<IpBandwidth> topIps = repository.getTopIpsByBandwidth(start, end, PageRequest.of(0, 1));
    Page<IpBandwidth> nextIps = repository.getTopIpsByBandwidth(start, end, PageRequest.of(1, 1));
    Page<ProtocolUsage> topProtocols =
        repository.getTopUsedProtocols(start, end, PageRequest.of(0, 1));
    Page<ProtocolUsage> nextProtocols =
        repository.getTopUsedProtocols(start, end, PageRequest.of(1, 1));

    assertThat(topIps.getTotalElements()).isEqualTo(2L);
    assertThat(topIps.getContent()).containsExactly(new IpBandwidth("10.0.0.1", 20000L));
    assertThat(nextIps.getContent()).containsExactly(new IpBandwidth("10.0.0.2", 2000L));
    assertThat(topProtocols.getTotalElements()).isEqualTo(2L);
    assertThat(topProtocols.getContent()).containsExactly(new ProtocolUsage(6, 2L));
    assertThat(topProtocols.getContent().get(0).getProtocolName()).isEqualTo("TCP");
    assertThat(nextProtocols.getContent()).containsExactly(new ProtocolUsage(17, 1L));
    assertThat(nextProtocols.getContent().get(0).getProtocolName()).isEqualTo("UDP");
  }

  @Test
  void appliesRequestedConnectionSortAndRejectsUnknownSqlProperties() {
    insert("1.1.1.1", TARGET_IP, 1000L, 1L, 2L, 6);
    insert("1.1.1.2", TARGET_IP, 1001L, 1L, 2L, 6);
    Page<ConnectionInfo> page =
        repository.getConnectionsForIp(
            TARGET_IP, 0L, 2000L, PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "srcIp")));
    assertThat(page.getContent())
        .extracting(ConnectionInfo::getSrcIp)
        .containsExactly("1.1.1.2", "1.1.1.1");

    assertThatThrownBy(
            () ->
                repository.getConnectionsForIp(
                    TARGET_IP,
                    0L,
                    2000L,
                    PageRequest.of(0, 10, Sort.by("srcIp; DROP TABLE netflow_raw"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Unsupported connection sort");
  }

  @Test
  void keepsResponseJsonFieldNames() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode summaryJson = mapper.readTree(mapper.writeValueAsString(new TrafficSummary(10L, 20L)));
    JsonNode protocolJson = mapper.readTree(mapper.writeValueAsString(new ProtocolUsage(6, 12L)));

    assertThat(summaryJson.size()).isEqualTo(2);
    assertThat(summaryJson.get("totalIn").longValue()).isEqualTo(10L);
    assertThat(summaryJson.get("totalOut").longValue()).isEqualTo(20L);
    assertThat(protocolJson.get("protocol").intValue()).isEqualTo(6);
    assertThat(protocolJson.get("protocolName").textValue()).isEqualTo("TCP");
    assertThat(protocolJson.get("usageCount").longValue()).isEqualTo(12L);
    assertThat(new ProtocolUsage(35, 1L).getProtocolName()).isEqualTo("IDPR");
    assertThat(new ProtocolUsage(204, 1L).getProtocolName()).isEqualTo("UNASSIGNED");
  }

  private long midnight() {
    return OffsetDateTime.parse("2026-09-21T00:00:00+03:30").toInstant().toEpochMilli();
  }

  private void insert(String src, String dst, long timestamp, long in, long out, int protocol) {
    jdbc.update(
        "INSERT INTO netflow_raw "
            + "(KAFKA_TIMESTAMP_MS, IPV4_SRC_ADDR, IPV4_DST_ADDR, IN_BYTES, OUT_BYTES, PROTOCOL) "
            + "VALUES (?, ?, ?, ?, ?, ?)",
        timestamp,
        src,
        dst,
        in,
        out,
        protocol);
  }
}
