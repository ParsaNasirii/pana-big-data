package io.github.panabigdata.api.repository.starrocks;

import io.github.panabigdata.api.dto.starrocks.ConnectionInfo;
import io.github.panabigdata.api.dto.starrocks.IpBandwidth;
import io.github.panabigdata.api.dto.starrocks.ProtocolUsage;
import io.github.panabigdata.api.dto.starrocks.TrafficSummary;
import java.util.List;
import java.util.StringJoiner;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class NetflowRawRepository {

  private static final String TIME_FILTER =
      """
      FROM netflow_raw n
      WHERE n.KAFKA_TIMESTAMP_MS BETWEEN :startTime AND :endTime
        AND n.KAFKA_DAY BETWEEN CAST(FROM_UNIXTIME(:startSecond) AS DATE)
                            AND CAST(FROM_UNIXTIME(:endSecond) AS DATE)
      """;

  private static final String IP_TIME_FILTER =
      TIME_FILTER + " AND (n.IPV4_SRC_ADDR = :ip OR n.IPV4_DST_ADDR = :ip) ";

  private final NamedParameterJdbcTemplate jdbc;

  public NetflowRawRepository(@Qualifier("starrocksDataSource") DataSource dataSource) {
    jdbc = new NamedParameterJdbcTemplate(dataSource);
  }

  public TrafficSummary getTrafficSummaryForIp(String ip, Long startTime, Long endTime) {
    return jdbc.queryForObject(
        "SELECT SUM(n.IN_BYTES) AS totalIn, SUM(n.OUT_BYTES) AS totalOut " + IP_TIME_FILTER,
        filterParameters(ip, startTime, endTime),
        (rs, rowNum) ->
            new TrafficSummary(
                rs.getObject("totalIn", Long.class), rs.getObject("totalOut", Long.class)));
  }

  public Page<ConnectionInfo> getConnectionsForIp(
      String ip, Long startTime, Long endTime, Pageable pageable) {
    MapSqlParameterSource parameters = filterParameters(ip, startTime, endTime);
    String sql =
        "SELECT n.IPV4_SRC_ADDR AS srcIp, n.IPV4_DST_ADDR AS dstIp "
            + IP_TIME_FILTER
            + connectionOrder(pageable.getSort());
    List<ConnectionInfo> content =
        jdbc.query(
            paginate(sql, pageable, parameters),
            parameters,
            (rs, rowNum) -> new ConnectionInfo(rs.getString("srcIp"), rs.getString("dstIp")));
    long total = jdbc.queryForObject("SELECT COUNT(*) " + IP_TIME_FILTER, parameters, Long.class);
    return new PageImpl<>(content, pageable, total);
  }

  public Page<IpBandwidth> getTopIpsByBandwidth(Long startTime, Long endTime, Pageable pageable) {
    MapSqlParameterSource parameters = timeParameters(startTime, endTime);
    String sql =
        "SELECT n.IPV4_SRC_ADDR AS ip, SUM(n.IN_BYTES + n.OUT_BYTES) AS totalBandwidth "
            + TIME_FILTER
            + " GROUP BY n.IPV4_SRC_ADDR ORDER BY totalBandwidth DESC, n.IPV4_SRC_ADDR ASC";
    List<IpBandwidth> content =
        jdbc.query(
            paginate(sql, pageable, parameters),
            parameters,
            (rs, rowNum) -> new IpBandwidth(rs.getString("ip"), rs.getLong("totalBandwidth")));
    long total =
        jdbc.queryForObject(
            "SELECT COUNT(DISTINCT n.IPV4_SRC_ADDR) " + TIME_FILTER, parameters, Long.class);
    return new PageImpl<>(content, pageable, total);
  }

  public Page<ProtocolUsage> getTopUsedProtocols(Long startTime, Long endTime, Pageable pageable) {
    MapSqlParameterSource parameters = timeParameters(startTime, endTime);
    String sql =
        "SELECT n.PROTOCOL AS protocol, COUNT(*) AS usageCount "
            + TIME_FILTER
            + " GROUP BY n.PROTOCOL ORDER BY usageCount DESC, n.PROTOCOL ASC";
    List<ProtocolUsage> content =
        jdbc.query(
            paginate(sql, pageable, parameters),
            parameters,
            (rs, rowNum) -> new ProtocolUsage(rs.getInt("protocol"), rs.getLong("usageCount")));
    long total =
        jdbc.queryForObject(
            "SELECT COUNT(DISTINCT n.PROTOCOL) " + TIME_FILTER, parameters, Long.class);
    return new PageImpl<>(content, pageable, total);
  }

  private static MapSqlParameterSource filterParameters(String ip, Long startTime, Long endTime) {
    return timeParameters(startTime, endTime).addValue("ip", ip);
  }

  private static MapSqlParameterSource timeParameters(Long startTime, Long endTime) {
    return new MapSqlParameterSource()
        .addValue("startTime", startTime)
        .addValue("endTime", endTime)
        .addValue("startSecond", startTime == null ? null : startTime / 1000)
        .addValue("endSecond", endTime == null ? null : endTime / 1000);
  }

  private static String paginate(String sql, Pageable pageable, MapSqlParameterSource parameters) {
    if (pageable.isUnpaged()) {
      return sql;
    }
    parameters.addValue("limit", pageable.getPageSize()).addValue("offset", pageable.getOffset());
    return sql + " LIMIT :limit OFFSET :offset";
  }

  private static String connectionOrder(Sort sort) {
    StringJoiner columns = new StringJoiner(", ", " ORDER BY ", "");
    for (Sort.Order order : sort) {
      String column =
          switch (order.getProperty()) {
            case "srcIp", "ipv4SrcAddr" -> "n.IPV4_SRC_ADDR";
            case "dstIp", "ipv4DstAddr" -> "n.IPV4_DST_ADDR";
            case "kafkaTimestampMs" -> "n.KAFKA_TIMESTAMP_MS";
            default ->
                throw new IllegalArgumentException(
                    "Unsupported connection sort: " + order.getProperty());
          };
      columns.add(column + " " + order.getDirection().name());
    }
    columns.add("n.KAFKA_TIMESTAMP_MS ASC");
    columns.add("n.IPV4_SRC_ADDR ASC");
    columns.add("n.IPV4_DST_ADDR ASC");
    return columns.toString();
  }
}
