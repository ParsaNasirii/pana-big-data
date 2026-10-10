package io.github.panabigdata.api.repository.postgres;

import io.github.panabigdata.api.entity.postgres.AggIpConnections;
import io.github.panabigdata.api.entity.postgres.AggIpConnectionsId;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.StringJoiner;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class AggIpConnectionsRepository {
  private static final String FILTER =
      """
      FROM agg_ip_to_ip_connection
      WHERE (src_ip = CAST(:ip AS inet) OR dst_ip = CAST(:ip AS inet))
        AND kafka_timestamp_bucket BETWEEN :startTime AND :endTime
      """;

  private final NamedParameterJdbcTemplate jdbc;

  public Page<AggIpConnections> getConnectionsForIp(
      String ip, OffsetDateTime startTime, OffsetDateTime endTime, Pageable pageable) {
    MapSqlParameterSource parameters =
        new MapSqlParameterSource()
            .addValue("ip", ip)
            .addValue("startTime", startTime)
            .addValue("endTime", endTime);
    String sql =
        """
        SELECT kafka_timestamp_bucket, host(src_ip) AS src_ip,
               host(dst_ip) AS dst_ip, total_flows, total_bytes
        """
            + FILTER
            + orderBy(pageable.getSort());
    if (pageable.isPaged()) {
      sql += " LIMIT :limit OFFSET :offset";
      parameters.addValue("limit", pageable.getPageSize()).addValue("offset", pageable.getOffset());
    }
    List<AggIpConnections> content =
        jdbc.query(
            sql,
            parameters,
            (rs, rowNum) -> {
              AggIpConnections result = new AggIpConnections();
              result.setId(
                  new AggIpConnectionsId(
                      rs.getObject("kafka_timestamp_bucket", OffsetDateTime.class),
                      rs.getString("src_ip"),
                      rs.getString("dst_ip")));
              result.setTotalFlows(rs.getLong("total_flows"));
              result.setTotalBytes(rs.getLong("total_bytes"));
              return result;
            });
    long total = jdbc.queryForObject("SELECT COUNT(*) " + FILTER, parameters, Long.class);
    return new PageImpl<>(content, pageable, total);
  }

  private static String orderBy(Sort sort) {
    StringJoiner columns = new StringJoiner(", ", " ORDER BY ", "");
    for (Sort.Order order : sort) {
      String column =
          switch (order.getProperty()) {
            case "id.kafkaTimestampBucket", "kafkaTimestampBucket" -> "kafka_timestamp_bucket";
            case "id.srcIp", "srcIp" -> "src_ip";
            case "id.dstIp", "dstIp" -> "dst_ip";
            case "totalFlows" -> "total_flows";
            case "totalBytes" -> "total_bytes";
            default ->
                throw new IllegalArgumentException(
                    "Unsupported connection sort: " + order.getProperty());
          };
      columns.add(column + " " + order.getDirection().name());
    }
    columns.add("kafka_timestamp_bucket ASC");
    columns.add("src_ip ASC");
    columns.add("dst_ip ASC");
    return columns.toString();
  }
}
