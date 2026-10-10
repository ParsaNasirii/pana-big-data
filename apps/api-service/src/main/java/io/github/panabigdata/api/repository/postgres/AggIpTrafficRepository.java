package io.github.panabigdata.api.repository.postgres;

import io.github.panabigdata.api.projection.IpBandwidth;
import io.github.panabigdata.api.projection.IpBandwidthProjection;
import io.github.panabigdata.api.projection.IpTrafficSummaryProjection;
import io.github.panabigdata.api.projection.TrafficSummary;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class AggIpTrafficRepository {
  private final NamedParameterJdbcTemplate jdbc;

  public IpTrafficSummaryProjection getTrafficSummaryForIp(
      String ip, OffsetDateTime startTime, OffsetDateTime endTime) {
    String sql =
        """
        SELECT SUM(total_in_bytes) AS total_in_bytes,
               SUM(total_out_bytes) AS total_out_bytes,
               SUM(total_bytes) AS total_bytes
        FROM agg_ip_traffic
        WHERE ip_address = CAST(:ip AS inet)
          AND kafka_timestamp_bucket BETWEEN :startTime AND :endTime
        """;
    MapSqlParameterSource parameters = timeParameters(startTime, endTime).addValue("ip", ip);
    return jdbc.queryForObject(
        sql,
        parameters,
        (rs, rowNum) ->
            new TrafficSummary(
                nullableLong(rs, "total_in_bytes"),
                nullableLong(rs, "total_out_bytes"),
                nullableLong(rs, "total_bytes")));
  }

  public List<IpBandwidthProjection> getTopIpsByBandwidth(
      OffsetDateTime startTime, OffsetDateTime endTime, Pageable pageable) {
    String sql =
        """
        SELECT host(ip_address) AS ip_address, SUM(total_bytes) AS total_bytes
        FROM agg_ip_traffic
        WHERE kafka_timestamp_bucket BETWEEN :startTime AND :endTime
        GROUP BY ip_address
        ORDER BY total_bytes DESC, ip_address ASC
        """;
    MapSqlParameterSource parameters = timeParameters(startTime, endTime);
    if (pageable.isPaged()) {
      sql += " LIMIT :limit OFFSET :offset";
      parameters.addValue("limit", pageable.getPageSize()).addValue("offset", pageable.getOffset());
    }
    return jdbc.query(
        sql,
        parameters,
        (rs, rowNum) -> new IpBandwidth(rs.getString("ip_address"), rs.getLong("total_bytes")));
  }

  private static MapSqlParameterSource timeParameters(OffsetDateTime start, OffsetDateTime end) {
    return new MapSqlParameterSource().addValue("startTime", start).addValue("endTime", end);
  }

  private static Long nullableLong(ResultSet rs, String column) throws SQLException {
    long value = rs.getLong(column);
    return rs.wasNull() ? null : value;
  }
}
