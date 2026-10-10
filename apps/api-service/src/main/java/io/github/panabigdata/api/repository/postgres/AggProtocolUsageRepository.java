package io.github.panabigdata.api.repository.postgres;

import io.github.panabigdata.api.projection.ProtocolUsage;
import io.github.panabigdata.api.projection.ProtocolUsageProjection;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class AggProtocolUsageRepository {
  private final NamedParameterJdbcTemplate jdbc;

  public List<ProtocolUsageProjection> getTopUsedProtocols(
      OffsetDateTime startTime, OffsetDateTime endTime, Pageable pageable) {
    String sql =
        """
        SELECT protocol, SUM(total_bytes) AS total_bytes
        FROM agg_protocol_usage
        WHERE kafka_timestamp_bucket BETWEEN :startTime AND :endTime
        GROUP BY protocol
        ORDER BY total_bytes DESC, protocol ASC
        """;
    MapSqlParameterSource parameters =
        new MapSqlParameterSource().addValue("startTime", startTime).addValue("endTime", endTime);
    if (pageable.isPaged()) {
      sql += " LIMIT :limit OFFSET :offset";
      parameters.addValue("limit", pageable.getPageSize()).addValue("offset", pageable.getOffset());
    }
    return jdbc.query(
        sql,
        parameters,
        (rs, rowNum) -> new ProtocolUsage(rs.getInt("protocol"), rs.getLong("total_bytes")));
  }
}
