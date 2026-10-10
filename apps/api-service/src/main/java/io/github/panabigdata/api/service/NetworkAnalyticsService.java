package io.github.panabigdata.api.service;

import io.github.panabigdata.api.entity.postgres.AggIpConnections;
import io.github.panabigdata.api.projection.IpBandwidthProjection;
import io.github.panabigdata.api.projection.IpTrafficSummaryProjection;
import io.github.panabigdata.api.projection.ProtocolUsageProjection;
import io.github.panabigdata.api.repository.postgres.AggIpConnectionsRepository;
import io.github.panabigdata.api.repository.postgres.AggIpTrafficRepository;
import io.github.panabigdata.api.repository.postgres.AggProtocolUsageRepository;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class NetworkAnalyticsService {

  private final AggIpTrafficRepository trafficRepository;
  private final AggIpConnectionsRepository connectionRepository;
  private final AggProtocolUsageRepository protocolRepository;

  public IpTrafficSummaryProjection getIpTrafficSummary(
      String ip, OffsetDateTime start, OffsetDateTime end) {
    return trafficRepository.getTrafficSummaryForIp(ip, start, end);
  }

  public Page<AggIpConnections> getIpConnections(
      String ip, OffsetDateTime start, OffsetDateTime end, Pageable pageable) {
    return connectionRepository.getConnectionsForIp(ip, start, end, pageable);
  }

  public List<IpBandwidthProjection> getTopBandwidthConsumers(
      OffsetDateTime start, OffsetDateTime end, int limit) {
    return trafficRepository.getTopIpsByBandwidth(start, end, PageRequest.of(0, limit));
  }

  public List<ProtocolUsageProjection> getMostUsedProtocols(
      OffsetDateTime start, OffsetDateTime end, int limit) {
    return protocolRepository.getTopUsedProtocols(start, end, PageRequest.of(0, limit));
  }
}
