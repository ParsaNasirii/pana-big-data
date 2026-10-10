package io.github.panabigdata.api.service;

import io.github.panabigdata.api.dto.starrocks.ConnectionInfo;
import io.github.panabigdata.api.dto.starrocks.IpBandwidth;
import io.github.panabigdata.api.dto.starrocks.ProtocolUsage;
import io.github.panabigdata.api.dto.starrocks.TrafficSummary;
import io.github.panabigdata.api.repository.starrocks.NetflowRawRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class NetflowService {

  private final NetflowRawRepository netflowRepository;

  public TrafficSummary getIpTrafficSummary(String ip, Long startTimeMs, Long endTimeMs) {
    return netflowRepository.getTrafficSummaryForIp(ip, startTimeMs, endTimeMs);
  }

  public Page<ConnectionInfo> getIpConnections(
      String ip, Long startTimeMs, Long endTimeMs, Pageable pageable) {
    return netflowRepository.getConnectionsForIp(ip, startTimeMs, endTimeMs, pageable);
  }

  public Page<IpBandwidth> getTopBandwidthConsumers(Long startTimeMs, Long endTimeMs, int limit) {
    PageRequest pageRequest = PageRequest.of(0, limit);
    return netflowRepository.getTopIpsByBandwidth(startTimeMs, endTimeMs, pageRequest);
  }

  public Page<ProtocolUsage> getMostUsedProtocols(Long startTimeMs, Long endTimeMs, int limit) {
    PageRequest pageRequest = PageRequest.of(0, limit);
    return netflowRepository.getTopUsedProtocols(startTimeMs, endTimeMs, pageRequest);
  }
}
