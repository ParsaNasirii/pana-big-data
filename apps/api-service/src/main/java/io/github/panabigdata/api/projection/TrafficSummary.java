package io.github.panabigdata.api.projection;

import lombok.Value;

@Value
public class TrafficSummary implements IpTrafficSummaryProjection {
  Long totalInBytes;
  Long totalOutBytes;
  Long totalBytes;
}
