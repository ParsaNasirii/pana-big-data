package io.github.panabigdata.api.projection;

public interface IpTrafficSummaryProjection {
  Long getTotalInBytes();

  Long getTotalOutBytes();

  Long getTotalBytes();
}
