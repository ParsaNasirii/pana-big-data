package io.github.panabigdata.api.projection;

import lombok.Value;

@Value
public class IpBandwidth implements IpBandwidthProjection {
  String ipAddress;
  Long totalBytes;
}
