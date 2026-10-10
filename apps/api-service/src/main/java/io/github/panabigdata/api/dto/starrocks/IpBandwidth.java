package io.github.panabigdata.api.dto.starrocks;

import lombok.Value;

@Value
public class IpBandwidth {
  String ip;
  Long totalBandwidth;
}
