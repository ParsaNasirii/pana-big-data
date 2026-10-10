package io.github.panabigdata.api.dto.starrocks;

import lombok.Value;

@Value
public class ConnectionInfo {
  String srcIp;
  String dstIp;
}
