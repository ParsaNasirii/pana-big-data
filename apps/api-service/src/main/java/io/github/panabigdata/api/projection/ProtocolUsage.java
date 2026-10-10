package io.github.panabigdata.api.projection;

import lombok.Value;

@Value
public class ProtocolUsage implements ProtocolUsageProjection {
  Integer protocol;
  Long totalBytes;
}
