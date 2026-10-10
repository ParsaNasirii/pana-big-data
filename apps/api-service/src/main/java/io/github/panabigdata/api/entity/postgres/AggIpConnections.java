package io.github.panabigdata.api.entity.postgres;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AggIpConnections {

  private AggIpConnectionsId id;

  private Long totalFlows = 0L;
  private Long totalBytes = 0L;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AggIpConnections)) return false;
    AggIpConnections that = (AggIpConnections) o;
    return id != null && id.equals(that.getId());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
