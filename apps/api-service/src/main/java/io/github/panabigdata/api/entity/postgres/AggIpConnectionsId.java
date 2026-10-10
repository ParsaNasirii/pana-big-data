package io.github.panabigdata.api.entity.postgres;

import java.time.OffsetDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggIpConnectionsId {

  private OffsetDateTime kafkaTimestampBucket;

  private String srcIp;

  private String dstIp;
}
