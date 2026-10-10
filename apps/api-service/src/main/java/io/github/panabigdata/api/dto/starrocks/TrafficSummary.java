package io.github.panabigdata.api.dto.starrocks;

import lombok.Value;

@Value
public class TrafficSummary {
  Long totalIn;
  Long totalOut;
}
