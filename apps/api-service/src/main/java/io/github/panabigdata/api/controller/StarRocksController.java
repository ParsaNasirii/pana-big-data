package io.github.panabigdata.api.controller;

import io.github.panabigdata.api.dto.TimeRangeFilterDto;
import io.github.panabigdata.api.dto.TrafficFilterDto;
import io.github.panabigdata.api.dto.starrocks.ConnectionInfo;
import io.github.panabigdata.api.dto.starrocks.IpBandwidth;
import io.github.panabigdata.api.dto.starrocks.ProtocolUsage;
import io.github.panabigdata.api.dto.starrocks.TrafficSummary;
import io.github.panabigdata.api.service.NetflowService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/starrocks")
@Tag(name = "StarRocks Analytics", description = "StarRocks netflow analysis APIs")
@RequiredArgsConstructor
@Validated
public class StarRocksController {

  private final NetflowService netflowService;

  @Operation(summary = "Get traffic summary for an IP")
  @GetMapping("/traffic-summary")
  public ResponseEntity<TrafficSummary> getTrafficSummary(
      @Valid @ParameterObject TrafficFilterDto filter) {

    Long startMs = toUnixMillis(filter.getStart());
    Long endMs = toUnixMillis(filter.getEnd());

    return ResponseEntity.ok(netflowService.getIpTrafficSummary(filter.getIp(), startMs, endMs));
  }

  @Operation(
      summary = "Get connections for an IP",
      description =
          "Sort by srcIp, dstIp or kafkaTimestampMs; the time interval includes both endpoints.")
  @GetMapping("/connections")
  public ResponseEntity<Page<ConnectionInfo>> getConnections(
      @Valid @ParameterObject TrafficFilterDto filter,
      @ParameterObject @PageableDefault(size = 20) Pageable pageable) {

    Long startMs = toUnixMillis(filter.getStart());
    Long endMs = toUnixMillis(filter.getEnd());

    return ResponseEntity.ok(
        netflowService.getIpConnections(filter.getIp(), startMs, endMs, pageable));
  }

  private Long toUnixMillis(OffsetDateTime dateTime) {
    return (dateTime != null) ? dateTime.toInstant().toEpochMilli() : null;
  }

  @Operation(
      summary = "Get top bandwidth-consuming IPs",
      description = "Returns IP addresses ranked by bandwidth consumption.")
  @GetMapping("/top-ips")
  public ResponseEntity<Page<IpBandwidth>> getTopIps(
      @Valid @ParameterObject TimeRangeFilterDto filter,
      @Parameter(description = "Number of top IPs to return", example = "10")
          @RequestParam(value = "limit", defaultValue = "10")
          @Min(1)
          @Max(1000)
          int limit) {

    return ResponseEntity.ok(
        netflowService.getTopBandwidthConsumers(
            toUnixMillis(filter.getStart()), toUnixMillis(filter.getEnd()), limit));
  }

  @Operation(
      summary = "Get most used protocols",
      description = "Returns network protocols ranked by usage.")
  @GetMapping("/top-protocols")
  public ResponseEntity<Page<ProtocolUsage>> getTopProtocols(
      @Valid @ParameterObject TimeRangeFilterDto filter,
      @Parameter(description = "Number of top protocols to return", example = "5")
          @RequestParam(value = "limit", defaultValue = "5")
          @Min(1)
          @Max(1000)
          int limit) {

    return ResponseEntity.ok(
        netflowService.getMostUsedProtocols(
            toUnixMillis(filter.getStart()), toUnixMillis(filter.getEnd()), limit));
  }
}
