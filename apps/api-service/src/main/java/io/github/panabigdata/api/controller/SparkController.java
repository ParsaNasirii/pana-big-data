package io.github.panabigdata.api.controller;

import io.github.panabigdata.api.dto.TimeRangeFilterDto;
import io.github.panabigdata.api.dto.TrafficFilterDto;
import io.github.panabigdata.api.entity.postgres.AggIpConnections;
import io.github.panabigdata.api.projection.IpBandwidthProjection;
import io.github.panabigdata.api.projection.IpTrafficSummaryProjection;
import io.github.panabigdata.api.projection.ProtocolUsageProjection;
import io.github.panabigdata.api.service.NetworkAnalyticsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
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
@RequestMapping("/api/spark")
@Tag(name = "Spark Analytics", description = "Spark network traffic analysis APIs")
@RequiredArgsConstructor
@Validated
public class SparkController {

  private final NetworkAnalyticsService analyticsService;

  @Operation(summary = "Get traffic summary for an IP")
  @GetMapping("/traffic-summary")
  public ResponseEntity<IpTrafficSummaryProjection> getTrafficSummary(
      @Valid @ParameterObject TrafficFilterDto filter) {
    return ResponseEntity.ok(
        analyticsService.getIpTrafficSummary(filter.getIp(), filter.getStart(), filter.getEnd()));
  }

  @Operation(summary = "Get connections for an IP")
  @GetMapping("/connections")
  public ResponseEntity<Page<AggIpConnections>> getConnections(
      @Valid @ParameterObject TrafficFilterDto filter,
      @ParameterObject @PageableDefault(size = 20) Pageable pageable) {
    return ResponseEntity.ok(
        analyticsService.getIpConnections(
            filter.getIp(), filter.getStart(), filter.getEnd(), pageable));
  }

  @Operation(summary = "Get top bandwidth-consuming IPs")
  @GetMapping("/top-ips")
  public ResponseEntity<List<IpBandwidthProjection>> getTopIps(
      @Valid @ParameterObject TimeRangeFilterDto filter,
      @Parameter(description = "Number of top IPs to return", example = "10")
          @RequestParam(value = "limit", defaultValue = "10")
          @Min(1)
          @Max(1000)
          int limit) {
    return ResponseEntity.ok(
        analyticsService.getTopBandwidthConsumers(filter.getStart(), filter.getEnd(), limit));
  }

  @Operation(summary = "Get most used protocols")
  @GetMapping("/top-protocols")
  public ResponseEntity<List<ProtocolUsageProjection>> getTopProtocols(
      @Valid @ParameterObject TimeRangeFilterDto filter,
      @Parameter(description = "Number of top protocols to return", example = "5")
          @RequestParam(value = "limit", defaultValue = "5")
          @Min(1)
          @Max(1000)
          int limit) {
    return ResponseEntity.ok(
        analyticsService.getMostUsedProtocols(filter.getStart(), filter.getEnd(), limit));
  }
}
