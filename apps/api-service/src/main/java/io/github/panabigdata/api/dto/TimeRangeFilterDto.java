package io.github.panabigdata.api.dto;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import lombok.Setter;
import org.springframework.format.annotation.DateTimeFormat;

@Setter
public class TimeRangeFilterDto {

  private static final ZoneId API_TIME_ZONE = ZoneId.of("Asia/Tehran");

  @NotNull(message = "start is required")
  @Parameter(
      description = "Start timestamp in ISO-8601 format; normalized to Asia/Tehran",
      example = "2023-10-01T12:00:00+03:30",
      required = true)
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private OffsetDateTime start;

  @NotNull(message = "end is required")
  @Parameter(
      description = "End timestamp in ISO-8601 format; normalized to Asia/Tehran",
      example = "2023-10-02T12:00:00+03:30",
      required = true)
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private OffsetDateTime end;

  public OffsetDateTime getStart() {
    return inApiTimeZone(start);
  }

  public OffsetDateTime getEnd() {
    return inApiTimeZone(end);
  }

  @AssertTrue(message = "end must be greater than or equal to start")
  @Schema(hidden = true)
  public boolean isTimeRangeValid() {
    return start == null || end == null || !start.toInstant().isAfter(end.toInstant());
  }

  private static OffsetDateTime inApiTimeZone(OffsetDateTime value) {
    return value == null ? null : value.atZoneSameInstant(API_TIME_ZONE).toOffsetDateTime();
  }
}
