package io.github.panabigdata.api.dto;

import io.swagger.v3.oas.annotations.Parameter;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TrafficFilterDto extends TimeRangeFilterDto {

  @Parameter(description = "IP address to filter", example = "192.168.1.100")
  @NotBlank(message = "ip is required")
  @Pattern(
      regexp =
          "(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])"
              + "(?:\\.(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){3}",
      message = "ip must be a valid IPv4 address")
  private String ip;
}
