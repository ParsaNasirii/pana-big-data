package io.github.panabigdata.api.controller;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.api.dto.TimeRangeFilterDto;
import jakarta.validation.Valid;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.GetMapping;

class ApiTimeRangeContractTest {

  private static final ZoneId TEHRAN = ZoneId.of("Asia/Tehran");

  @Test
  void everyApiEndpointHasATimeRangeFilter() {
    assertTimeRangeFilterOnEveryEndpoint(SparkController.class);
    assertTimeRangeFilterOnEveryEndpoint(StarRocksController.class);
  }

  @Test
  void normalizesApiTimeRangeToTehranWithoutChangingTheInstant() {
    TimeRangeFilterDto filter = new TimeRangeFilterDto();
    OffsetDateTime utcStart = OffsetDateTime.parse("2023-10-01T08:30:00Z");
    OffsetDateTime utcEnd = OffsetDateTime.parse("2023-10-01T09:30:00Z");
    filter.setStart(utcStart);
    filter.setEnd(utcEnd);

    assertThat(filter.getStart().toInstant()).isEqualTo(utcStart.toInstant());
    assertThat(filter.getEnd().toInstant()).isEqualTo(utcEnd.toInstant());
    assertThat(filter.getStart().getOffset())
        .isEqualTo(TEHRAN.getRules().getOffset(utcStart.toInstant()));
    assertThat(filter.getEnd().getOffset())
        .isEqualTo(TEHRAN.getRules().getOffset(utcEnd.toInstant()));
  }

  private void assertTimeRangeFilterOnEveryEndpoint(Class<?> controller) {
    Arrays.stream(controller.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(GetMapping.class))
        .forEach(
            method ->
                assertThat(hasValidatedTimeRangeFilter(method)).as(method.getName()).isTrue());
  }

  private boolean hasValidatedTimeRangeFilter(Method method) {
    return Arrays.stream(method.getParameters()).anyMatch(this::isValidatedTimeRangeFilter);
  }

  private boolean isValidatedTimeRangeFilter(Parameter parameter) {
    return TimeRangeFilterDto.class.isAssignableFrom(parameter.getType())
        && parameter.isAnnotationPresent(Valid.class);
  }
}
