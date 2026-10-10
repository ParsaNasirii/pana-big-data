package io.github.panabigdata.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackfillTest {
  private static final ZoneId TEHRAN = ZoneId.of("Asia/Tehran");

  @Test
  void createsOnePartitionForAWindowInsideOneDay() {
    Instant start = Instant.parse("2026-09-20T10:00:00Z");
    Instant end = Instant.parse("2026-09-20T11:00:00Z");

    assertEquals(List.of(LocalDate.of(2026, 9, 20)), Backfill.partitionDays(start, end, TEHRAN));
  }

  @Test
  void includesBothDaysWhenTheWindowCrossesTehranMidnight() {
    Instant start = Instant.parse("2026-09-20T20:00:00Z");
    Instant end = Instant.parse("2026-09-20T21:00:00Z");

    assertEquals(
        List.of(LocalDate.of(2026, 9, 20), LocalDate.of(2026, 9, 21)),
        Backfill.partitionDays(start, end, TEHRAN));
  }

  @Test
  void excludesTheNextDayWhenEndIsExactlyTehranMidnight() {
    Instant start = Instant.parse("2026-09-20T20:00:00Z");
    Instant end = Instant.parse("2026-09-20T20:30:00Z");

    assertEquals(List.of(LocalDate.of(2026, 9, 20)), Backfill.partitionDays(start, end, TEHRAN));
  }

  @Test
  void startsOnTheNewDayWhenStartIsExactlyTehranMidnight() {
    Instant start = Instant.parse("2026-09-20T20:30:00Z");
    Instant end = Instant.parse("2026-09-20T21:00:00Z");

    assertEquals(List.of(LocalDate.of(2026, 9, 21)), Backfill.partitionDays(start, end, TEHRAN));
  }

  @Test
  void includesTheLastMillisecondBeforeTehranMidnight() {
    Instant start = Instant.parse("2026-09-20T20:29:59.999Z");
    Instant end = Instant.parse("2026-09-20T20:30:00Z");

    assertEquals(List.of(LocalDate.of(2026, 9, 20)), Backfill.partitionDays(start, end, TEHRAN));
  }

  @Test
  void includesEveryDayOfAMultiDayWindowInOrder() {
    Instant start = Instant.parse("2026-09-20T10:00:00Z");
    Instant end = Instant.parse("2026-09-22T11:00:00Z");

    assertEquals(
        List.of(LocalDate.of(2026, 9, 20), LocalDate.of(2026, 9, 21), LocalDate.of(2026, 9, 22)),
        Backfill.partitionDays(start, end, TEHRAN));
  }
}
