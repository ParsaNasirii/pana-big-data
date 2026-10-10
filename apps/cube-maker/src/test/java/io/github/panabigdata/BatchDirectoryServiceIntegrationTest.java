package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.panabigdata.metrics.CubeMakerMetrics;
import io.github.panabigdata.storage.DirectoryTrackingStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchDirectoryServiceIntegrationTest extends BaseSparkTest {
  @TempDir Path temporaryDirectory;

  private final Set<String> processed = new HashSet<>();
  private final DirectoryTrackingStore trackingStore =
      new DirectoryTrackingStore() {
        @Override
        public Set<String> getProcessedDirectories() {
          return Set.copyOf(processed);
        }

        @Override
        public void markDirectoryAsProcessed(String path) {
          processed.add(path);
        }
      };

  @Test
  void defersRecentDirectoriesUntilTheirSettlingTimePasses() throws Exception {
    String previousDay = createDirectory("2026-10-08/23");
    String alreadyProcessed = createDirectory("2026-10-09/08");
    String eligibleAtNoon = createDirectory("2026-10-09/09");
    createDirectory("2026-10-09/10");
    createDirectory("2026-10-09/12");
    createDirectory("2026-10-10/00");
    processed.add(alreadyProcessed);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try {
      // 11:59:59 and 12:00:00 in Asia/Tehran.
      directories("2026-10-09T08:29:59Z", 2, registry).process(path -> true);
      assertThat(processed).containsExactlyInAnyOrder(previousDay, alreadyProcessed);

      assertThat(directories("2026-10-09T08:30:00Z", 3, registry).getUnprocessedDirectories())
          .isEmpty();
      BatchDirectoryService atNoon = directories("2026-10-09T08:30:00Z", 2, registry);
      assertThat(atNoon.getUnprocessedDirectories()).containsExactly(eligibleAtNoon);

      atNoon.process(path -> true);
      assertThat(processed)
          .containsExactlyInAnyOrder(previousDay, alreadyProcessed, eligibleAtNoon);
      assertThat(atNoon.getUnprocessedDirectories()).isEmpty();
    } finally {
      registry.close();
    }
  }

  private BatchDirectoryService directories(
      String now, int settlingHours, SimpleMeterRegistry registry) {
    return new BatchDirectoryService(
        spark,
        trackingStore,
        temporaryDirectory.toString(),
        new CubeMakerMetrics(registry),
        "Asia/Tehran",
        Duration.ofHours(settlingHours),
        Clock.fixed(Instant.parse(now), ZoneOffset.UTC));
  }

  private String createDirectory(String relativePath) throws Exception {
    Path directory = Files.createDirectories(temporaryDirectory.resolve(relativePath));
    return new org.apache.hadoop.fs.Path("file", null, directory.toString()).toString();
  }
}
