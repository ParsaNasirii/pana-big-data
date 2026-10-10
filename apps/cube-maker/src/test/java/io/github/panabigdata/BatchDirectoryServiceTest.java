package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.panabigdata.metrics.CubeMakerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchDirectoryServiceTest {
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final CubeMakerMetrics metrics = new CubeMakerMetrics(registry);
  private final List<String> marked = new ArrayList<>();
  private boolean trackingFails;
  private boolean sizeFails;
  private boolean lockAvailable = true;

  private final BatchDirectoryService service =
      new BatchDirectoryService(null, null, "/input", metrics, "Asia/Tehran", Duration.ofHours(2)) {
        @Override
        List<String> getUnprocessedDirectories() {
          return List.of("first", "second");
        }

        @Override
        long getInputBytes(String path) throws IOException {
          if (sizeFails) throw new IOException("HDFS unavailable");
          return 100;
        }

        @Override
        java.io.Closeable tryAcquireLock(String path) {
          return lockAvailable ? () -> {} : null;
        }

        @Override
        void markDirectoryAsProcessed(String path) throws SQLException, SQLException {
          if (trackingFails) throw new SQLException("Tracking unavailable");
          marked.add(path);
        }
      };

  @Test
  void busyDirectoryIsRetriedWithoutMarkingItProcessed() throws SQLException, IOException {
    lockAvailable = false;
    service.process(
        path -> {
          throw new AssertionError("Locked directory must not be read");
        });
    assertThat(marked).isEmpty();
    assertPending(2);
    lockAvailable = true;
    service.process(path -> true);
    assertThat(marked).containsExactly("first", "second");
    assertPending(0);
  }

  @Test
  void countsInputOncePerDirectoryAndKeepsSkippedSeparate() throws SQLException, IOException {
    service.process(path -> path.equals("first"));
    assertThat(marked).containsExactly("first", "second");
    assertOutcome("succeeded", 1, 100);
    assertOutcome("skipped", 1, 100);
    assertOutcome("failed", 0, 0);
    assertPending(0);
  }

  @Test
  void failureStopsProcessingAndRetryCountsAnotherAttempt() throws SQLException, IOException {
    assertThatThrownBy(
            () ->
                service.process(
                    path -> {
                      throw new SQLException("Write failed");
                    }))
        .isInstanceOf(SQLException.class);
    assertThat(marked).isEmpty();
    assertOutcome("failed", 1, 100);
    assertPending(2);
    service.process(path -> true);
    assertOutcome("succeeded", 2, 200);
    assertOutcome("failed", 1, 100);
    assertPending(0);
  }

  @Test
  void trackingFailureIsNotCountedAsSuccess() {
    trackingFails = true;
    assertThatThrownBy(() -> service.process(path -> true)).isInstanceOf(SQLException.class);
    assertOutcome("failed", 1, 100);
    assertOutcome("succeeded", 0, 0);
    assertPending(2);
  }

  @Test
  void unavailableInputSizeStillCountsFailedAttempt() {
    sizeFails = true;
    assertThatThrownBy(() -> service.process(path -> true)).isInstanceOf(IOException.class);
    assertOutcome("failed", 1, 0);
    assertThat(marked).isEmpty();
    assertPending(2);
  }

  @Test
  void shutdownAfterOneDirectoryLeavesRemainingDirectoriesPending() {
    var stopping = new java.util.concurrent.atomic.AtomicBoolean();
    assertThatThrownBy(
            () ->
                service.process(
                    path -> {
                      stopping.set(true);
                      return true;
                    },
                    stopping::get))
        .isInstanceOf(java.util.concurrent.CancellationException.class);
    assertThat(marked).containsExactly("first");
    assertOutcome("succeeded", 1, 100);
    assertOutcome("failed", 0, 0);
    assertPending(1);
  }

  private void assertOutcome(String outcome, double attempts, double bytes) {
    assertThat(
            registry
                .get("cube_maker_directories_processed")
                .tag("outcome", outcome)
                .counter()
                .count())
        .isEqualTo(attempts);
    assertThat(
            registry
                .get("cube_maker_directory_input_bytes")
                .tag("outcome", outcome)
                .counter()
                .count())
        .isEqualTo(bytes);
  }

  private void assertPending(double count) {
    assertThat(registry.get("cube_maker_pending_directories").gauge().value()).isEqualTo(count);
  }
}
