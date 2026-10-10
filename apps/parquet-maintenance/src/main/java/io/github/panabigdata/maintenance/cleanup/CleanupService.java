package io.github.panabigdata.maintenance.cleanup;

import io.github.panabigdata.maintenance.config.MaintenanceConfig;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@RequiredArgsConstructor
@Slf4j
public final class CleanupService {
  private static final String STAGE_SUFFIX = ".compacting-";
  private static final String ORIGINALS_DIRECTORY = "originals";

  private final FileSystem fs;
  private final Path base;
  private final MaintenanceConfig config;
  private final Clock clock;

  public void clean() throws IOException {
    if (!fs.exists(base)) {
      return;
    }

    Path temporaryDirectory = new Path(base, "_temporary");
    cleanupTemporaryFiles(temporaryDirectory);

    for (FileStatus day : fs.listStatus(base)) {
      if (isValidDayDirectory(day)) {
        cleanupDay(day.getPath());
      }
    }
  }

  public boolean hasUnrecoveredStage(Path hour) throws IOException {
    String stagePrefix = "." + hour.getName() + STAGE_SUFFIX;
    for (FileStatus entry : fs.listStatus(hour.getParent())) {
      if (entry.isDirectory()
          && entry.getPath().getName().startsWith(stagePrefix)
          && fs.exists(new Path(entry.getPath(), ORIGINALS_DIRECTORY))) {
        return true;
      }
    }
    return false;
  }

  private void cleanupDay(Path day) throws IOException {
    cleanupExpiredLocks(day);

    for (FileStatus entry : fs.listStatus(day)) {
      cleanupUnlockedStage(day, entry);
    }
  }

  private void cleanupUnlockedStage(Path day, FileStatus entry) throws IOException {
    if (!entry.isDirectory()) {
      return;
    }

    String name = entry.getPath().getName();
    if (!name.matches("\\.(?:[01]\\d|2[0-3])\\.compacting-.+")) {
      return;
    }

    String hour = name.substring(1, 3);
    Path lock = new Path(day, "." + hour + ".maintenance.lock");
    if (fs.exists(lock)) {
      return;
    }

    Path originals = new Path(entry.getPath(), ORIGINALS_DIRECTORY);
    if (fs.exists(originals)) {
      recoverInterruptedMerge(entry.getPath(), new Path(day, hour), "." + hour + STAGE_SUFFIX);
    } else if (isOld(entry)) {
      log.info("Deleting abandoned staging directory: {}", entry.getPath());
      delete(entry.getPath(), true);
    }
  }

  private boolean isValidDayDirectory(FileStatus entry) {
    if (!entry.isDirectory()) {
      return false;
    }

    try {
      LocalDate.parse(entry.getPath().getName());
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  private void cleanupExpiredLocks(Path day) throws IOException {
    for (FileStatus entry : fs.listStatus(day)) {
      if (!isExpiredLock(entry)) {
        continue;
      }

      recoverInterruptedMerges(day, entry);
      log.warn("Deleting expired hourly directory lock: {}", entry.getPath());
      delete(entry.getPath(), false);
    }
  }

  private boolean isExpiredLock(FileStatus entry) throws IOException {
    return entry.isFile() && isHourlyLock(entry) && isOld(entry, config.getStaleLockAge());
  }

  private boolean isHourlyLock(FileStatus entry) {
    return entry.getPath().getName().matches("\\.(?:[01]\\d|2[0-3])\\.maintenance\\.lock");
  }

  private void recoverInterruptedMerges(Path day, FileStatus lock) throws IOException {
    String stagePrefix = getStagePrefix(lock);
    Path hour = new Path(day, lock.getPath().getName().substring(1, 3));

    for (FileStatus entry : fs.listStatus(day)) {
      if (!entry.isDirectory() || !entry.getPath().getName().startsWith(stagePrefix)) {
        continue;
      }

      recoverInterruptedMerge(entry.getPath(), hour, stagePrefix);
    }
  }

  private void recoverInterruptedMerge(Path stage, Path hour, String stagePrefix)
      throws IOException {
    Path originals = new Path(stage, ORIGINALS_DIRECTORY);
    if (!fs.exists(originals)) {
      return;
    }

    String id = stage.getName().substring(stagePrefix.length());
    Path output = new Path(hour, "merged-" + id + ".parquet");
    if (fs.exists(output)) {
      log.warn("Completing interrupted merge with published output: {}", stage);
    } else {
      log.warn("Restoring originals from interrupted merge: {}", stage);
      restoreOriginals(originals, hour);
    }
    delete(stage, true);
  }

  private void restoreOriginals(Path originals, Path hour) throws IOException {
    for (FileStatus original : fs.listStatus(originals)) {
      if (!original.isFile() || !original.getPath().getName().endsWith(".parquet")) {
        throw new IOException("Unexpected original in " + originals + ": " + original.getPath());
      }
      Path destination = new Path(hour, original.getPath().getName());
      if (fs.exists(destination) || !fs.rename(original.getPath(), destination)) {
        throw new IOException("Cannot restore " + original.getPath() + " to " + destination);
      }
    }
  }

  private String getStagePrefix(FileStatus lock) {
    String name = lock.getPath().getName();
    return "." + name.substring(1, 3) + STAGE_SUFFIX;
  }

  private void cleanupTemporaryFiles(Path directory) throws IOException {
    if (!fs.exists(directory)) {
      return;
    }

    for (FileStatus entry : fs.listStatus(directory)) {
      if (entry.isDirectory()) {
        cleanupTemporaryFiles(entry.getPath());
        deleteIfEmpty(entry.getPath());
      } else if (isExpiredTemporaryFile(entry)) {
        log.info("Deleting expired writer temporary file: {}", entry.getPath());
        delete(entry.getPath(), false);
      }
    }
  }

  private void deleteIfEmpty(Path directory) throws IOException {
    if (fs.listStatus(directory).length == 0) {
      delete(directory, false);
    }
  }

  private boolean isExpiredTemporaryFile(FileStatus file) {
    return file.getPath().getName().endsWith(".tmp") && isOld(file);
  }

  private boolean isOld(FileStatus status) {
    return isOld(status, config.getTemporaryFileRetention());
  }

  private boolean isOld(FileStatus status, Duration retention) {
    long cutoff = clock.instant().minus(retention).toEpochMilli();
    return status.getModificationTime() < cutoff;
  }

  private void delete(Path path, boolean recursive) throws IOException {
    if (!fs.delete(path, recursive)) {
      throw new IOException("Cannot delete " + path);
    }
  }
}
