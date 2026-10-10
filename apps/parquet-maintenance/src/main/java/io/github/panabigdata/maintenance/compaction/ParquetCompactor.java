package io.github.panabigdata.maintenance.compaction;

import io.github.panabigdata.hdfs.HourlyDirectoryLock;
import io.github.panabigdata.maintenance.cleanup.CleanupService;
import io.github.panabigdata.maintenance.config.MaintenanceConfig;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ParquetCompactor {
  private final SparkSession spark;
  private final MaintenanceConfig config;
  private final FileSystem fs;
  private final HourlyParquetScanner scanner;
  private final SmallFileMerger merger;
  private final CleanupService cleaner;

  public record Result(int selected, int compacted, long rows) {}

  public ParquetCompactor(SparkSession spark, MaintenanceConfig config, Clock clock)
      throws IOException {
    this.spark = spark.newSession();
    this.config = config;
    Path source = new Path(config.getBasePath());
    fs = source.getFileSystem(spark.sparkContext().hadoopConfiguration());
    Path base = fs.makeQualified(source);
    if (base.isRoot()) {
      throw new IllegalArgumentException("Filesystem root cannot be compacted");
    }
    this.spark.conf().set("spark.sql.files.ignoreCorruptFiles", "false");
    this.spark.conf().set("spark.sql.files.ignoreMissingFiles", "false");
    scanner = new HourlyParquetScanner(fs, base, clock);
    merger = new SmallFileMerger(this.spark, scanner, config);
    cleaner = new CleanupService(fs, base, config, clock);
  }

  public Result compactPreviousDays() throws IOException {
    if (config.isDryRun()) {
      return previewPreviousDays();
    }

    cleaner.clean();
    int selectedDirectories = 0;
    int compactedDirectories = 0;
    long totalMergedRows = 0;

    for (HourlyParquetFiles directory : scanner.findPreviousDayFiles()) {
      List<List<ParquetFileSnapshot>> groups = planSizeLimitedMerges(directory);
      if (groups.isEmpty()) {
        continue;
      }
      selectedDirectories++;

      try (HourlyDirectoryLock lock = HourlyDirectoryLock.tryAcquire(fs, directory.path())) {
        if (lock == null) {
          log.info("Skipping locked hourly directory: {}", directory.path());
        } else if (cleaner.hasUnrecoveredStage(directory.path())) {
          log.warn("Skipping hourly directory with unfinished merge: {}", directory.path());
        } else {
          for (List<ParquetFileSnapshot> group : groups) {
            totalMergedRows = Math.addExact(totalMergedRows, mergeGroup(directory, group));
          }
          compactedDirectories++;
        }
      }
    }

    return new Result(selectedDirectories, compactedDirectories, totalMergedRows);
  }

  private Result previewPreviousDays() throws IOException {
    int selectedDirectories = 0;
    for (HourlyParquetFiles directory : scanner.findPreviousDayFiles()) {
      List<List<ParquetFileSnapshot>> groups = planSizeLimitedMerges(directory);
      if (!groups.isEmpty()) {
        selectedDirectories++;
        log.info("DRY RUN {}: {} size-limited merges", directory.path(), groups.size());
      }
    }
    return new Result(selectedDirectories, 0, 0);
  }

  private List<List<ParquetFileSnapshot>> planSizeLimitedMerges(HourlyParquetFiles directory) {
    if (directory.files().size() < 2) {
      return List.of();
    }
    List<ParquetFileSnapshot> files = new ArrayList<>(directory.files());
    files.sort(Comparator.comparingLong(ParquetFileSnapshot::length));
    return createMergeGroups(files);
  }

  private List<List<ParquetFileSnapshot>> createMergeGroups(List<ParquetFileSnapshot> files) {

    long targetSize = config.getTargetFileSizeBytes();

    List<List<ParquetFileSnapshot>> groups = new ArrayList<>();
    List<ParquetFileSnapshot> currentGroup = new ArrayList<>();
    long currentSize = 0;

    for (ParquetFileSnapshot file : files) {
      if (file.length() >= targetSize) {
        continue;
      }
      if (currentSize > targetSize - file.length()) {
        if (currentGroup.size() > 1) {
          groups.add(List.copyOf(currentGroup));
        }
        currentGroup = new ArrayList<>();
        currentSize = 0;
      }

      currentGroup.add(file);
      currentSize += file.length();
    }

    if (currentGroup.size() > 1) {
      groups.add(List.copyOf(currentGroup));
    }

    return groups;
  }

  private long mergeGroup(HourlyParquetFiles directory, List<ParquetFileSnapshot> group)
      throws IOException {
    Path hour = directory.path();
    String id = UUID.randomUUID().toString();
    Path stage = new Path(hour.getParent(), "." + hour.getName() + ".compacting-" + id);
    Path output = new Path(hour, "merged-" + id + ".parquet");
    Path originalsDirectory = new Path(stage, "originals");

    try {
      long mergedRows = merger.writeMergedFile(directory, group, stage);
      scanner.verifySelectedFiles(hour, group);
      Path stagedOutput = new Path(stage, scanner.listParquetFiles(stage).get(0).name());
      moveOriginalsToStaging(hour, group, originalsDirectory);
      renameWithoutOverwrite(stagedOutput, output);

      deleteCompletedStage(stage);
      log.info("Merged {} small files in {} into {}", group.size(), hour, output);
      return mergedRows;
    } catch (IOException | RuntimeException failure) {
      rollbackFailedMerge(hour, group, stage, originalsDirectory, output, failure);
      throw new IOException(
          "Failed to merge small files in " + hour + " (stage " + stage + ")", failure);
    }
  }

  private void moveOriginalsToStaging(
      Path hour, List<ParquetFileSnapshot> group, Path originalsDirectory) throws IOException {
    fs.mkdirs(originalsDirectory);
    for (ParquetFileSnapshot file : group) {
      renameWithoutOverwrite(
          new Path(hour, file.name()), new Path(originalsDirectory, file.name()));
    }
  }

  private void deleteCompletedStage(Path stage) {
    try {
      if (!fs.delete(stage, true)) {
        log.warn("Could not delete completed merge stage: {}", stage);
      }
    } catch (IOException cleanupFailure) {
      log.warn("Could not delete completed merge stage: {}", stage, cleanupFailure);
    }
  }

  private void rollbackFailedMerge(
      Path hour,
      List<ParquetFileSnapshot> group,
      Path stage,
      Path originalsDirectory,
      Path output,
      Exception failure) {
    try {
      // The rename may have succeeded even if its acknowledgement was lost.
      if (fs.exists(output) && !fs.delete(output, false)) {
        throw new IOException("Cannot remove " + output);
      }
      restoreMovedOriginals(hour, group, originalsDirectory);
      if (fs.exists(stage) && !fs.delete(stage, true)) {
        throw new IOException("Cannot remove rolled-back staging directory: " + stage);
      }
    } catch (IOException | RuntimeException rollbackFailure) {
      failure.addSuppressed(rollbackFailure);
    }
  }

  private void restoreMovedOriginals(
      Path hour, List<ParquetFileSnapshot> group, Path originalsDirectory) throws IOException {
    if (!fs.exists(originalsDirectory)) {
      return;
    }
    for (ParquetFileSnapshot file : group) {
      Path stagedOriginal = new Path(originalsDirectory, file.name());
      if (fs.exists(stagedOriginal)) {
        renameWithoutOverwrite(stagedOriginal, new Path(hour, file.name()));
      }
    }
  }

  void renameWithoutOverwrite(Path source, Path destination) throws IOException {
    FileContext.getFileContext(fs.getUri(), spark.sparkContext().hadoopConfiguration())
        .rename(source, destination, Options.Rename.NONE);
  }
}
