package io.github.panabigdata.maintenance.compaction;

import io.github.panabigdata.hdfs.DateHourDirectory;
import io.github.panabigdata.hdfs.HourlyDirectoryService;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

final class HourlyParquetScanner {
  private final HourlyDirectoryService directories;
  private final Path base;
  private final Clock clock;

  HourlyParquetScanner(FileSystem fs, Path base, Clock clock) {
    this.directories = new HourlyDirectoryService(fs);
    this.base = base;
    this.clock = clock;
  }

  List<HourlyParquetFiles> findPreviousDayFiles() throws IOException {
    List<HourlyParquetFiles> hours = new ArrayList<>();
    LocalDate today = LocalDate.now(clock);
    for (DateHourDirectory hour : directories.findHourlyDirectories(base)) {
      if (hour.date().isBefore(today)) {
        List<ParquetFileSnapshot> files = listParquetFiles(hour.path());
        if (!files.isEmpty()) {
          hours.add(new HourlyParquetFiles(hour.path(), files));
        }
      }
    }
    hours.sort(Comparator.comparing(hour -> hour.path().toString()));
    return List.copyOf(hours);
  }

  List<ParquetFileSnapshot> listParquetFiles(Path directory) throws IOException {
    List<ParquetFileSnapshot> files = new ArrayList<>();
    for (FileStatus status : directories.listFinalizedParquetFiles(directory)) {
      files.add(
          new ParquetFileSnapshot(
              status.getPath().getName(), status.getLen(), status.getModificationTime()));
    }
    return List.copyOf(files);
  }

  void verifySelectedFiles(Path directory, List<ParquetFileSnapshot> selected) throws IOException {
    List<ParquetFileSnapshot> current = listParquetFiles(directory);
    if (!current.containsAll(selected)) {
      throw new IOException("Selected files changed while compacting: " + directory);
    }
  }
}
