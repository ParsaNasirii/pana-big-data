package io.github.panabigdata.hdfs;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class HourlyDirectoryService {
  private final FileSystem fileSystem;

  public HourlyDirectoryService(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  public List<DateHourDirectory> findHourlyDirectories(Path baseDirectory) throws IOException {
    List<DateHourDirectory> directories = new ArrayList<>();

    for (FileStatus dayDirectory : fileSystem.listStatus(baseDirectory)) {
      LocalDate day = parseDay(dayDirectory);
      if (day == null) {
        continue;
      }

      for (FileStatus hourDirectory : fileSystem.listStatus(dayDirectory.getPath())) {
        Integer hour = parseHour(hourDirectory);
        if (hour != null) {
          directories.add(new DateHourDirectory(hourDirectory.getPath(), day, hour));
        }
      }
    }

    directories.sort(Comparator.comparing(DateHourDirectory::path));
    return List.copyOf(directories);
  }

  /** Lists completed Parquet files and rejects unexpected entries in an hourly directory. */
  public List<FileStatus> listFinalizedParquetFiles(Path directory) throws IOException {
    List<FileStatus> files = new ArrayList<>();
    for (FileStatus status : fileSystem.listStatus(directory)) {
      String name = status.getPath().getName();
      if (status.isFile() && name.equals("_SUCCESS")) {
        continue;
      }
      if (!isParquetFile(status) || name.startsWith(".") || name.startsWith("_")) {
        throw new IOException("Unexpected entry in Parquet directory: " + status.getPath());
      }
      files.add(status);
    }
    files.sort(Comparator.comparing(status -> status.getPath().getName()));
    return List.copyOf(files);
  }

  public List<Path> findParquetFiles(
      Path baseDirectory, Instant start, Instant end, ZoneId timeZone) throws IOException {
    List<Path> files = new ArrayList<>();
    DateTimeFormatter directoryFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH");

    for (var hour = start.atZone(timeZone).truncatedTo(ChronoUnit.HOURS);
        hour.toInstant().isBefore(end);
        hour = hour.plusHours(1)) {
      Path directory = new Path(baseDirectory, hour.format(directoryFormat));
      if (!fileSystem.exists(directory)) {
        continue;
      }
      for (FileStatus file : fileSystem.listStatus(directory)) {
        if (isParquetFile(file)) {
          files.add(file.getPath());
        }
      }
    }

    files.sort(Comparator.naturalOrder());
    return List.copyOf(files);
  }

  private LocalDate parseDay(FileStatus directory) {
    if (!directory.isDirectory()) {
      return null;
    }

    try {
      return LocalDate.parse(directory.getPath().getName());
    } catch (DateTimeParseException ignored) {
      return null;
    }
  }

  private boolean isParquetFile(FileStatus status) {
    return status.isFile() && status.getPath().getName().endsWith(".parquet");
  }

  private Integer parseHour(FileStatus directory) {
    String name = directory.getPath().getName();
    if (!directory.isDirectory() || !name.matches("[01][0-9]|2[0-3]")) {
      return null;
    }

    return Integer.valueOf(name);
  }
}
