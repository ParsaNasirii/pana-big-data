package io.github.panabigdata.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HourlyDirectoryServiceTest {
  @TempDir File temporaryDirectory;

  private FileSystem fileSystem;
  private Path baseDirectory;
  private HourlyDirectoryService service;

  @BeforeEach
  void setUp() throws IOException {
    fileSystem = FileSystem.newInstance(URI.create("file:///"), new Configuration());
    baseDirectory = new Path(temporaryDirectory.toURI());
    service = new HourlyDirectoryService(fileSystem);
  }

  @AfterEach
  void closeFileSystem() throws IOException {
    fileSystem.close();
  }

  @Test
  void returnsHourlyDirectoriesInChronologicalOrder() throws IOException {
    createDirectory("2024-02-02/23");
    createDirectory("2024-02-01/07");
    createDirectory("2024-02-01/00");

    List<DateHourDirectory> directories = service.findHourlyDirectories(baseDirectory);

    assertEquals(
        List.of(
            dateHourDirectory("2024-02-01/00", "2024-02-01", 0),
            dateHourDirectory("2024-02-01/07", "2024-02-01", 7),
            dateHourDirectory("2024-02-02/23", "2024-02-02", 23)),
        directories);
  }

  @Test
  void ignoresEntriesThatAreNotValidDayAndHourDirectories() throws IOException {
    createDirectory("2024-02-01/00");
    createDirectory("2024-02-01/24");
    createDirectory("2024-02-01/7");
    createDirectory("2024-02-30/01");
    createDirectory("not-a-day/01");
    createFile("2024-02-01/01");
    createFile("2024-02-02");

    List<DateHourDirectory> directories = service.findHourlyDirectories(baseDirectory);

    assertEquals(List.of(dateHourDirectory("2024-02-01/00", "2024-02-01", 0)), directories);
  }

  @Test
  void returnsAnEmptyListForAnEmptyBaseDirectory() throws IOException {
    List<DateHourDirectory> directories = service.findHourlyDirectories(baseDirectory);

    assertEquals(List.of(), directories);
  }

  @Test
  void listsFinalizedParquetFilesInNameOrder() throws IOException {
    createFile("2024-02-01/00/z.parquet");
    createFile("2024-02-01/00/a.parquet");
    createFile("2024-02-01/00/_SUCCESS");

    assertEquals(
        List.of("a.parquet", "z.parquet"),
        service.listFinalizedParquetFiles(new Path(baseDirectory, "2024-02-01/00")).stream()
            .map(status -> status.getPath().getName())
            .toList());
  }

  @Test
  void rejectsUnexpectedEntriesInFinalizedParquetDirectory() throws IOException {
    createFile("2024-02-01/00/a.parquet");
    createFile("2024-02-01/00/open.tmp");

    assertThrows(
        IOException.class,
        () -> service.listFinalizedParquetFiles(new Path(baseDirectory, "2024-02-01/00")));
  }

  @Test
  void returnsOnlyParquetFilesFromHourlyDirectories() throws IOException {
    createFile("2024-02-02/23/second.parquet");
    createFile("2024-02-01/07/first.parquet");
    createFile("2024-02-01/07/_SUCCESS");
    createFile("2024-02-01/07/open.tmp");
    createFile("_temporary/2024-02-01/07/pending.parquet");

    assertEquals(
        List.of(
            new Path(baseDirectory, "2024-02-01/07/first.parquet"),
            new Path(baseDirectory, "2024-02-02/23/second.parquet")),
        service.findParquetFiles(
            baseDirectory,
            Instant.parse("2024-02-01T00:00:00Z"),
            Instant.parse("2024-02-03T00:00:00Z"),
            ZoneId.of("Asia/Tehran")));
  }

  @Test
  void readsOnlyOverlappingHoursInTehranTime() throws IOException {
    createFile("2024-02-01/22/before.parquet");
    createFile("2024-02-01/23/first.parquet");
    createFile("2024-02-02/00/second.parquet");
    createFile("2024-02-02/01/after.parquet");

    assertEquals(
        List.of(
            new Path(baseDirectory, "2024-02-01/23/first.parquet"),
            new Path(baseDirectory, "2024-02-02/00/second.parquet")),
        service.findParquetFiles(
            baseDirectory,
            Instant.parse("2024-02-01T20:00:00Z"),
            Instant.parse("2024-02-01T21:30:00Z"),
            ZoneId.of("Asia/Tehran")));
  }

  @Test
  void includesTheLastHourWhenEndIsInsideIt() throws IOException {
    createFile("2024-02-01/23/first.parquet");
    createFile("2024-02-02/00/second.parquet");

    assertEquals(
        List.of(
            new Path(baseDirectory, "2024-02-01/23/first.parquet"),
            new Path(baseDirectory, "2024-02-02/00/second.parquet")),
        service.findParquetFiles(
            baseDirectory,
            Instant.parse("2024-02-01T20:00:00Z"),
            Instant.parse("2024-02-01T20:45:00Z"),
            ZoneId.of("Asia/Tehran")));
  }

  @Test
  void returnsNoFilesWhenTheWindowHasNoHourlyDirectory() throws IOException {
    createFile("2024-02-01/07/outside.parquet");

    assertEquals(
        List.of(),
        service.findParquetFiles(
            baseDirectory,
            Instant.parse("2024-02-01T20:00:00Z"),
            Instant.parse("2024-02-01T21:00:00Z"),
            ZoneId.of("Asia/Tehran")));
  }

  private DateHourDirectory dateHourDirectory(String relativePath, String date, int hour) {
    return new DateHourDirectory(
        new Path(baseDirectory, relativePath), LocalDate.parse(date), hour);
  }

  private void createDirectory(String relativePath) throws IOException {
    fileSystem.mkdirs(new Path(baseDirectory, relativePath));
  }

  private void createFile(String relativePath) throws IOException {
    Path path = new Path(baseDirectory, relativePath);
    fileSystem.mkdirs(path.getParent());
    fileSystem.create(path).close();
  }
}
