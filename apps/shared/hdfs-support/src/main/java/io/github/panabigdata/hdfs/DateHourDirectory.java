package io.github.panabigdata.hdfs;

import java.time.LocalDate;
import org.apache.hadoop.fs.Path;

public record DateHourDirectory(Path path, LocalDate date, int hour) {}
