package io.github.panabigdata.hdfs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** A lock outside the hour directory, so it survives changes to that directory. */
public final class HourlyDirectoryLock implements Closeable {
  private final FileSystem fs;
  private final Path path;

  private HourlyDirectoryLock(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  public static HourlyDirectoryLock tryAcquire(FileSystem fs, Path hour) throws IOException {
    Path lock = new Path(hour.getParent(), "." + hour.getName() + ".maintenance.lock");
    try {
      fs.create(lock, false).close();
      return new HourlyDirectoryLock(fs, lock);
    } catch (FileAlreadyExistsException busy) {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    if (!fs.delete(path, false)) {
      throw new IOException("Cannot release hourly directory lock: " + path);
    }
  }
}
