package io.github.panabigdata.maintenance.compaction;

import io.github.panabigdata.maintenance.config.MaintenanceConfig;
import java.io.IOException;
import java.util.List;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class SmallFileMerger {
  private final SparkSession spark;
  private final HourlyParquetScanner scanner;
  private final MaintenanceConfig config;

  long rowCount(Path path) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(path, spark.sparkContext().hadoopConfiguration()))) {
      long rows = 0;
      for (var block : reader.getFooter().getBlocks()) {
        rows = Math.addExact(rows, block.getRowCount());
      }
      return rows;
    } catch (RuntimeException invalidParquet) {
      throw new IOException("Cannot read Parquet metadata from " + path, invalidParquet);
    }
  }

  long writeMergedFile(HourlyParquetFiles directory, List<ParquetFileSnapshot> files, Path staging)
      throws IOException {
    long rows = 0;
    String[] sourcePaths = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      Path path = new Path(directory.path(), files.get(i).name());
      rows = Math.addExact(rows, rowCount(path));
      sourcePaths[i] = path.toString();
    }

    if ("ZSTD".equals(config.getCompressionCodec())) {
      spark
          .sparkContext()
          .hadoopConfiguration()
          .setLong("parquet.compression.codec.zstd.level", config.getZstdCompressionLevel());
    }

    spark
        .read()
        .option("mergeSchema", "true")
        .parquet(sourcePaths)
        .coalesce(1)
        .write()
        .mode(SaveMode.ErrorIfExists)
        .option("compression", config.getCompressionCodec())
        .parquet(staging.toString());

    List<ParquetFileSnapshot> output = scanner.listParquetFiles(staging);
    if (output.size() != 1) {
      throw new IOException("Expected one merged Parquet file in " + staging);
    }
    if (rowCount(new Path(staging, output.get(0).name())) != rows) {
      throw new IOException("Merged Parquet validation failed: " + staging);
    }
    return rows;
  }
}
