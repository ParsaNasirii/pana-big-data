package io.github.panabigdata.cube;

import io.github.panabigdata.BatchDirectoryService;
import io.github.panabigdata.cube.maker.BasicCubeMaker;
import io.github.panabigdata.metrics.CubeMakerMetrics;
import io.github.panabigdata.storage.CubeWriter;
import java.util.List;
import org.apache.spark.sql.SparkSession;

public class CubeMakerFactory {

  private CubeMakerFactory() {
    throw new IllegalStateException("Utility class");
  }

  public static CubeMakerService create(
      SparkSession spark,
      List<BasicCubeMaker> producers,
      CubeWriter writer,
      BatchDirectoryService directories,
      CubeMakerMetrics metrics) {
    return new CubeMakerService(spark, List.copyOf(producers), writer, directories, metrics);
  }
}
