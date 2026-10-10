package io.github.panabigdata.cube.maker;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface BasicCubeMaker {
  String getTableName();

  Dataset<Row> processDataFrame(Dataset<Row> dataframe);

  List<String> primaryKeys();

  String toString();
}
