package io.github.panabigdata.storage;

import io.github.panabigdata.cube.maker.BasicCubeMaker;
import java.sql.SQLException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface CubeWriter extends AutoCloseable {
  void writeCube(Dataset<Row> dataframe, BasicCubeMaker maker) throws SQLException;

  @Override
  void close() throws SQLException;
}
