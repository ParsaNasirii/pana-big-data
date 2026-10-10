package io.github.panabigdata.cube.maker;

import static io.github.panabigdata.cube.CubeColumns.*;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ProtocolUsageCubeMaker implements BasicCubeMaker {

  @Override
  public String getTableName() {
    return "agg_protocol_usage";
  }

  @Override
  public String toString() {
    return "ProtocolUsage";
  }

  @Override
  public Dataset<Row> processDataFrame(Dataset<Row> dataframe) {

    return dataframe
        .select(
            col("PROTOCOL").alias(PROTOCOL),
            col("IN_BYTES").alias(IN_BYTES),
            col("OUT_BYTES").alias(OUT_BYTES),
            col("IN_PKTS").alias(IN_PKTS),
            col("OUT_PKTS").alias(OUT_PKTS))
        .groupBy(PROTOCOL)
        .agg(
            sum(col(IN_BYTES).plus(col(OUT_BYTES))).alias(TOTAL_BYTES),
            count(PROTOCOL).alias(TOTAL_FLOWS));
  }

  @Override
  public List<String> primaryKeys() {
    List<String> primaryKeys = new ArrayList<>(List.of(KAFKA_TIMESTAMP_BUCKET));
    primaryKeys.add(PROTOCOL);
    return primaryKeys;
  }
}
