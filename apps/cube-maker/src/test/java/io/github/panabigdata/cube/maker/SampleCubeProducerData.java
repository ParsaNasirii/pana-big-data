package io.github.panabigdata.cube.maker;

import io.github.panabigdata.BaseSparkTest;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;

class SampleCubeProducerData extends BaseSparkTest {

  protected static final long NOW = 1700000000000L;
  protected static Dataset<Row> sampleData;
  protected static StructType sampleSchema;

  @BeforeAll
  static void setUpData() {
    sampleData = sampleDataset();
    sampleSchema = sampleSchema();
  }

  private static Dataset<Row> sampleDataset() {
    StructType schema = sampleSchema();

    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 1000L, 500L, 2L, 1L, NOW),
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 2000L, 1000L, 1L, 1L, NOW + 1000),
            RowFactory.create("192.168.1.1", "10.0.0.1", "UDP", 3000L, 1500L, 1L, 2L, NOW + 2000),
            RowFactory.create("10.0.0.1", "192.168.1.1", "TCP", 6000L, 1500L, 1L, 1L, NOW + 1600),
            RowFactory.create("10.0.0.1", "192.168.1.1", "UDP", 3000L, 1500L, 1L, 1L, NOW + 2100),
            RowFactory.create("192.168.1.2", "10.0.0.1", "TCP", 500L, 200L, 1L, 1L, NOW + 3000),
            RowFactory.create("192.168.1.1", "10.0.0.2", "TCP", 1500L, 750L, 1L, 10L, NOW + 4000),
            RowFactory.create("10.0.0.3", "10.0.0.4", "ICMP", 100L, 50L, 5L, 15L, NOW + 5000));

    return spark.createDataFrame(rows, schema);
  }

  private static StructType sampleSchema() {
    return new StructType()
        .add("IPV4_SRC_ADDR", DataTypes.StringType, false)
        .add("IPV4_DST_ADDR", DataTypes.StringType, false)
        .add("PROTOCOL", DataTypes.StringType, false)
        .add("IN_BYTES", DataTypes.LongType, false)
        .add("OUT_BYTES", DataTypes.LongType, false)
        .add("IN_PKTS", DataTypes.LongType, false)
        .add("OUT_PKTS", DataTypes.LongType, false)
        .add("KAFKA_TIMESTAMP", DataTypes.LongType, false);
  }
}
