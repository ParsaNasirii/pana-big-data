package io.github.panabigdata.cube.maker;

import static io.github.panabigdata.cube.CubeColumns.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class IPTrafficCubeProducerTest extends SampleCubeProducerData {

  private static IPTrafficCubeMaker producer;
  private static Dataset<Row> producerResult;

  @BeforeAll
  static void setUpProducer() {
    producer = new IPTrafficCubeMaker();
    producerResult = producer.processDataFrame(sampleData);
  }

  @Test
  void testCubeNameCorrectness() {
    assertEquals("IPTraffic", producer.toString());
  }

  @Test
  void testTableNameCorrectness() {
    assertEquals("agg_ip_traffic", producer.getTableName());
  }

  @Test
  void testUniqueIPCount() {
    List<Row> rows = producerResult.collectAsList();
    assertEquals(6, rows.size());
  }

  @Test
  void testResultSchemaCorrectness() {
    Dataset<Row> producerResult = producer.processDataFrame(sampleData);
    StructType resultSchema = producerResult.schema();

    String[] expectedColumns = {
      IP_ADDRESS,
      TOTAL_IN_BYTES,
      TOTAL_OUT_BYTES,
      TOTAL_BYTES,
      TOTAL_IN_PKTS,
      TOTAL_OUT_PKTS,
      TOTAL_PKTS
    };
    String[] actualColumns = resultSchema.fieldNames();

    assertArrayEquals(expectedColumns, actualColumns, "Schema should have correct column names");

    assertEquals(DataTypes.StringType, resultSchema.apply(IP_ADDRESS).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_IN_BYTES).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_OUT_BYTES).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_BYTES).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_IN_PKTS).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_OUT_PKTS).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_PKTS).dataType());
  }

  @Test
  void testEmptyDataframe() {
    Dataset<Row> emptyData = spark.createDataFrame(List.of(), sampleSchema);
    Dataset<Row> result = producer.processDataFrame(emptyData);

    assertEquals(0, result.count(), "Should return empty dataframe for empty input");
  }

  @Test
  void testZeroByteValues() {
    List<Row> rows =
        List.of(RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 0L, 0L, 0L, 0L, NOW));

    Dataset<Row> zeroData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(zeroData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(2, resultRows.size());

    for (Row row : resultRows) {
      assertEquals(0L, (long) row.getAs(TOTAL_IN_BYTES));
      assertEquals(0L, (long) row.getAs(TOTAL_OUT_BYTES));
      assertEquals(0L, (long) row.getAs(TOTAL_BYTES));
      assertEquals(0L, (long) row.getAs(TOTAL_IN_PKTS));
      assertEquals(0L, (long) row.getAs(TOTAL_OUT_PKTS));
      assertEquals(0L, (long) row.getAs(TOTAL_PKTS));
    }
  }

  @Test
  void testPacketAggregationForIP() {
    List<Row> rows = producerResult.collectAsList();

    Row ipRow =
        rows.stream()
            .filter(row -> "192.168.1.1".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(ipRow, "Should find row for 192.168.1.1");

    assertEquals(2L, (long) ipRow.getAs(TOTAL_IN_PKTS));
    assertEquals(5L, (long) ipRow.getAs(TOTAL_OUT_PKTS));
    assertEquals(7L, (long) ipRow.getAs(TOTAL_PKTS));
  }

  @Test
  void testSingleDirectionTraffic() {
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.10", "10.0.0.20", "TCP", 1000L, 2000L, 2L, 3L, NOW),
            RowFactory.create("192.168.1.10", "10.0.0.30", "UDP", 500L, 300L, 4L, 5L, NOW + 500));

    Dataset<Row> singleDirData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(singleDirData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(3, resultRows.size());

    Row srcRow =
        resultRows.stream()
            .filter(row -> "192.168.1.10".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(srcRow);
    assertEquals(0L, (long) srcRow.getAs(TOTAL_IN_BYTES));
    assertEquals(1500L, (long) srcRow.getAs(TOTAL_OUT_BYTES));
    assertEquals(1500L, (long) srcRow.getAs(TOTAL_BYTES));
    assertEquals(0L, (long) srcRow.getAs(TOTAL_IN_PKTS));
    assertEquals(6L, (long) srcRow.getAs(TOTAL_OUT_PKTS));
    assertEquals(6L, (long) srcRow.getAs(TOTAL_PKTS));
  }

  @Test
  void testIPActingAsBothSourceAndDestination() {
    Dataset<Row> result = producer.processDataFrame(sampleData);
    List<Row> rows = result.collectAsList();

    Row ipRow =
        rows.stream()
            .filter(row -> "192.168.1.1".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(ipRow, "Should find row for 192.168.1.1");

    assertTrue((long) ipRow.getAs(TOTAL_IN_BYTES) > 0, "Should have inbound traffic");
    assertTrue((long) ipRow.getAs(TOTAL_OUT_BYTES) > 0, "Should have outbound traffic");
    assertTrue((long) ipRow.getAs(TOTAL_BYTES) > 0, "Should have total traffic");
    assertTrue((long) ipRow.getAs(TOTAL_IN_PKTS) > 0, "Should have inbound packets");
    assertTrue((long) ipRow.getAs(TOTAL_OUT_PKTS) > 0, "Should have outbound packets");
    assertTrue((long) ipRow.getAs(TOTAL_PKTS) > 0, "Should have total packets");
  }

  @Test
  void testLargeByteValues() {
    long largeValue = Long.MAX_VALUE / 4;
    List<Row> rows =
        List.of(
            RowFactory.create(
                "192.168.1.1", "10.0.0.1", "TCP", largeValue, largeValue, 1L, 1L, NOW),
            RowFactory.create(
                "192.168.1.1", "10.0.0.2", "TCP", largeValue, largeValue, 1L, 1L, NOW + 1000));

    Dataset<Row> largeData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(largeData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(3, resultRows.size());

    Row srcRow =
        resultRows.stream()
            .filter(row -> "192.168.1.1".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(srcRow, "Should find row for 192.168.1.1");
    assertEquals(largeValue * 2, (long) srcRow.getAs(TOTAL_OUT_BYTES));
    assertEquals(0L, (long) srcRow.getAs(TOTAL_IN_BYTES));
    assertEquals(largeValue * 2, (long) srcRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testBidirectionalTrafficForSameIP() {
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.100", "10.0.0.200", "TCP", 100L, 200L, 1L, 2L, NOW),
            RowFactory.create("10.0.0.200", "192.168.1.100", "TCP", 300L, 400L, 3L, 4L, NOW + 1000),
            RowFactory.create("192.168.1.100", "10.0.0.200", "UDP", 50L, 60L, 5L, 6L, NOW + 2000));

    Dataset<Row> bidirData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(bidirData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(2, resultRows.size());

    Row ip1Row =
        resultRows.stream()
            .filter(row -> "192.168.1.100".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(ip1Row);
    assertEquals(150L, (long) ip1Row.getAs(TOTAL_OUT_BYTES));
    assertEquals(400L, (long) ip1Row.getAs(TOTAL_IN_BYTES));
    assertEquals(550L, (long) ip1Row.getAs(TOTAL_BYTES));
    assertEquals(6L, (long) ip1Row.getAs(TOTAL_OUT_PKTS));
    assertEquals(4L, (long) ip1Row.getAs(TOTAL_IN_PKTS));
    assertEquals(10L, (long) ip1Row.getAs(TOTAL_PKTS));

    Row ip2Row =
        resultRows.stream()
            .filter(row -> "10.0.0.200".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(ip2Row);
    assertEquals(260L, (long) ip2Row.getAs(TOTAL_IN_BYTES));
    assertEquals(300L, (long) ip2Row.getAs(TOTAL_OUT_BYTES));
    assertEquals(560L, (long) ip2Row.getAs(TOTAL_BYTES));
    assertEquals(8L, (long) ip2Row.getAs(TOTAL_IN_PKTS));
    assertEquals(3L, (long) ip2Row.getAs(TOTAL_OUT_PKTS));
    assertEquals(11L, (long) ip2Row.getAs(TOTAL_PKTS));
  }

  @Test
  void testAggregationForSourceAndDestinationIPs() {
    List<Row> rows = producerResult.collectAsList();

    Row srcAggregatedRow =
        rows.stream()
            .filter(row -> "192.168.1.1".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(srcAggregatedRow, "Should find aggregated row for 192.168.1.1");

    assertEquals(3000L, (long) srcAggregatedRow.getAs(TOTAL_IN_BYTES));
    assertEquals(7500L, (long) srcAggregatedRow.getAs(TOTAL_OUT_BYTES));

    Row dstAggregatedRow =
        rows.stream()
            .filter(row -> "10.0.0.1".equals(row.getAs(IP_ADDRESS)))
            .findFirst()
            .orElse(null);

    assertNotNull(dstAggregatedRow, "Should find aggregated row for 10.0.0.1");

    assertEquals(3200L, (long) dstAggregatedRow.getAs(TOTAL_IN_BYTES));
    assertEquals(9000L, (long) dstAggregatedRow.getAs(TOTAL_OUT_BYTES));
    assertEquals(12200L, (long) dstAggregatedRow.getAs(TOTAL_BYTES));
  }
}
