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

class ProtocolUsageCubeProducerTest extends SampleCubeProducerData {

  private static ProtocolUsageCubeMaker producer;
  private static Dataset<Row> producerResult;

  @BeforeAll
  static void setUpProducer() {
    producer = new ProtocolUsageCubeMaker();
    producerResult = producer.processDataFrame(sampleData);
  }

  @Test
  void testCubeNameCorrectness() {
    assertEquals("ProtocolUsage", producer.toString());
  }

  @Test
  void testTableNameCorrectness() {
    assertEquals("agg_protocol_usage", producer.getTableName());
  }

  @Test
  void testPrimaryKeys() {
    assertEquals(List.of(KAFKA_TIMESTAMP_BUCKET, PROTOCOL), producer.primaryKeys());
  }

  @Test
  void testUniqueProtocolCount() {
    List<Row> rows = producerResult.collectAsList();
    assertEquals(3, rows.size());
  }

  @Test
  void testResultSchemaCorrectness() {
    Dataset<Row> producerResult = producer.processDataFrame(sampleData);
    StructType resultSchema = producerResult.schema();

    String[] expectedColumns = {PROTOCOL, TOTAL_BYTES, TOTAL_FLOWS};
    String[] actualColumns = resultSchema.fieldNames();

    assertArrayEquals(expectedColumns, actualColumns, "Schema should have correct column names");

    assertEquals(DataTypes.StringType, resultSchema.apply(PROTOCOL).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_BYTES).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_FLOWS).dataType());
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
    assertEquals(1, resultRows.size());

    Row tcpRow = resultRows.get(0);
    assertEquals("TCP", tcpRow.getAs(PROTOCOL));
    assertEquals(1L, (long) tcpRow.getAs(TOTAL_FLOWS));
    assertEquals(0L, (long) tcpRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testSingleProtocolAggregation() {
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 100L, 200L, 1L, 2L, NOW),
            RowFactory.create("192.168.1.2", "10.0.0.2", "TCP", 300L, 400L, 3L, 4L, NOW + 1000),
            RowFactory.create("192.168.1.3", "10.0.0.3", "TCP", 500L, 600L, 5L, 6L, NOW + 2000));

    Dataset<Row> tcpOnlyData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(tcpOnlyData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(1, resultRows.size(), "Should have only one protocol");

    Row tcpRow = resultRows.get(0);
    assertEquals("TCP", tcpRow.getAs(PROTOCOL));
    assertEquals(3L, (long) tcpRow.getAs(TOTAL_FLOWS));
    assertEquals(2100L, (long) tcpRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testTCPProtocolAggregation() {
    List<Row> rows = producerResult.collectAsList();

    Row tcpRow =
        rows.stream().filter(row -> "TCP".equals(row.getAs(PROTOCOL))).findFirst().orElse(null);

    assertNotNull(tcpRow, "Should find aggregated row for TCP protocol");

    assertEquals(5L, (long) tcpRow.getAs(TOTAL_FLOWS));
    assertEquals(14950L, (long) tcpRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testUDPProtocolAggregation() {
    List<Row> rows = producerResult.collectAsList();

    Row udpRow =
        rows.stream().filter(row -> "UDP".equals(row.getAs(PROTOCOL))).findFirst().orElse(null);

    assertNotNull(udpRow, "Should find aggregated row for UDP protocol");

    assertEquals(2L, (long) udpRow.getAs(TOTAL_FLOWS));
    assertEquals(9000L, (long) udpRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testICMPProtocolAggregation() {
    List<Row> rows = producerResult.collectAsList();

    Row icmpRow =
        rows.stream().filter(row -> "ICMP".equals(row.getAs(PROTOCOL))).findFirst().orElse(null);

    assertNotNull(icmpRow, "Should find aggregated row for ICMP protocol");

    assertEquals(1L, (long) icmpRow.getAs(TOTAL_FLOWS));
    assertEquals(150L, (long) icmpRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testLargeByteValues() {
    long largeValue = Long.MAX_VALUE / 4;
    List<Row> rows =
        List.of(
            RowFactory.create(
                "192.168.1.1", "10.0.0.1", "TCP", largeValue, largeValue, 1L, 1L, NOW),
            RowFactory.create(
                "192.168.1.2", "10.0.0.2", "TCP", largeValue, largeValue, 1L, 1L, NOW + 1000));

    Dataset<Row> largeData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(largeData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(1, resultRows.size(), "Should have only TCP protocol");

    Row tcpRow = resultRows.get(0);
    assertEquals(2L, (long) tcpRow.getAs(TOTAL_FLOWS));
    assertEquals(largeValue * 4, (long) tcpRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testProtocolCaseSensitivity() {
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 100L, 200L, 1L, 2L, NOW),
            RowFactory.create("192.168.1.2", "10.0.0.2", "tcp", 300L, 400L, 3L, 4L, NOW + 1000),
            RowFactory.create("192.168.1.3", "10.0.0.3", "Tcp", 500L, 600L, 5L, 6L, NOW + 2000));

    Dataset<Row> caseSensitiveData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(caseSensitiveData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(3, resultRows.size(), "Should treat protocols as case-sensitive");
  }

  @Test
  void testProtocolWithSpaces() {
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.1", "10.0.0.1", " TCP", 100L, 200L, 1L, 2L, NOW),
            RowFactory.create("192.168.1.2", "10.0.0.2", "TCP ", 300L, 400L, 3L, 4L, NOW + 1000),
            RowFactory.create("192.168.1.3", "10.0.0.3", "TCP", 500L, 600L, 5L, 6L, NOW + 2000));

    Dataset<Row> spacedProtocolData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(spacedProtocolData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(3, resultRows.size(), "Should treat protocols with spaces as distinct");
  }

  @Test
  void testFlowCountAccuracy() {
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 100L, 200L, 1L, 2L, NOW),
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 300L, 400L, 3L, 4L, NOW + 1000),
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", 500L, 600L, 5L, 6L, NOW + 2000),
            RowFactory.create("192.168.1.2", "10.0.0.2", "TCP", 700L, 800L, 7L, 8L, NOW + 3000),
            RowFactory.create("192.168.1.3", "10.0.0.3", "UDP", 900L, 1000L, 9L, 10L, NOW + 4000));

    Dataset<Row> flowData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(flowData);

    List<Row> resultRows = result.collectAsList();

    Row tcpRow =
        resultRows.stream()
            .filter(row -> "TCP".equals(row.getAs(PROTOCOL)))
            .findFirst()
            .orElse(null);

    Row udpRow =
        resultRows.stream()
            .filter(row -> "UDP".equals(row.getAs(PROTOCOL)))
            .findFirst()
            .orElse(null);

    assertNotNull(tcpRow, "Should find TCP row");
    assertNotNull(udpRow, "Should find UDP row");

    assertEquals(4L, (long) tcpRow.getAs(TOTAL_FLOWS), "TCP should have 4 flows");
    assertEquals(3600L, (long) tcpRow.getAs(TOTAL_BYTES), "TCP should have 3600 total bytes");
    assertEquals(1L, (long) udpRow.getAs(TOTAL_FLOWS), "UDP should have 1 flow");
    assertEquals(1900L, (long) udpRow.getAs(TOTAL_BYTES), "UDP should have 1900 total bytes");
  }
}
