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

class IPToIPConnectionCubeProducerTest extends SampleCubeProducerData {

  private static IPToIPConnectionCubeMaker producer;
  private static Dataset<Row> producerResult;

  @BeforeAll
  static void setUpProducer() {
    producer = new IPToIPConnectionCubeMaker();
    producerResult = producer.processDataFrame(sampleData);
  }

  @Test
  void testNameCorrectness() {
    assertEquals("IPToIPConnection", producer.toString());
  }

  @Test
  void testTableNameCorrectness() {
    assertEquals("agg_ip_to_ip_connection", producer.getTableName());
  }

  @Test
  void testUniqueIPPairsCount() {
    List<Row> rows = producerResult.collectAsList();
    assertEquals(5, rows.size());
  }

  @Test
  void testResultSchemaCorrectness() {
    Dataset<Row> producerResult = producer.processDataFrame(sampleData);
    StructType resultSchema = producerResult.schema();

    String[] expectedColumns = {SRC_IP, DST_IP, TOTAL_FLOWS, TOTAL_BYTES};
    String[] actualColumns = resultSchema.fieldNames();

    assertArrayEquals(expectedColumns, actualColumns, "Schema should have correct column names");

    assertEquals(DataTypes.StringType, resultSchema.apply(SRC_IP).dataType());
    assertEquals(DataTypes.StringType, resultSchema.apply(DST_IP).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_FLOWS).dataType());
    assertEquals(DataTypes.LongType, resultSchema.apply(TOTAL_BYTES).dataType());
  }

  @Test
  void testSingleFlowBetweenIPPair() {
    Dataset<Row> result = producer.processDataFrame(sampleData);
    List<Row> rows = result.collectAsList();

    Row singleFlowRow =
        rows.stream()
            .filter(
                row ->
                    "192.168.1.2".equals(row.getAs(SRC_IP)) && "10.0.0.1".equals(row.getAs(DST_IP)))
            .findFirst()
            .orElse(null);

    assertNotNull(singleFlowRow, "Should find row for 192.168.1.2 -> 10.0.0.1");

    assertEquals(1L, (long) singleFlowRow.getAs(TOTAL_FLOWS));
    assertEquals(700L, (long) singleFlowRow.getAs(TOTAL_BYTES));
  }

  @Test
  void testBidirectionalByteCalculation() {
    Dataset<Row> result = producer.processDataFrame(sampleData);
    List<Row> rows = result.collectAsList();

    Row bidirectionalRow =
        rows.stream()
            .filter(
                row -> "10.0.0.3".equals(row.getAs(SRC_IP)) && "10.0.0.4".equals(row.getAs(DST_IP)))
            .findFirst()
            .orElse(null);

    assertNotNull(bidirectionalRow, "Should find row for 10.0.0.3 -> 10.0.0.4");

    assertEquals(150L, (long) bidirectionalRow.getAs(TOTAL_BYTES));
    assertEquals(1L, (long) bidirectionalRow.getAs(TOTAL_FLOWS));
  }

  @Test
  void testEmptyDataframe() {
    Dataset<Row> emptyData = spark.createDataFrame(List.of(), sampleSchema);
    Dataset<Row> result = producer.processDataFrame(emptyData);

    assertEquals(0, result.count(), "Should return empty dataframe for empty input");
  }

  @Test
  void testAggregationForMultipleFlowsBetweenSameIPs() {
    List<Row> rows = producerResult.collectAsList();

    Row aggregatedRow =
        rows.stream()
            .filter(
                row ->
                    "192.168.1.1".equals(row.getAs(SRC_IP)) && "10.0.0.1".equals(row.getAs(DST_IP)))
            .findFirst()
            .orElse(null);

    Row aggregatedRow2 =
        rows.stream()
            .filter(
                row ->
                    "10.0.0.1".equals(row.getAs(SRC_IP)) && "192.168.1.1".equals(row.getAs(DST_IP)))
            .findFirst()
            .orElse(null);

    assertNotNull(aggregatedRow, "Should find aggregated row for 192.168.1.1 -> 10.0.0.1");
    assertNotNull(aggregatedRow2, "Should find aggregated row for 10.0.0.1 -> 192.168.1.1");

    assertEquals(3L, (long) aggregatedRow.getAs(TOTAL_FLOWS));
    assertEquals(9000L, (long) aggregatedRow.getAs(TOTAL_BYTES));

    assertEquals(2L, (long) aggregatedRow2.getAs(TOTAL_FLOWS));
    assertEquals(12000L, (long) aggregatedRow2.getAs(TOTAL_BYTES));
  }

  @Test
  void testLargeByteValues() {
    long largeValue = Long.MAX_VALUE / 4;
    List<Row> rows =
        List.of(
            RowFactory.create("192.168.1.1", "10.0.0.1", "TCP", largeValue, largeValue, NOW),
            RowFactory.create(
                "192.168.1.1", "10.0.0.1", "TCP", largeValue, largeValue, NOW + 1000));

    Dataset<Row> largeData = spark.createDataFrame(rows, sampleSchema);
    Dataset<Row> result = producer.processDataFrame(largeData);

    List<Row> resultRows = result.collectAsList();
    assertEquals(1, resultRows.size());

    long expectedTotal = (largeValue + largeValue) * 2;
    assertEquals(expectedTotal, (long) resultRows.get(0).getAs(TOTAL_BYTES));
    assertEquals(2L, (long) resultRows.get(0).getAs(TOTAL_FLOWS));
  }
}
