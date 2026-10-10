package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StarRocksNetFlowRowTest {

  @Test
  void shouldMapEveryAvroFieldAndKafkaTimestampToDeclaredCsvColumns() {
    ConsumerRecord<byte[], GenericRecord> record = StarRocksTestData.record();
    record.value().put("IN_BYTES", Long.MAX_VALUE);
    record.value().put("L7_PROTO", 1.25);
    long timestampMs = 1_723_456_789_000L;
    String csv = csv(StarRocksNetFlowRow.from(record, timestampMs, StarRocksTestData.COLUMNS));
    String[] columns =
        Stream.concat(StarRocksTestData.COLUMNS.stream(), Stream.of("KAFKA_TIMESTAMP_MS"))
            .toArray(String[]::new);
    String[] values = csv.stripTrailing().split(",", -1);

    assertThat(columns).doesNotHaveDuplicates();
    assertThat(values).hasSize(columns.length);
    for (int index = 0; index < StarRocksTestData.COLUMNS.size(); index++) {
      assertThat(values[index])
          .as(columns[index])
          .isEqualTo(record.value().get(columns[index]).toString());
    }
    assertThat(values[StarRocksTestData.COLUMNS.size()]).isEqualTo(Long.toString(timestampMs));
    assertThat(csv).endsWith("\n");
    assertThat(csv.lines()).hasSize(1);
  }

  @Test
  void shouldUseRecordTimestampByDefault() {
    long timestampMs = 1_723_456_789_123L;
    ConsumerRecord<byte[], GenericRecord> record =
        new ConsumerRecord<>(
            "netflow",
            0,
            10L,
            timestampMs,
            TimestampType.CREATE_TIME,
            0,
            0,
            null,
            StarRocksTestData.record().value(),
            new RecordHeaders(),
            Optional.empty());

    assertThat(csv(StarRocksNetFlowRow.from(record, StarRocksTestData.COLUMNS)))
        .endsWith("," + timestampMs + "\n");
  }

  @Test
  void shouldKeepTheRowSnapshotWhenOriginalRecordChanges() {
    ConsumerRecord<byte[], GenericRecord> record = StarRocksTestData.record();
    StarRocksNetFlowRow row = StarRocksNetFlowRow.from(record, 1_000L, StarRocksTestData.COLUMNS);
    String before = csv(row);
    ((Utf8) ((CharSequence) record.value().get("IPV4_SRC_ADDR"))).set("192.0.2.1");
    ((Utf8) ((CharSequence) record.value().get("IPV4_DST_ADDR"))).set("192.0.2.2");
    record.value().put("IN_BYTES", 1L);

    assertThat(csv(row)).isEqualTo(before);
    StringBuilder repeated = new StringBuilder();
    row.appendCsv(repeated);
    row.appendCsv(repeated);
    assertThat(repeated.toString()).isEqualTo(before + before);
  }

  @ParameterizedTest
  @ValueSource(strings = {"10.1,2.3", "10.1\n2.3", "10.1\r2.3"})
  void shouldRejectCsvSeparatorsInEitherIpAddress(String invalidIp) {
    ConsumerRecord<byte[], GenericRecord> sourceRecord = StarRocksTestData.record();
    sourceRecord.value().put("IPV4_SRC_ADDR", new Utf8(invalidIp));
    assertThatThrownBy(() -> StarRocksNetFlowRow.from(sourceRecord, StarRocksTestData.COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CSV separators");

    ConsumerRecord<byte[], GenericRecord> destinationRecord = StarRocksTestData.record();
    destinationRecord.value().put("IPV4_DST_ADDR", new Utf8(invalidIp));
    assertThatThrownBy(() -> StarRocksNetFlowRow.from(destinationRecord, StarRocksTestData.COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CSV separators");
  }

  @Test
  void shouldRejectNullKafkaRecord() {
    assertThatThrownBy(() -> StarRocksNetFlowRow.from(null, StarRocksTestData.COLUMNS))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("record must not be null");
  }

  @Test
  void shouldRejectKafkaRecordWithNullValue() {
    ConsumerRecord<byte[], GenericRecord> record =
        new ConsumerRecord<>("netflow", 0, 10L, null, null);

    assertThatThrownBy(() -> StarRocksNetFlowRow.from(record, StarRocksTestData.COLUMNS))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("record value must not be null");
  }

  private String csv(StarRocksNetFlowRow row) {
    StringBuilder csv = new StringBuilder();
    row.appendCsv(csv);
    return csv.toString();
  }
}
