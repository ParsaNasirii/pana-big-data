package io.github.panabigdata;

import java.util.List;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class StarRocksNetFlowRow {

  private final String csvLine;

  public static StarRocksNetFlowRow from(
      ConsumerRecord<byte[], GenericRecord> record, List<String> columns) {
    Objects.requireNonNull(record, "record must not be null");
    return from(record, record.timestamp(), columns);
  }

  public static StarRocksNetFlowRow from(
      ConsumerRecord<byte[], GenericRecord> record, long kafkaTimestampMs, List<String> columns) {
    return new StarRocksNetFlowRow(record, kafkaTimestampMs, columns);
  }

  private StarRocksNetFlowRow(
      ConsumerRecord<byte[], GenericRecord> record, long kafkaTimestampMs, List<String> columns) {
    Objects.requireNonNull(record, "record must not be null");
    GenericRecord netFlow = Objects.requireNonNull(record.value(), "record value must not be null");
    Objects.requireNonNull(columns, "columns must not be null");
    requireCsvSafeIp(((CharSequence) netFlow.get("IPV4_SRC_ADDR")).toString());
    requireCsvSafeIp(((CharSequence) netFlow.get("IPV4_DST_ADDR")).toString());

    StringBuilder csv = new StringBuilder();
    for (String column : columns) {
      csv.append(netFlow.get(column)).append(',');
    }
    csv.append(kafkaTimestampMs).append('\n');
    this.csvLine = csv.toString();
  }

  void appendCsv(StringBuilder csv) {
    csv.append(csvLine);
  }

  private static void requireCsvSafeIp(String ip) {
    if (ip.indexOf(',') >= 0 || ip.indexOf('\n') >= 0 || ip.indexOf('\r') >= 0) {
      throw new IllegalArgumentException("IPv4 address must not contain CSV separators");
    }
  }
}
