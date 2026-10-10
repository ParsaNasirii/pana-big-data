package io.github.panabigdata;

import io.github.panabigdata.kafka.RandomRecordGenerator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class StarRocksTestData {

  private static final long FIXED_KAFKA_TIMESTAMP_MS = 1723456789000L;
  static final List<String> COLUMNS =
      record().value().getSchema().getFields().stream().map(Schema.Field::name).toList();

  private StarRocksTestData() {}

  public static ConsumerRecord<byte[], GenericRecord> record() {
    GenericRecord value = RandomRecordGenerator.generate(1, true).get(0);
    value.put("IPV4_SRC_ADDR", new Utf8("10.1.2.3"));
    value.put("IPV4_DST_ADDR", new Utf8("10.9.8.7"));

    return new ConsumerRecord<>("netflow", 0, 100L, null, value);
  }

  public static StarRocksNetFlowRow row() {
    return StarRocksNetFlowRow.from(record(), FIXED_KAFKA_TIMESTAMP_MS, COLUMNS);
  }
}
