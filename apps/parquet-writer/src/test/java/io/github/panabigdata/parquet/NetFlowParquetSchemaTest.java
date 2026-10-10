package io.github.panabigdata.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.panabigdata.kafka.RandomRecordGenerator;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class NetFlowParquetSchemaTest {
  private final Schema schema = NetFlowParquetSchema.createSchema(RandomRecordGenerator.schema());

  @Test
  void testSchemaName() {
    Schema source = RandomRecordGenerator.schema();

    assertEquals(source.getName() + "WithTimestamp", schema.getName());
  }

  @Test
  void testSchemaNamespace() {
    Schema source = RandomRecordGenerator.schema();

    assertEquals(source.getNamespace(), schema.getNamespace());
  }

  @Test
  void testSchemaContainsAllSourceFields() {
    Schema source = RandomRecordGenerator.schema();

    for (Schema.Field field : source.getFields()) {
      Schema.Field resultField = schema.getField(field.name());

      assertNotNull(resultField);
      assertEquals(field.schema(), resultField.schema());
    }
  }

  @Test
  void testkafkaTimestampFieldExists() {
    Schema.Field field = schema.getField(NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD);

    assertNotNull(field);
    assertEquals(Schema.Type.STRING, field.schema().getType());
  }

  @Test
  void testkafkaTimestampFieldIsLast() {
    Schema.Field lastField = schema.getFields().get(schema.getFields().size() - 1);

    assertEquals(NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD, lastField.name());
  }

  @Test
  void testkafkaTimestampFieldHasNoDefaultValue() {
    Schema.Field field = schema.getField(NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD);

    assertNotNull(field);
    assertFalse(field.hasDefaultValue());
  }

  @Test
  void testSourceFieldDefaultsArePreserved() {
    Schema source = RandomRecordGenerator.schema();

    for (Schema.Field field : source.getFields()) {
      Schema.Field resultField = schema.getField(field.name());

      assertNotNull(resultField);
      assertEquals(field.hasDefaultValue(), resultField.hasDefaultValue());

      if (field.hasDefaultValue()) {
        assertEquals(field.defaultVal(), resultField.defaultVal());
      }
    }
  }

  @Test
  void testkafkaTimestampFieldConstant() {
    assertEquals("KAFKA_TIMESTAMP", NetFlowParquetSchema.KAFKA_TIMESTAMP_FIELD);
  }
}
