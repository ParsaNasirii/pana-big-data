package io.github.panabigdata.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public final class NetFlowParquetSchema {

  public static final String KAFKA_TIMESTAMP_FIELD = "KAFKA_TIMESTAMP";

  private NetFlowParquetSchema() {}

  public static Schema createSchema(Schema source) {

    SchemaBuilder.FieldAssembler<Schema> builder =
        SchemaBuilder.record(source.getName() + "WithTimestamp")
            .namespace(source.getNamespace())
            .fields();

    for (Schema.Field field : source.getFields()) {
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder = builder.name(field.name());

      if (field.hasDefaultValue()) {
        fieldBuilder.type(field.schema()).withDefault(field.defaultVal());
      } else {
        fieldBuilder.type(field.schema()).noDefault();
      }
    }

    return builder.name(KAFKA_TIMESTAMP_FIELD).type().stringType().noDefault().endRecord();
  }
}
