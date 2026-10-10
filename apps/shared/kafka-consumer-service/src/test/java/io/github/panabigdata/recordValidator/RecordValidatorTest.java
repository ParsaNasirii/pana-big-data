package io.github.panabigdata.recordValidator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.panabigdata.kafka.RandomRecordGenerator;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

public class RecordValidatorTest {
  @Test
  void generatedValidRecordsShouldPassValidation() {
    List<GenericRecord> records = RandomRecordGenerator.generate(100, true);

    for (GenericRecord record : records) {
      assertTrue(RecordValidator.isValid(record));
    }
  }

  @Test
  void shouldRejectRepresentativeInvalidRecords() {
    assertFalse(RecordValidator.isValid(null));

    GenericRecord invalidIp = validRecord();
    invalidIp.put("IPV4_SRC_ADDR", "999.1.1.1");
    assertFalse(RecordValidator.isValid(invalidIp));

    GenericRecord invalidPort = validRecord();
    invalidPort.put("L4_SRC_PORT", -1);
    assertFalse(RecordValidator.isValid(invalidPort));

    GenericRecord negativeBytes = validRecord();
    negativeBytes.put("IN_BYTES", -1L);
    assertFalse(RecordValidator.isValid(negativeBytes));

    GenericRecord invalidTtlOrder = validRecord();
    invalidTtlOrder.put("MIN_TTL", 200);
    invalidTtlOrder.put("MAX_TTL", 100);
    assertFalse(RecordValidator.isValid(invalidTtlOrder));
  }

  private GenericRecord validRecord() {
    return RandomRecordGenerator.generate(1, true).get(0);
  }
}
