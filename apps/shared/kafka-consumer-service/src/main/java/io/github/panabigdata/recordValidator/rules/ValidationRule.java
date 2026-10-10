package io.github.panabigdata.recordValidator.rules;

import org.apache.avro.generic.GenericRecord;

public interface ValidationRule {

  boolean isValid(GenericRecord record);
}
