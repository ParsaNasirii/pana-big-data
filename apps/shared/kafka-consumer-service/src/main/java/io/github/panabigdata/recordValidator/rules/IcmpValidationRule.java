package io.github.panabigdata.recordValidator.rules;

import org.apache.avro.generic.GenericRecord;

public class IcmpValidationRule implements ValidationRule {

  @Override
  public boolean isValid(GenericRecord record) {
    return isValidIcmpType(((Integer) record.get("ICMP_TYPE")))
        && isValidIcmpV4Type(((Integer) record.get("ICMP_IPV4_TYPE")));
  }

  private boolean isValidIcmpType(int value) {
    return value >= 0 && value <= 65535;
  }

  private boolean isValidIcmpV4Type(int value) {
    return value >= 0 && value <= 255;
  }
}
