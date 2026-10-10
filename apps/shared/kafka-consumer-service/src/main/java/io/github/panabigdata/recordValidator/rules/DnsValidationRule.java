package io.github.panabigdata.recordValidator.rules;

import org.apache.avro.generic.GenericRecord;

public class DnsValidationRule implements ValidationRule {

  @Override
  public boolean isValid(GenericRecord record) {
    return isValidDnsQueryId(((Integer) record.get("DNS_QUERY_ID")))
        && isValidDnsQueryType(((Integer) record.get("DNS_QUERY_TYPE")))
        && ((Long) record.get("DNS_TTL_ANSWER")) >= 0;
  }

  private boolean isValidDnsQueryId(int value) {
    return value >= 0 && value <= 65535;
  }

  private boolean isValidDnsQueryType(int value) {
    return value >= 0 && value <= 65535;
  }
}
