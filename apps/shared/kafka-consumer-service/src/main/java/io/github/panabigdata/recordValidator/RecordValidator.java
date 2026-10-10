package io.github.panabigdata.recordValidator;

import io.github.panabigdata.recordValidator.rules.*;
import java.util.List;
import org.apache.avro.generic.GenericRecord;

public class RecordValidator {

  private static final List<ValidationRule> RULES =
      List.of(
          new NetworkValidationRule(),
          new TcpValidationRule(),
          new IcmpValidationRule(),
          new DnsValidationRule(),
          new FtpValidationRule());

  private RecordValidator() {}

  public static boolean isValid(GenericRecord record) {
    if (record == null) {
      return false;
    }

    return RULES.stream().allMatch(rule -> rule.isValid(record));
  }
}
