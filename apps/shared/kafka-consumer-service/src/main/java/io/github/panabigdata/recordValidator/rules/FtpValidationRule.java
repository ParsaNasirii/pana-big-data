package io.github.panabigdata.recordValidator.rules;

import org.apache.avro.generic.GenericRecord;

public class FtpValidationRule implements ValidationRule {

  @Override
  public boolean isValid(GenericRecord record) {
    return isValidFtpReturnCode(((Integer) record.get("FTP_COMMAND_RET_CODE")));
  }

  private boolean isValidFtpReturnCode(int code) {
    return code == 0 || (code >= 100 && code <= 599);
  }
}
