package io.github.panabigdata.recordValidator.rules;

import org.apache.avro.generic.GenericRecord;

public class TcpValidationRule implements ValidationRule {

  @Override
  public boolean isValid(GenericRecord record) {
    return isValidTcpFlags(((Integer) record.get("TCP_FLAGS")))
        && isValidTcpFlags(((Integer) record.get("CLIENT_TCP_FLAGS")))
        && isValidTcpFlags(((Integer) record.get("SERVER_TCP_FLAGS")))
        && isValidTcpWindow(((Integer) record.get("TCP_WIN_MAX_IN")))
        && isValidTcpWindow(((Integer) record.get("TCP_WIN_MAX_OUT")));
  }

  private boolean isValidTcpFlags(int flags) {
    return flags >= 0 && flags <= 255;
  }

  private boolean isValidTcpWindow(int window) {
    return window >= 0 && window <= 65535;
  }
}
