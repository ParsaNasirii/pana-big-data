package io.github.panabigdata.recordValidator.rules;

import org.apache.avro.generic.GenericRecord;

public class NetworkValidationRule implements ValidationRule {

  @Override
  public boolean isValid(GenericRecord record) {
    return isValidIPv4(((CharSequence) record.get("IPV4_SRC_ADDR")))
        && isValidIPv4(((CharSequence) record.get("IPV4_DST_ADDR")))
        && isValidPort(((Integer) record.get("L4_SRC_PORT")))
        && isValidPort(((Integer) record.get("L4_DST_PORT")))
        && isValidProtocol(((Integer) record.get("PROTOCOL")))
        && isValidNonNegativeDouble(((Double) record.get("L7_PROTO")))
        && isValidCounters(record)
        && isValidTtlAndOrdering(record)
        && isValidPacketLengthsAndOrdering(record);
  }

  private boolean isValidIPv4(CharSequence value) {
    if (value == null) {
      return false;
    }

    String ip = value.toString();
    String[] parts = ip.split("\\.", -1);

    if (parts.length != 4) {
      return false;
    }

    for (String part : parts) {
      if (part.isEmpty()) {
        return false;
      }

      int number;

      try {
        number = Integer.parseInt(part);
      } catch (NumberFormatException e) {
        return false;
      }

      if (number < 0 || number > 255) {
        return false;
      }
    }

    return true;
  }

  private boolean isValidPort(int port) {
    return port >= 0 && port <= 65535;
  }

  private boolean isValidProtocol(int protocol) {
    return protocol >= 0 && protocol <= 255;
  }

  private boolean isValidNonNegativeDouble(double value) {
    return Double.isFinite(value) && value >= 0;
  }

  private boolean isValidCounters(GenericRecord record) {
    return isValidNonNegativeLong(((Long) record.get("IN_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("OUT_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("IN_PKTS")))
        && isValidNonNegativeLong(((Long) record.get("OUT_PKTS")))
        && isValidNonNegativeLong(((Long) record.get("FLOW_DURATION_MILLISECONDS")))
        && isValidNonNegativeLong(((Long) record.get("DURATION_IN")))
        && isValidNonNegativeLong(((Long) record.get("DURATION_OUT")))
        && isValidNonNegativeLong(((Long) record.get("SRC_TO_DST_SECOND_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("DST_TO_SRC_SECOND_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("RETRANSMITTED_IN_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("RETRANSMITTED_IN_PKTS")))
        && isValidNonNegativeLong(((Long) record.get("RETRANSMITTED_OUT_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("RETRANSMITTED_OUT_PKTS")))
        && isValidNonNegativeLong(((Long) record.get("SRC_TO_DST_AVG_THROUGHPUT")))
        && isValidNonNegativeLong(((Long) record.get("DST_TO_SRC_AVG_THROUGHPUT")))
        && isValidNonNegativeLong(((Long) record.get("NUM_PKTS_UP_TO_128_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("NUM_PKTS_128_TO_256_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("NUM_PKTS_256_TO_512_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("NUM_PKTS_512_TO_1024_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("NUM_PKTS_1024_TO_1514_BYTES")))
        && isValidNonNegativeLong(((Long) record.get("DNS_TTL_ANSWER")));
  }

  private boolean isValidNonNegativeLong(long value) {
    return value >= 0;
  }

  private boolean isValidTtlAndOrdering(GenericRecord record) {
    int minTtl = ((Integer) record.get("MIN_TTL"));
    int maxTtl = ((Integer) record.get("MAX_TTL"));

    return isValidTtl(minTtl) && isValidTtl(maxTtl) && minTtl <= maxTtl;
  }

  private boolean isValidTtl(int ttl) {
    return ttl >= 0 && ttl <= 255;
  }

  private boolean isValidPacketLengthsAndOrdering(GenericRecord record) {

    int longest = ((Integer) record.get("LONGEST_FLOW_PKT"));
    int shortest = ((Integer) record.get("SHORTEST_FLOW_PKT"));
    int min = ((Integer) record.get("MIN_IP_PKT_LEN"));
    int max = ((Integer) record.get("MAX_IP_PKT_LEN"));

    return isValidPacketLength(longest)
        && isValidPacketLength(shortest)
        && isValidPacketLength(min)
        && isValidPacketLength(max)
        && shortest <= longest
        && min <= max;
  }

  private boolean isValidPacketLength(int length) {
    return length >= 0 && length <= 65535;
  }
}
