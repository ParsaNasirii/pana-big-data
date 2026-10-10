package io.github.panabigdata.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public final class RandomRecordGenerator {

  private static final long SEED = 123456789L;
  private static final Schema SCHEMA =
      SchemaBuilder.record("NetFlowV2Record")
          .namespace("org.panabigdata.netflow.avro")
          .fields()
          .requiredString("IPV4_SRC_ADDR")
          .requiredString("IPV4_DST_ADDR")
          .requiredInt("L4_SRC_PORT")
          .requiredInt("L4_DST_PORT")
          .requiredInt("PROTOCOL")
          .requiredDouble("L7_PROTO")
          .requiredLong("IN_BYTES")
          .requiredLong("OUT_BYTES")
          .requiredLong("IN_PKTS")
          .requiredLong("OUT_PKTS")
          .requiredLong("FLOW_DURATION_MILLISECONDS")
          .requiredInt("TCP_FLAGS")
          .requiredInt("CLIENT_TCP_FLAGS")
          .requiredInt("SERVER_TCP_FLAGS")
          .requiredLong("DURATION_IN")
          .requiredLong("DURATION_OUT")
          .requiredInt("MIN_TTL")
          .requiredInt("MAX_TTL")
          .requiredInt("LONGEST_FLOW_PKT")
          .requiredInt("SHORTEST_FLOW_PKT")
          .requiredInt("MIN_IP_PKT_LEN")
          .requiredInt("MAX_IP_PKT_LEN")
          .requiredLong("SRC_TO_DST_SECOND_BYTES")
          .requiredLong("DST_TO_SRC_SECOND_BYTES")
          .requiredLong("RETRANSMITTED_IN_BYTES")
          .requiredLong("RETRANSMITTED_IN_PKTS")
          .requiredLong("RETRANSMITTED_OUT_BYTES")
          .requiredLong("RETRANSMITTED_OUT_PKTS")
          .requiredLong("SRC_TO_DST_AVG_THROUGHPUT")
          .requiredLong("DST_TO_SRC_AVG_THROUGHPUT")
          .requiredLong("NUM_PKTS_UP_TO_128_BYTES")
          .requiredLong("NUM_PKTS_128_TO_256_BYTES")
          .requiredLong("NUM_PKTS_256_TO_512_BYTES")
          .requiredLong("NUM_PKTS_512_TO_1024_BYTES")
          .requiredLong("NUM_PKTS_1024_TO_1514_BYTES")
          .requiredInt("TCP_WIN_MAX_IN")
          .requiredInt("TCP_WIN_MAX_OUT")
          .requiredInt("ICMP_TYPE")
          .requiredInt("ICMP_IPV4_TYPE")
          .requiredInt("DNS_QUERY_ID")
          .requiredInt("DNS_QUERY_TYPE")
          .requiredLong("DNS_TTL_ANSWER")
          .requiredInt("FTP_COMMAND_RET_CODE")
          .endRecord();

  private RandomRecordGenerator() {}

  public static Schema schema() {
    return SCHEMA;
  }

  public static Schema schemaWithJavaStrings() {
    Schema schema = new Schema.Parser().parse(SCHEMA.toString());
    GenericData.setStringType(
        schema.getField("IPV4_SRC_ADDR").schema(), GenericData.StringType.String);
    GenericData.setStringType(
        schema.getField("IPV4_DST_ADDR").schema(), GenericData.StringType.String);
    return schema;
  }

  public static GenericRecord withSchema(GenericRecord source, Schema schema) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : source.getSchema().getFields()) {
      if (schema.getField(field.name()) != null) {
        builder.set(field.name(), source.get(field.name()));
      }
    }
    return builder.build();
  }

  public static List<GenericRecord> generate(int count, boolean valid) {
    Random random = new Random(SEED);
    List<GenericRecord> records = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      records.add(generateOne(random, valid));
    }

    return records;
  }

  private static GenericRecord generateOne(Random random, boolean valid) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    // IPs are always syntactically valid.
    record.put("IPV4_SRC_ADDR", randomIp(random));
    record.put("IPV4_DST_ADDR", randomIp(random));

    if (valid) {
      generateValid(record, random);
    } else {
      generateRandom(record, random);
    }

    return record;
  }

  private static void generateValid(GenericRecord record, Random random) {

    // Ports
    record.put("L4_SRC_PORT", random.nextInt(65536));
    record.put("L4_DST_PORT", random.nextInt(65536));

    // Protocol
    record.put("PROTOCOL", random.nextInt(256));
    record.put("L7_PROTO", random.nextDouble(0, 256));

    // Counters
    record.put("IN_BYTES", randomNonNegativeLong(random, 1_000_000));
    record.put("OUT_BYTES", randomNonNegativeLong(random, 1_000_000));

    record.put("IN_PKTS", randomNonNegativeLong(random, 10_000));
    record.put("OUT_PKTS", randomNonNegativeLong(random, 10_000));

    // Duration
    record.put("FLOW_DURATION_MILLISECONDS", randomNonNegativeLong(random, 1_000_000));
    record.put("DURATION_IN", randomNonNegativeLong(random, 1_000_000));
    record.put("DURATION_OUT", randomNonNegativeLong(random, 1_000_000));

    // TCP flags
    record.put("TCP_FLAGS", random.nextInt(256));
    record.put("CLIENT_TCP_FLAGS", random.nextInt(256));
    record.put("SERVER_TCP_FLAGS", random.nextInt(256));

    // TTL
    int minTtl = random.nextInt(256);
    int maxTtl = randomIntBetween(random, minTtl, 255);

    record.put("MIN_TTL", minTtl);
    record.put("MAX_TTL", maxTtl);

    // Packet sizes
    int shortestFlowPacket = randomIntBetween(random, 1, 1500);
    int longestFlowPacket = randomIntBetween(random, shortestFlowPacket, 65535);

    int minIpPacket = randomIntBetween(random, 1, 1500);
    int maxIpPacket = randomIntBetween(random, minIpPacket, 65535);

    record.put("SHORTEST_FLOW_PKT", shortestFlowPacket);
    record.put("LONGEST_FLOW_PKT", longestFlowPacket);

    record.put("MIN_IP_PKT_LEN", minIpPacket);
    record.put("MAX_IP_PKT_LEN", maxIpPacket);

    // Throughput
    record.put("SRC_TO_DST_SECOND_BYTES", randomNonNegativeLong(random, 1_000_000));
    record.put("DST_TO_SRC_SECOND_BYTES", randomNonNegativeLong(random, 1_000_000));

    record.put("RETRANSMITTED_IN_BYTES", randomNonNegativeLong(random, 10_000));
    record.put("RETRANSMITTED_IN_PKTS", randomNonNegativeLong(random, 1_000));
    record.put("RETRANSMITTED_OUT_BYTES", randomNonNegativeLong(random, 10_000));
    record.put("RETRANSMITTED_OUT_PKTS", randomNonNegativeLong(random, 1_000));

    record.put("SRC_TO_DST_AVG_THROUGHPUT", randomNonNegativeLong(random, 10_000_000));
    record.put("DST_TO_SRC_AVG_THROUGHPUT", randomNonNegativeLong(random, 10_000_000));

    // Packet-size buckets
    record.put("NUM_PKTS_UP_TO_128_BYTES", randomNonNegativeLong(random, 1_000));
    record.put("NUM_PKTS_128_TO_256_BYTES", randomNonNegativeLong(random, 1_000));
    record.put("NUM_PKTS_256_TO_512_BYTES", randomNonNegativeLong(random, 1_000));
    record.put("NUM_PKTS_512_TO_1024_BYTES", randomNonNegativeLong(random, 1_000));
    record.put("NUM_PKTS_1024_TO_1514_BYTES", randomNonNegativeLong(random, 1_000));

    // TCP windows
    record.put("TCP_WIN_MAX_IN", random.nextInt(65536));
    record.put("TCP_WIN_MAX_OUT", random.nextInt(65536));

    // ICMP
    record.put("ICMP_TYPE", random.nextInt(65536));
    record.put("ICMP_IPV4_TYPE", random.nextInt(256));

    // DNS
    record.put("DNS_QUERY_ID", random.nextInt(65536));
    record.put("DNS_QUERY_TYPE", random.nextInt(65536));
    record.put("DNS_TTL_ANSWER", randomNonNegativeLong(random, 100_000));

    // FTP
    record.put("FTP_COMMAND_RET_CODE", random.nextInt(100, 600));
  }

  private static void generateRandom(GenericRecord record, Random random) {

    // Keep IP format valid, but other values are only
    // plausibly random and are not guaranteed to satisfy
    // all validation rules.

    record.put("L4_SRC_PORT", random.nextInt());
    record.put("L4_DST_PORT", random.nextInt());

    record.put("PROTOCOL", random.nextInt());
    record.put("L7_PROTO", random.nextDouble(-100, 500));

    record.put("IN_BYTES", random.nextLong());
    record.put("OUT_BYTES", random.nextLong());

    record.put("IN_PKTS", random.nextLong());
    record.put("OUT_PKTS", random.nextLong());

    record.put("FLOW_DURATION_MILLISECONDS", random.nextLong());
    record.put("DURATION_IN", random.nextLong());
    record.put("DURATION_OUT", random.nextLong());

    record.put("TCP_FLAGS", random.nextInt());
    record.put("CLIENT_TCP_FLAGS", random.nextInt());
    record.put("SERVER_TCP_FLAGS", random.nextInt());

    record.put("MIN_TTL", random.nextInt(512) - 128);
    record.put("MAX_TTL", random.nextInt(512) - 128);

    record.put("LONGEST_FLOW_PKT", random.nextInt(100_000));
    record.put("SHORTEST_FLOW_PKT", random.nextInt(100_000));

    record.put("MIN_IP_PKT_LEN", random.nextInt(100_000));
    record.put("MAX_IP_PKT_LEN", random.nextInt(100_000));

    record.put("SRC_TO_DST_SECOND_BYTES", random.nextLong());
    record.put("DST_TO_SRC_SECOND_BYTES", random.nextLong());

    record.put("RETRANSMITTED_IN_BYTES", random.nextLong());
    record.put("RETRANSMITTED_IN_PKTS", random.nextLong());
    record.put("RETRANSMITTED_OUT_BYTES", random.nextLong());
    record.put("RETRANSMITTED_OUT_PKTS", random.nextLong());

    record.put("SRC_TO_DST_AVG_THROUGHPUT", random.nextLong());
    record.put("DST_TO_SRC_AVG_THROUGHPUT", random.nextLong());

    record.put("NUM_PKTS_UP_TO_128_BYTES", random.nextLong());
    record.put("NUM_PKTS_128_TO_256_BYTES", random.nextLong());
    record.put("NUM_PKTS_256_TO_512_BYTES", random.nextLong());
    record.put("NUM_PKTS_512_TO_1024_BYTES", random.nextLong());
    record.put("NUM_PKTS_1024_TO_1514_BYTES", random.nextLong());

    record.put("TCP_WIN_MAX_IN", random.nextInt());
    record.put("TCP_WIN_MAX_OUT", random.nextInt());

    record.put("ICMP_TYPE", random.nextInt());
    record.put("ICMP_IPV4_TYPE", random.nextInt());

    record.put("DNS_QUERY_ID", random.nextInt());
    record.put("DNS_QUERY_TYPE", random.nextInt());

    record.put("DNS_TTL_ANSWER", random.nextLong());
    record.put("FTP_COMMAND_RET_CODE", random.nextInt());
  }

  private static String randomIp(Random random) {
    return random.nextInt(256)
        + "."
        + random.nextInt(256)
        + "."
        + random.nextInt(256)
        + "."
        + random.nextInt(256);
  }

  private static long randomNonNegativeLong(Random random, long bound) {
    return random.nextLong(bound);
  }

  private static int randomIntBetween(Random random, int min, int max) {
    return min + random.nextInt(max - min + 1);
  }
}
