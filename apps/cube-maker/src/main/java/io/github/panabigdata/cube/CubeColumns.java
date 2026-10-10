package io.github.panabigdata.cube;

public final class CubeColumns {
  private CubeColumns() {}

  public static final String KAFKA_TIMESTAMP_BUCKET = "kafka_timestamp_bucket";
  public static final String IP_ADDRESS = "ip_address";
  public static final String PROTOCOL = "protocol";
  public static final String DST_IP = "dst_ip";
  public static final String SRC_IP = "src_ip";
  public static final String IN_BYTES = "in_bytes";
  public static final String OUT_BYTES = "out_bytes";
  public static final String IN_PKTS = "in_pkts";
  public static final String OUT_PKTS = "out_pkts";
  public static final String TOTAL_IN_BYTES = "total_in_bytes";
  public static final String TOTAL_OUT_BYTES = "total_out_bytes";
  public static final String TOTAL_IN_PKTS = "total_in_pkts";
  public static final String TOTAL_OUT_PKTS = "total_out_pkts";
  public static final String TOTAL_FLOWS = "total_flows";
  public static final String TOTAL_BYTES = "total_bytes";
  public static final String TOTAL_PKTS = "total_pkts";
}
