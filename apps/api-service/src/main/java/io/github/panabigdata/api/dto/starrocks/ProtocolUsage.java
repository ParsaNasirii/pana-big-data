package io.github.panabigdata.api.dto.starrocks;

import lombok.Value;

@Value
public class ProtocolUsage {
  Integer protocol;
  String protocolName;
  Long usageCount;

  public ProtocolUsage(Integer protocol, Long usageCount) {
    this.protocol = protocol;
    this.protocolName = protocolName(protocol);
    this.usageCount = usageCount;
  }

  private static String protocolName(Integer protocol) {
    if (protocol == null) {
      return "UNKNOWN";
    }
    if (protocol >= 148 && protocol <= 252) {
      return "UNASSIGNED";
    }
    return switch (protocol) {
      case 0 -> "HOPOPT";
      case 1 -> "ICMP";
      case 2 -> "IGMP";
      case 4 -> "IPv4";
      case 6 -> "TCP";
      case 17 -> "UDP";
      case 33 -> "DCCP";
      case 35 -> "IDPR";
      case 41 -> "IPv6";
      case 43 -> "IPv6-Route";
      case 44 -> "IPv6-Frag";
      case 46 -> "RSVP";
      case 47 -> "GRE";
      case 50 -> "ESP";
      case 51 -> "AH";
      case 58 -> "IPv6-ICMP";
      case 59 -> "IPv6-NoNxt";
      case 60 -> "IPv6-Opts";
      case 88 -> "EIGRP";
      case 89 -> "OSPFIGP";
      case 94 -> "IPIP";
      case 103 -> "PIM";
      case 112 -> "VRRP";
      case 115 -> "L2TP";
      case 132 -> "SCTP";
      case 136 -> "UDPLite";
      case 137 -> "MPLS-in-IP";
      case 138 -> "MANET";
      case 139 -> "HIP";
      case 140 -> "Shim6";
      case 141 -> "WESP";
      case 142 -> "ROHC";
      case 143 -> "Ethernet";
      case 144 -> "AGGFRAG";
      case 145 -> "NSH";
      case 146 -> "Homa";
      case 147 -> "BIT-EMU";
      case 253, 254 -> "EXPERIMENTAL";
      case 255 -> "RESERVED";
      default -> "IP_PROTOCOL_" + protocol;
    };
  }
}
