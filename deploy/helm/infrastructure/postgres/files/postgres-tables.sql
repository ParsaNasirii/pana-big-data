-- The database is created by the PostgreSQL Helm chart before this schema is applied.

CREATE TABLE IF NOT EXISTS batch_tracking
(
    path_name    VARCHAR PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- cube tables

CREATE TABLE IF NOT EXISTS agg_ip_traffic
(
    kafka_timestamp_bucket TIMESTAMPTZ NOT NULL,
    ip_address          INET        NOT NULL,
    total_in_bytes      BIGINT DEFAULT 0,
    total_out_bytes     BIGINT DEFAULT 0,
    total_bytes         BIGINT DEFAULT 0,
    total_in_pkts       BIGINT DEFAULT 0,
    total_out_pkts      BIGINT DEFAULT 0,
    total_pkts          BIGINT DEFAULT 0,

    PRIMARY KEY (kafka_timestamp_bucket, ip_address)
) PARTITION BY RANGE (kafka_timestamp_bucket);

CREATE TABLE IF NOT EXISTS agg_ip_traffic_202608
    PARTITION OF agg_ip_traffic
        FOR VALUES FROM ('2026-08-01 00:00:00+00')
        TO   ('2026-09-01 00:00:00+00');

CREATE INDEX IF NOT EXISTS idx_ip_traffic_ip_bytes ON agg_ip_traffic (ip_address, total_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_ip_traffic_total_bytes ON agg_ip_traffic (total_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_ip_traffic_ip_pkts ON agg_ip_traffic (ip_address, total_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_ip_traffic_total_pkts ON agg_ip_traffic (total_bytes DESC);


CREATE TABLE IF NOT EXISTS agg_ip_to_ip_connection
(
    kafka_timestamp_bucket TIMESTAMPTZ NOT NULL,
    src_ip              INET        NOT NULL,
    dst_ip              INET        NOT NULL,
    total_flows         BIGINT DEFAULT 0,
    total_bytes         BIGINT DEFAULT 0,

    PRIMARY KEY (kafka_timestamp_bucket, src_ip, dst_ip)
) PARTITION BY RANGE (kafka_timestamp_bucket);

CREATE TABLE IF NOT EXISTS agg_ip_to_ip_connection_202608
    PARTITION OF agg_ip_to_ip_connection
        FOR VALUES FROM ('2026-08-01 00:00:00+00')
        TO   ('2026-09-01 00:00:00+00');

CREATE INDEX IF NOT EXISTS idx_ip_connections_src ON agg_ip_to_ip_connection (src_ip, kafka_timestamp_bucket);
CREATE INDEX IF NOT EXISTS idx_ip_connections_dst ON agg_ip_to_ip_connection (dst_ip, kafka_timestamp_bucket);


CREATE TABLE IF NOT EXISTS agg_protocol_usage
(
    kafka_timestamp_bucket TIMESTAMPTZ NOT NULL,
    protocol            INT         NOT NULL,
    total_bytes         BIGINT DEFAULT 0,
    total_flows         BIGINT DEFAULT 0,

    PRIMARY KEY (kafka_timestamp_bucket, protocol)
) PARTITION BY RANGE (kafka_timestamp_bucket);

CREATE TABLE IF NOT EXISTS agg_protocol_usage_202608
    PARTITION OF agg_protocol_usage
        FOR VALUES FROM ('2026-08-01 00:00:00+00')
        TO   ('2026-09-01 00:00:00+00');

CREATE INDEX IF NOT EXISTS idx_protocol_usage_bytes ON agg_protocol_usage (total_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_protocol_usage_flows ON agg_protocol_usage (total_flows DESC);

-- stage tables

CREATE TABLE IF NOT EXISTS agg_ip_traffic_staging
(
    kafka_timestamp_bucket TIMESTAMPTZ NOT NULL,
    ip_address          INET        NOT NULL,
    total_in_bytes      BIGINT DEFAULT 0,
    total_out_bytes     BIGINT DEFAULT 0,
    total_bytes         BIGINT DEFAULT 0,
    total_in_pkts       BIGINT DEFAULT 0,
    total_out_pkts      BIGINT DEFAULT 0,
    total_pkts          BIGINT DEFAULT 0,

    PRIMARY KEY (kafka_timestamp_bucket, ip_address)
);

CREATE TABLE IF NOT EXISTS agg_ip_to_ip_connection_staging
(
    kafka_timestamp_bucket TIMESTAMPTZ NOT NULL,
    src_ip              INET        NOT NULL,
    dst_ip              INET        NOT NULL,
    total_flows         BIGINT DEFAULT 0,
    total_bytes         BIGINT DEFAULT 0,

    PRIMARY KEY (kafka_timestamp_bucket, src_ip, dst_ip)
);

CREATE TABLE IF NOT EXISTS agg_protocol_usage_staging
(
    kafka_timestamp_bucket TIMESTAMPTZ NOT NULL,
    protocol            INT         NOT NULL,
    total_bytes         BIGINT DEFAULT 0,
    total_flows         BIGINT DEFAULT 0,

    PRIMARY KEY (kafka_timestamp_bucket, protocol)
);
