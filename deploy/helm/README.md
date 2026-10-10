# Helm deployment

These charts preserve the distributed deployment: external Kafka, HDFS, and
StarRocks; Kubernetes-hosted Schema Registry, Spark, PostgreSQL, applications,
and monitoring. They are examples to configure for your own cluster.

Build and push the application images as described in the repository README.
For every application chart, set `image.repository` to your published image and
`image.tag` to its version. The `ghcr.io/your-github-user/pana-big-data/*` values
are placeholders, not published images. `starrocks-writer` and
`starrocks-backfill` require an explicit image tag.

## Environment configuration

- Replace the documentation addresses `192.0.2.11`–`192.0.2.14` with your Kafka,
  HDFS, and StarRocks endpoints. They are not reachable deployment defaults.
- Configure `infrastructure/hdfs-client/files/core-site.xml` and `hdfs-site.xml`
  for your HDFS nameservice. The sample nameservice is `netflow` and the Hadoop
  user is `netflow`. Keep the corresponding Spark event-log paths and
  `apps/cube-maker/values.yaml` Hadoop settings consistent.
- Update Kafka bootstrap servers in both writer charts and Schema Registry.
  The Parquet writer also has `env.KAFKA_BOOTSTRAP_SERVERS` to update.
- Set StarRocks endpoints/database in the writer, backfill, API, and Grafana
  datasource values. Apply the repository's StarRocks schema before ingestion.
- Adjust `infrastructure/monitoring-stack/prometheus/prometheus.yml` scrape
  targets to your exporters and nodes.
- Blank storage class values use your cluster's default StorageClass. Set
  `storage.storageClass` for PostgreSQL and each component's
  `storage.storageClass` in the monitoring chart when a particular class is
  needed. Provision suitable persistent storage before installing.
- The charts use LoadBalancer services for several UIs. Configure service
  types and access according to your cluster. Keep one namespace for the
  supplied service names, or override references to match your deployment.
- The example time zone is `Asia/Tehran`; change the application, database
  session, Spark, and scheduler time-zone settings together if needed.

## Required Secrets

Create each Secret in the release namespace before installing its chart.
Supply the Secret name through your local values file or `--set-string`.
The charts do not generate or store passwords. Do not put credentials in the
charts' `env`, `config`, or ConfigMap values.

| Chart | Required value | Required Secret keys |
| --- | --- | --- |
| `infrastructure/postgres` | `postgres.existingSecret` | `username`, `password`, `replication-password` |
| `apps/cube-maker` | `existingSecret` | `POSTGRES_USERNAME`, `POSTGRES_PASSWORD` |
| `apps/api-service` | `existingSecret` | `SPRING_DATASOURCE_POSTGRES_USERNAME`, `SPRING_DATASOURCE_POSTGRES_PASSWORD`, `SPRING_DATASOURCE_STARROCKS_USERNAME`, `SPRING_DATASOURCE_STARROCKS_PASSWORD` |
| `apps/starrocks-writer` | `credentials.existingSecret` | `STARROCKS_USERNAME`, `STARROCKS_PASSWORD` |
| `apps/starrocks-backfill` | `credentials.existingSecret` | `STARROCKS_USERNAME`, `STARROCKS_PASSWORD` |
| `infrastructure/monitoring-stack` | `grafana.existingSecret` | `admin-user`, `admin-password`, `STARROCKS_USERNAME`, `STARROCKS_PASSWORD`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD` |

Choose your own credentials. The application and Grafana database credentials
must identify users that exist in the corresponding databases and have the
necessary permissions. The PostgreSQL chart's `username` identifies the Spilo
superuser; `replication-password` belongs to its `replicator` account. Secret
names may be shared where their key contracts match.

Grafana reads datasource credentials from Secret-backed environment variables;
its provisioning ConfigMap contains variable references only. Its configured
PostgreSQL SSL mode defaults to `require` and can be changed through
`grafana.datasources.postgres.sslmode`. Grafana documents the
[environment-variable provisioning syntax](https://grafana.com/docs/grafana/latest/administration/provisioning/).

## Installation order

With the external services and Secrets ready, install `hdfs-client`,
`schema-registry`, `postgres`, `spark-cluster`, and `monitoring-stack`, then the
application charts. Use release names `schema-registry`, `postgres`, `spark`,
and `monitoring` to match the supplied application service references. Check
each chart's values before installation.

The `starrocks-backfill` chart is an on-demand Job. Supply `backfill.start` and
`backfill.end` along with its image tag and Secret. Stop `starrocks-writer`
until the restore finishes, as the backfill values describe.

## Local validation

From the repository root, lint and render every chart without connecting to a
cluster. These synthetic Secret names are used only to validate templates;
the commands do not create Secrets or deploy workloads.

```bash
for chart in deploy/helm/apps/* deploy/helm/infrastructure/*; do
  [ -f "$chart/Chart.yaml" ] || continue
  validation_args=(
    --set-string image.tag=validation-only
    --set-string existingSecret=validation-only
    --set-string credentials.existingSecret=validation-only
    --set-string postgres.existingSecret=validation-only
    --set-string grafana.existingSecret=validation-only
    --set-string backfill.start=2026-01-01T00:00:00Z
    --set-string backfill.end=2026-01-02T00:00:00Z
  )
  helm lint "$chart" "${validation_args[@]}" || exit 1
  helm template validation "$chart" "${validation_args[@]}" >/dev/null || exit 1
done
```

## Upstream images

Infrastructure images retain their pinned versions. The Schema Registry image
uses the public `confluentinc/cp-schema-registry` repository described in
[Confluent's image reference](https://docs.confluent.io/platform/current/installation/docker/image-reference.html).
Public tag metadata was checked on 2026-10-10:

| Image | Upstream tag |
| --- | --- |
| Schema Registry | [confluentinc/cp-schema-registry:8.0.2](https://hub.docker.com/v2/repositories/confluentinc/cp-schema-registry/tags/8.0.2) |
| Spark | [apache/spark:4.1.1](https://hub.docker.com/v2/repositories/apache/spark/tags/4.1.1) |
| PostgreSQL / Spilo | [ghcr.io/zalando/spilo-17:4.0-p2](https://github.com/zalando/spilo/pkgs/container/spilo-17) |
| Prometheus | [prom/prometheus:v3.5.0](https://hub.docker.com/v2/repositories/prom/prometheus/tags/v3.5.0) |
| Pushgateway | [prom/pushgateway:v1.11.3](https://hub.docker.com/v2/repositories/prom/pushgateway/tags/v1.11.3) |
| Grafana | [grafana/grafana:13.2.0](https://hub.docker.com/v2/repositories/grafana/grafana/tags/13.2.0) |
| Node exporter | [prom/node-exporter:v1.12.1](https://hub.docker.com/v2/repositories/prom/node-exporter/tags/v1.12.1) |

Template validation does not verify a live cluster, image startup, storage,
network access, or database permissions.
