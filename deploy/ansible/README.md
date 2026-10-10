# Ansible deployment

This playbook installs Kafka 3.7.2 in KRaft mode and StarRocks 4.0.12 on four
Ubuntu x86-64 hosts. Each host runs a Kafka broker/controller, a StarRocks FE,
and a StarRocks BE. Java is Eclipse Temurin 17.0.6+10. The playbook also installs
Prometheus JMX exporter 0.17.2 and initializes the `panabigdata.netflow_raw`
schema from `apps/starrocks-writer/init.sql`.

The inventory contains documentation addresses (`192.0.2.11` through
`192.0.2.14`). Replace them with your own reachable hosts before deployment.
Kafka quorum addresses and StarRocks priority networks derive from the inventory.

## Controller setup

Run these commands from `deploy/ansible` so Ansible loads the local configuration
and role paths:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --index-url https://pypi.org/simple ansible PyMySQL==1.2.0
ansible-galaxy collection install -r requirements.yml
```

The controller needs Python with `venv`/`pip`, SSH access to all four hosts, and
SQL access to their StarRocks query ports (`9030`). The playbook installs PyMySQL
into the same Python environment as Ansible. Password-based SSH additionally
requires `sshpass`; SSH keys can be used instead.

Hosts need Python 3, `sudo`, `tar`, a systemd-based Ubuntu installation compatible
with the StarRocks Ubuntu package, and enough disk and memory for Kafka and
StarRocks together. Review the upstream
[StarRocks deployment prerequisites](https://docs.starrocks.io/docs/deployment/preparation/deployment_prerequisites/)
and [environment configuration](https://docs.starrocks.io/docs/deployment/preparation/environment_configurations/).
The roles do not provision hosts, partition disks, or configure firewalls.

## Deployment inputs

1. Edit `inventory/hosts.yml` with the four host addresses.
2. Set `ansible_user` in `inventory/group_vars/all/vars.yml` to your SSH account
   (the example account is `netflow`). Configure your SSH key or set `SSH_PASS`.
3. Review `kafka_data_dir` and `starrocks_data_root` in their group variables;
   their defaults use `/mnt/disk1`. Review FE heap and BE memory settings for
   the resources available on each host.
4. Create and encrypt a local vault containing your own StarRocks password:

   ```bash
   cp vault.example.yml inventory/group_vars/all/vault.yml
   chmod 600 inventory/group_vars/all/vault.yml
   ${EDITOR:-vi} inventory/group_vars/all/vault.yml
   ansible-vault encrypt inventory/group_vars/all/vault.yml
   ```

   Replace the placeholder before encryption. Add `ansible_become_password` if
   sudo requires a password. Real `vault.yml` files are excluded from version
   control. Configure application clients with the same StarRocks credentials.
5. Generate one Kafka cluster ID for a new cluster:

   ```bash
   export KAFKA_CLUSTER_ID="$(python3 -c 'import base64, uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')"
   ```

   Save this value and reuse it on later runs and when adding hosts. You can
   also set `cluster_id` directly in the Kafka group variables. An existing
   cluster must retain the ID recorded in its `meta.properties` files.

The controller fetches JMX exporter from Maven Central and Python packages from
PyPI. Each target host downloads Java, Kafka, and StarRocks from their public
upstreams and therefore needs outbound HTTPS access. Override the download URL
variables and matching checksums if you maintain your own artifact mirror.

## Validate and run

```bash
ansible-inventory -i inventory/hosts.yml --graph
ansible-playbook -i inventory/hosts.yml playbooks/main.yml --syntax-check --ask-vault-pass
ansible-playbook -i inventory/hosts.yml playbooks/main.yml --ask-vault-pass
```

Use `--tags kafka` or `--tags starrocks` to deploy one subsystem. Syntax checking
does not contact hosts. This playbook contains SQL health checks, so Ansible
`--check` is not a substitute for a deployment test on a prepared cluster.

The default cluster uses Kafka PLAINTEXT listeners and exposes metrics without
authentication. SSH host-key checking is disabled in the supplied lab
configuration. These settings are intended for a private lab network; configure
the access controls appropriate to your environment before exposing services.

## Public artifact sources

| Component | Version | Upstream |
| --- | --- | --- |
| Kafka (Scala 2.13) | 3.7.2 | [Apache archive](https://archive.apache.org/dist/kafka/3.7.2/) |
| Java | 17.0.6+10 | [Eclipse Temurin release](https://github.com/adoptium/temurin17-binaries/releases/tag/jdk-17.0.6%2B10) |
| StarRocks Ubuntu AMD64 | 4.0.12 | [Official distribution](https://releases.starrocks.io/starrocks/StarRocks-4.0.12-ubuntu-amd64.tar.gz) |
| Prometheus JMX exporter | 0.17.2 | [Maven Central](https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/) |
| PyMySQL | 1.2.0 | [PyPI](https://pypi.org/project/PyMySQL/1.2.0/) |

The existing pinned collection versions are listed in `requirements.yml`.
