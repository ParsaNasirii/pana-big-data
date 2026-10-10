# Jenkins pipelines

The repository supports two independent CI entry points:

- **GitHub Actions** runs the complete Maven verification on pushes and pull
  requests. It does not publish images or deploy a cluster.
- **Jenkins** can select applications, verify them, publish images with Jib,
  deploy the platform, and run an on-demand archive restore.

No Jenkins instance is provisioned by this repository. Configure the jobs and
their environment before using them; adding these files to GitHub does not
start a Jenkins deployment.

| File | Responsibility |
| --- | --- |
| [Jenkinsfile.ci](Jenkinsfile.ci) | Select applications → build dependencies → verify → optional SonarQube → optional image publication |
| [Jenkinsfile.cd](Jenkinsfile.cd) | Select targets → validate inputs → prepare infrastructure → deploy applications |
| [Jenkinsfile.jobs](Jenkinsfile.jobs) | Check for conflicting workloads → run Backfill → collect logs and check completion |
| [detectApplications.groovy](detectApplications.groovy) | Shared application, Helm, and Ansible change detection |
| [maven-settings.xml](maven-settings.xml) | Public Maven repositories; used as both user and global settings |

## 1. Prepare Jenkins and its agent

Use a supported Jenkins installation with these plugins and their dependencies:

- Pipeline (including Declarative), Git, Credentials Binding, JUnit, and
  Workspace Cleanup.
- Docker Pipeline and Lockable Resources for CI.
- Pipeline Utility Steps for deployment and job JSON/log handling.
- SSH Credentials when supplying an Ansible private key through Jenkins.
- SonarQube Scanner for Jenkins for the CI file's optional analysis stage.

Set up a **Linux agent labelled `docker`**, or change `AGENT_LABEL` to your
agent's label. CI needs Git, the Docker CLI, access to `/var/run/docker.sock`,
and public artifact registries. Maven runs in the public
`maven:3.9.9-eclipse-temurin-17` image. The agent user's Maven and Sonar caches
are mounted into that container. The workspace and socket must be accessible
to the same local Docker daemon; a remote Docker daemon is not configured here.

CD and Jobs execute tools directly on the agent: install **Helm 3**, `kubectl`,
and Git. For Ansible targets, also prepare Python, Ansible, PyMySQL and the
collections from the [Ansible guide](../ansible/README.md). Ensure that Python
environment's executables are on the agent's `PATH`, including its `pip` next
to the Python interpreter used by Ansible. Password-based SSH needs
`sshpass`.

Use trusted jobs and agents for image publication and deployment credentials.
The provided GitHub Actions workflow remains the credential-free verification
path for pull requests.

## 2. Create three Pipeline jobs

In Jenkins, create a **Pipeline** job, choose **Pipeline script from SCM**,
select Git, and use:

```text
Repository URL: https://github.com/ParsaNasirii/pana-big-data.git
Branch:         */main
```

Create the following jobs with separate script paths:

| Suggested job name | Script path |
| --- | --- |
| `pana-ci` | `deploy/pipeline/Jenkinsfile.ci` |
| `pana-cd` | `deploy/pipeline/Jenkinsfile.cd` |
| `pana-jobs` | `deploy/pipeline/Jenkinsfile.jobs` |

Each file declares its own parameters. The first run registers them in Jenkins;
subsequent runs expose **Build with Parameters**. CI defaults to verification,
with image publication and Sonar disabled. CD target modes default to `NEVER`,
and Jobs defaults to `STARROCKS_BACKFILL_RUN=false`, so their initial runs do
not deploy or restore anything. No schedules or automatic job chaining are
declared. Configure a CI webhook separately if wanted.

To use another branch, change the job's SCM configuration. Image tags default
to the **full SHA of the checked-out commit**. There is no implicit `latest`
tag and no dependency on a code-review server.

## 3. Verify and publish applications

CI supports `PARQUET_WRITER_MODE`, `PARQUET_MAINTENANCE_MODE`,
`STARROCKS_WRITER_MODE`, `STARROCKS_BACKFILL_MODE`, `CUBE_MAKER_MODE`, and
`API_SERVICE_MODE`:

| Mode | Behavior |
| --- | --- |
| `IF_CHANGED` | Select tests and image publication for affected applications; publication still requires `PUSH_IMAGES=true` |
| `NEVER` | Skip this application |
| `TEST_ONLY` | Build and verify, without selecting its image for publication |
| `PUSH_ONLY` | Build and select its image; skip this application's tests |
| `TEST_AND_PUSH` | Build, verify, and select its image regardless of change detection |

All selected applications and required dependencies are installed first. When
testing an application, CI also verifies the shared modules. StarRocks Writer,
the shared Kafka consumer service, and Backfill tests run serially. Remaining
modules use at most `PARALLEL_TEST_WORKERS` workers (default `3`; set `1` for
a smaller agent). Per-project `verify` runs tests, formatting checks, and
JaCoCo reporting; JUnit results are published in Jenkins.

To publish an image:

1. Create a Jenkins **Username with password** credential, for example
   `container-registry`. For GHCR, use your registry username and a token
   permitted to publish the package as the password.
2. Set `IMAGE_REGISTRY` to your registry/repository prefix. Its default is
   `ghcr.io/parsanasirii/pana-big-data`; change this for your account.
3. Set `REGISTRY_CREDENTIALS_ID`, enable `PUSH_IMAGES`, and choose
   `TEST_AND_PUSH` for each application you want to publish.
4. After success, use the printed commit SHA as the deployment image tag.

The resulting reference is `IMAGE_REGISTRY/<application>:<commit-sha>`.
The image stage runs only when the preceding build remains successful. Prefer
`TEST_AND_PUSH` for a release; `PUSH_ONLY` deliberately does not establish a
fresh test result. Publishing an image does not make a private package public
or configure cluster pull credentials.

### Optional SonarQube

Configure a SonarQube server and token in Jenkins, then set `RUN_SONAR=true`,
`SONAR_INSTALLATION`, `SONAR_PROJECT_KEY`, and `SONAR_PROJECT_NAME`. Configure
the server's webhook to `https://YOUR_JENKINS/sonarqube-webhook/` so
`waitForQualityGate` can resume. Analysis covers the selected Maven projects
and their shared dependencies; pull-request decoration is not configured.
A non-OK gate marks the build unstable and prevents image publication.

## How change detection works

CI and CD compare the checkout with `BASE_COMMIT` when supplied, otherwise
with their own job's `GIT_PREVIOUS_SUCCESSFUL_COMMIT`. This includes changes
across multiple commits. On a first build or when the base is unavailable,
the detector considers all tracked files. Renames include both paths.

Application/shared-code changes map to the applications that use them;
Helm and Ansible changes map to their infrastructure targets. Parent build or
pipeline changes select all applications. These are path rules, not a Maven
dependency graph resolver.

A previous successful Jenkins build is **not** a record of which targets were
actually published or deployed. For a manual rerun at the same commit, choose
CI `TEST_AND_PUSH` or CD `ALWAYS`; `IF_CHANGED` can correctly select nothing.
You can also set `BASE_COMMIT` explicitly for an intended comparison range.

## 4. Configure a deployment environment

The charts and inventory contain example addresses. Follow the
[Helm guide](../helm/README.md) and [Ansible guide](../ansible/README.md) to
configure real endpoints, storage, database users, and existing Secrets.

### Kubernetes and Helm

Store your target kubeconfig as a Jenkins **Secret file** credential. Set
`KUBECONFIG_CREDENTIALS_ID` (default `kubernetes-config`) and `NAMESPACE`
(default `panabigdata`). All cluster operations use that bound kubeconfig.
CD also creates/applies the namespace, so its credential needs permission for
that operation in addition to the resources managed by the selected charts.

Create an absolute directory on the trusted deployment agent, such as
`/opt/pana/values`, and set `HELM_VALUES_DIR` to it. Put a configured values
file there for **each selected release**:

```text
/opt/pana/values/
  hdfs-client.yaml
  postgres.yaml
  schema-registry.yaml
  spark.yaml
  monitoring.yaml
  parquet-writer.yaml
  parquet-maintenance.yaml
  starrocks-writer.yaml
  cube-maker.yaml
  api-service.yaml
  starrocks-backfill.yaml
```

Use Secret names in these files; create the actual Secrets in the namespace
before deploying. A file may contain `{}` only when the chart's other inputs
are already configured. CD renders all selected charts before changing the
cluster, which catches missing required values. Rendering does not prove that
Secrets, endpoints, storage or images exist.

Values override chart defaults. Application `image.repository` and `image.tag`
are then overridden by Jenkins' `IMAGE_REGISTRY/<application>` and `IMAGE_TAG`.
Blank `IMAGE_TAG` uses the checkout SHA; publish that image first, or supply
an existing immutable tag. Infrastructure image versions come from chart
defaults or your values files.

Some configuration is packaged as **chart files**, not values: HDFS XML files
and Prometheus scrape configuration must be customized in the deployment
checkout/branch as described in the Helm guide. `HELM_VALUES_DIR` cannot
override these files by itself. Keep private deployment configuration out of
this public repository.

### Ansible

When selecting Kafka or StarRocks, supply:

| Parameter | Input |
| --- | --- |
| `ANSIBLE_INVENTORY` | Absolute agent path to your inventory file; copy and configure the full inventory tree, including `group_vars` |
| `ANSIBLE_VAULT_FILE` | Absolute agent path to an encrypted YAML vars file, based on `vault.example.yml` |
| `ANSIBLE_VAULT_PASSWORD_CREDENTIALS_ID` | Jenkins Secret file containing the Vault decryption password |
| `KAFKA_CLUSTER_ID` | Persistent cluster UUID when deploying Kafka, unless `cluster_id` is already set in your inventory |
| `ANSIBLE_SSH_CREDENTIALS_ID` | Optional SSH username/private-key credential; blank uses the agent's existing SSH configuration |
| `ANSIBLE_SSH_PASSWORD_CREDENTIALS_ID` | Optional Secret text exposed as `SSH_PASS` for the inventory's password lookup |

The supplied private key must work noninteractively; this pipeline does not
provide its passphrase to SSH. Its username and key path override the inventory
connection settings. With password authentication, set the remote username in
your inventory. Put any required sudo password in the encrypted vars file.
Both Ansible targets require the Vault input; Kafka can share the configured
environment Vault with StarRocks. The playbook receives it as extra vars.

## 5. Deploy selected targets

CD has `*_MODE` parameters with `NEVER`, `IF_CHANGED`, and `ALWAYS` choices.
Application targets are API Service, both Writers, Parquet Maintenance and
Cube Maker. Infrastructure targets are HDFS Client, PostgreSQL, Schema
Registry, Spark, Monitoring, Kafka and StarRocks. **Backfill is in Jobs**,
because deploying the application set must not automatically restore data.

For a manual deployment, select only the targets you intend to change and use
`ALWAYS`. Selected infrastructure operations run in parallel; applications
start deploying after that stage succeeds. Each selected Ansible subsystem
gets at most two attempts. Helm uses `--atomic --wait` for ordinary CD
releases. This rollback concerns Helm resources, not database transactions.

For a first installation, prepare external HDFS/Kubernetes and database
credentials, then follow the guides' host → cluster services → applications
order. The selector does not automatically enable prerequisite targets or
create Secrets. Configure pull access separately for private application images.
Most application charts rely on the namespace's default ServiceAccount for
image pull credentials; configure it with `imagePullSecrets`, or publish images
that the cluster can pull anonymously.

## 6. Run Backfill explicitly

1. Publish a tested Backfill image and configure `starrocks-backfill.yaml`,
   including its database endpoint and `credentials.existingSecret`.
2. Ensure the requested HDFS archive is complete and stable. Stop
   `starrocks-writer` for the **entire** restore; do not redeploy it from CD
   until the restore finishes.
3. In `pana-jobs`, set the cluster/values parameters, enable
   `STARROCKS_BACKFILL_RUN`, and supply:

   ```text
   STARROCKS_BACKFILL_IMAGE_TAG=<published-commit-sha>
   STARROCKS_BACKFILL_START=2026-01-01T10:00:00Z
   STARROCKS_BACKFILL_END=2026-01-01T11:00:00Z
   ```

   Use a window that exists in your archive. The start is inclusive and the
   end is exclusive. These Jenkins values override the environment values.
4. Inspect the Jenkins result and archived `*.log` files, then resume the Writer.

The job refuses to start when an earlier restore is unfinished or matching
Writer/Backfill pods are still active, including Pending pods. It rechecks
immediately before starting. Writer detection uses the fixed Helm release label
`app.kubernetes.io/instance=starrocks-writer`; a differently named Writer release
must be stopped explicitly. This is a preflight check, not a cross-job lock:
operators must keep the Writer stopped and avoid other restore launchers.

Helm waits for the Backfill hook, with a 50-minute timeout. The pipeline checks
Job completion, saves logs, and stops an unfinished active Job on failure.
Backfill deliberately does **not** use Helm's automatic rollback: rollback
cannot undo StarRocks writes and may rerun an older restore hook. The archive
completeness and schema limits in the root README still apply.

## Verification and references

The three Jenkinsfiles have been validated with Jenkins' Declarative Pipeline
linter. The change detector has also been exercised with local Git histories,
including first-build and multi-commit cases. These checks do not replace an
image publication or deployment test against your configured Jenkins/cluster.
GitHub Actions continues to verify the application reactor separately.

- [Jenkins Pipeline syntax](https://www.jenkins.io/doc/book/pipeline/syntax/)
- [Docker with Pipeline](https://www.jenkins.io/doc/book/pipeline/docker/)
- [Jenkins credentials](https://www.jenkins.io/doc/book/using/using-credentials/)
- [Pipeline linter](https://www.jenkins.io/doc/book/pipeline/development/#linter)
