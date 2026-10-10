import groovy.transform.Field

@Field def applicationKeywords = [
        'parquet-writer': [
                'hdfs-client',
                'parquet-writer',
                'kafka',
                'apps/shared/',

                'deploy/ansible/inventory/hosts.yml',
                'deploy/ansible/inventory/group_vars/all/vars.yml',
                'deploy/ansible/playbooks/main.yml',
                'deploy/ansible/ansible.cfg',
                'deploy/ansible/requirements.yml'
        ],
        'parquet-maintenance': [
                'hdfs-client',
                'parquet',
                'spark',
                'apps/shared/config-support/',
                'apps/shared/hdfs-support/',
                'apps/shared/pom.xml'
        ],
        'starrocks-writer': [
                'starrocks-writer',
                'deploy/ansible/roles/starrocks',
                'deploy/ansible/inventory/group_vars/starrocks/',
                'kafka',
                'apps/shared/',

                'deploy/ansible/inventory/hosts.yml',
                'deploy/ansible/inventory/group_vars/all/vars.yml',
                'deploy/ansible/playbooks/main.yml',
                'deploy/ansible/ansible.cfg',
                'deploy/ansible/requirements.yml'
        ],
        'starrocks-backfill': [
                'starrocks-backfill',
                'deploy/helm/infrastructure/hdfs-client/',
                'apps/shared/config-support/',
                'apps/shared/hdfs-support/',
                'apps/shared/pom.xml'
        ],
        'cube-maker': [
                'spark',
                'cube-maker',
                'postgres',
                'apps/shared/config-support/',
                'apps/shared/hdfs-support/',
                'apps/shared/pom.xml'
        ],
        'api-service': [
                'spring',
                'api-service',
                'apps/shared/config-support/',
                'apps/shared/pom.xml',
                'postgres'
        ]
]

@Field def allApplicationsPaths = [
        'pom.xml',
        'deploy/pipeline/Jenkinsfile.ci',
        'deploy/pipeline/Jenkinsfile.cd',
        'deploy/pipeline/Jenkinsfile.jobs',
        'deploy/pipeline/detectApplications.groovy',
        'deploy/pipeline/maven-settings.xml'
]

@Field def allAnsibleInfrastructurePaths = [
        'deploy/ansible/ansible.cfg',
        'deploy/ansible/controller-requirements.txt',
        'deploy/ansible/inventory/hosts.yml',
        'deploy/ansible/inventory/group_vars/all/',
        'deploy/ansible/playbooks/main.yml',
        'deploy/ansible/requirements.yml'
]

// The first build and shallow checkouts without the base select all tracked files.
def getChangedFiles(String baseCommit) {
    return withEnv(["CHANGE_BASE=${baseCommit?.trim() ?: ''}"]) {
        sh(
            script: '''
                set -eu
                if [ -n "$CHANGE_BASE" ] && \
                   base=$(git rev-parse --verify --quiet --end-of-options "${CHANGE_BASE}^{commit}"); then
                    git diff --name-only --no-renames "$base" HEAD --
                else
                    git ls-files
                fi
            ''',
            returnStdout: true
        ).readLines()
    }
}

def selectChangedApplications(List applications, List changedFiles) {

    if (changedFiles.any { changedFile ->
        allApplicationsPaths.any { path ->
            changedFile.startsWith(path)
        }
    }) {
        return applications
    }

    return applications.findAll { appName ->
        applicationKeywords[appName]?.any { path ->
            changedFiles.any { changedFile ->
                changedFile.contains(path)
            }
        }
    }
}

def selectChangedHelmInfrastructure(Map infrastructureCharts, List changedFiles) {
    return infrastructureCharts.findAll { infrastructureName, chartPath ->
        changedFiles.any { changedFile ->
            changedFile.startsWith("${chartPath}/")
        }
    }.keySet() as List
}

def selectChangedAnsibleInfrastructure(List infrastructure, List changedFiles) {
    return infrastructure.findAll { infrastructureName ->
        changedFiles.any { changedFile ->
            allAnsibleInfrastructurePaths.any { path ->
                changedFile.startsWith(path)
            } || changedFile.startsWith(
                "deploy/ansible/roles/${infrastructureName}"
            ) || changedFile.startsWith(
                "deploy/ansible/inventory/group_vars/${infrastructureName}/"
            ) || (infrastructureName == 'starrocks' && changedFile == 'apps/starrocks-writer/init.sql')
        }
    }
}

return this
