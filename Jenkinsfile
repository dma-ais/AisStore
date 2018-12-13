pipeline {
    agent any

    tools {
        maven 'M3.3.9'
    }

    triggers {
        pollSCM('H H/4 * * *')
    }

    stages {
        stage('checkout') {
            steps {
                checkout scm
            }
        }

        stage('build') {
            steps {
                withMaven() {
                    sh 'mvn -e -DskipTests -DincludeSrcJavadocs clean source:jar install'
                }
            }
        }
    }
    post {
        success {
            archiveArtifacts artifacts: 'ais-store-cli/**/ais-store-cli-*.jar, ais-store-common/target/ais-store-common-*.jar, ais-store-web/target/ais-store-web-*.war, ais-store-rest/target/ais-store-rest-*.jar', fingerprint: true
            sh 'curl --data "build=true" -X POST https://registry.hub.docker.com/u/dmadk/ais-store-cli/trigger/a995ad1e-4a4c-11e4-a6f5-dafbef59e66b/'
            sh 'curl --data "build=true" -X POST https://registry.hub.docker.com/u/dmadk/ais-store-web/trigger/55519437-3b1c-44e8-bc89-e8ec4e5e4d1f/'
            sh 'curl --data "build=true" -X POST https://registry.hub.docker.com/u/dmadk/ais-store-rest/trigger/504d94da-49f6-4c48-918c-350e2e067e6a/'
        }
    }
//    failure {
//        // notify users when the Pipeline fails
//        mail to: 'steen@lundogbendsen.dk',
//                subject: "Failed Pipeline: ${currentBuild.fullDisplayName}",
//                body: "Something is wrong with ${env.BUILD_URL}"
//    }
}
