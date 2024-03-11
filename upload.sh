#!/bin/bash

set -eu
set -o pipefail
set +o posix
shopt -s inherit_errexit

ci_pipeline_id="${CI_PIPELINE_ID}"
ci_project_name="${CI_PROJECT_NAME}"
ARTIFACTORY_URL=${ARTIFACTORY_URL:-"https://artifactory.vastdata.com"}
artifactory_repo_path=${ARTIFACTORY_REPO_PATH:-"files"}

jf-install() {
    local af_url
    local af_user
    local af_password

    af_url=${ARTIFACTORY_URL}
    af_user=${ARTIFACTORY_JF_USER}
    af_pw=${ARTIFACTORY_JF_PW}

    command -v jfrog || curl -sSfL https://getcli.jfrog.io | sh
    mkdir -p ~/.local/bin
    mv jfrog ~/.local/bin
    export PATH="$HOME/.local/bin:$PATH"

    # Configure
    jfrog c add --url ${af_url} --user=${af_user} --password=${af_pw} --interactive=false vastdata
}

main() {
    local unique_build
    local repopath

    # We included githash in Release so the build is unique
    unique_build=$(ls -1 dist/ | sed 's/.vast_.*//g')
    unique_build=${unique_build%-py3-none-any.whl}

    artifacts_path=${artifactory_repo_path}/${ci_project_name}/${unique_build}
    prep_upload_path=upload/${artifacts_path}

    # Prepare upload path
    rm -rf upload
    mkdir -p ${prep_upload_path}

    cp -al dist/* ${prep_upload_path} # Take the build artifacts
    # List is needed so that speardrive can download the artifact tree structure
    (cd ${prep_upload_path} && find -type f) > list.txt
    mv list.txt ${prep_upload_path}
    # Save more info from where this build came from:
    echo ${ci_pipeline_id} > ${prep_upload_path}/pipeline-id.txt
    git rev-list HEAD > ${prep_upload_path}/rev-list.txt

    # Upload it
    cd upload
    local first_component other_components
    first_component=$(echo "$artifacts_path" | awk -F'/' '{print $1}')
    other_components=${artifacts_path#*/}

    cd "${first_component}"

    jfrog rt upload --flat=false "${other_components}/" "${first_component}/"

    echo "Uploaded to: ${ARTIFACTORY_URL}/${artifacts_path}"
}

jf-install
main "$@"
