#!/bin/bash

set -euo pipefail

export GIT_USER_NAME="${GIT_USER_NAME:-vastabase}"
export GIT_USER_EMAIL="${GIT_USER_EMAIL:-vastabase@vastdata.com}"

echo "--- Get token for GitHub ---"
GITHUB_TOKEN=$(docker run --rm \
  -e GITHUB_CI_BOT_PEM="/github_ci_bot_key.pem" \
  -v "${GITHUB_CI_BOT_PEM}:/github_ci_bot_key.pem" \
  110450271409.dkr.ecr.eu-west-1.amazonaws.com/devops/tools:gh-token-generator "$GITHUB_CI_BOT_APP_ID")

# Allow connections to any host without interactive prompts
mkdir -p ~/.ssh
cat <<EOF > ~/.ssh/config
Host *
  StrictHostKeyChecking no
EOF

echo "--- Pre-release checks ---"
VERSION=$(docker run --rm "$PYSDK_IMAGE" python setup.py --version)
echo "Found version: v${VERSION}"

if ! grep -q "${VERSION}" CHANGELOG.md; then
  echo "Error: CHANGELOG.md does not contain an entry for v${VERSION}"
  exit 1
fi

echo "--- Configuring GPG and Git ---"
echo "$GPG_PRIVATE_KEY" | gpg --batch --import
GPG_KEY_ID=$(gpg --list-secret-keys --with-colons | grep 'sec:' | cut -d : -f5)
git config --global user.signingkey "$GPG_KEY_ID"
git config --global user.name "${GIT_USER_NAME}"
git config --global user.email "${GIT_USER_EMAIL}"

echo "--- Creating and pushing signed tag v${VERSION} to GitLab ---"
if git tag -l "v${VERSION}" | grep -q "v${VERSION}"; then
  echo "Git tag v${VERSION} already exists. Skipping tagging"
  if [[ "$CI_COMMIT_SHA" != "$(git rev-parse v${VERSION}^{commit})" ]]; then
    echo "Error: Pipeline is not running on the commit tagged with current version"
    exit 1
  fi
  if ! git tag --verify v${VERSION}; then
    echo "Error: tag is not signed, manual resolution needed. Skipping"
    exit 1
  fi
else
  git tag --sign "v${VERSION}" -m "Release v${VERSION}"
fi

# Push to GitLab using the CI job token over HTTPS
GITLAB_PUSH_URL="https://oauth2:${GITLAB_PUSH_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"
git push $GITLAB_PUSH_URL v${VERSION}

echo "--- Pushing tag v${VERSION} to GitHub ---"
GITHUB_PUSH_URL="https://x-access-token:${GITHUB_TOKEN}@github.com/vast-data/vastdb_sdk.git"
git push ${GITHUB_PUSH_URL} "v${VERSION}"

echo "--- Releasing v${VERSION} to PyPI ---"
if curl -s ${PYPI_PACKAGE_URL}/json | jq  -e ".releases | has(\"${VERSION}\")"; then
  echo "Version ${VERSION} already exists in pypi. Skipping release"
else
  docker run --rm \
    -e TWINE_REPOSITORY_URL="$TWINE_REPOSITORY_URL" \
    -e TWINE_USERNAME="$TWINE_USERNAME" \
    -e TWINE_PASSWORD="$TWINE_TOKEN" \
    "$PYSDK_IMAGE" scripts/release.sh
fi

echo "--- Release complete ---"
