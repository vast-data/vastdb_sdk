#!/bin/bash

set -euo pipefail

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
TAG="v${VERSION}"
echo "Found version: ${TAG}"

if [[ "${CI_COMMIT_TAG:-}" != "${TAG}" ]]; then
  echo "Error: pipeline tag '${CI_COMMIT_TAG:-}' does not match package version '${TAG}'"
  exit 1
fi

if ! grep -q "${VERSION}" CHANGELOG.md; then
  echo "Error: CHANGELOG.md does not contain an entry for ${TAG}"
  exit 1
fi

if [[ "${CI_COMMIT_SHA}" != "$(git rev-parse "${TAG}^{commit}")" ]]; then
  echo "Error: Pipeline is not running on the commit tagged with current version"
  exit 1
fi

git fetch origin main --quiet
if ! git merge-base --is-ancestor "${CI_COMMIT_SHA}" origin/main; then
  echo "Error: tagged commit ${CI_COMMIT_SHA} is not on main"
  exit 1
fi

# Tag already exists on GitLab (it triggered this job); do not re-push (protected).

# Add GitHub remote
git remote -v | grep -q vastdb_sdk_github && git remote remove vastdb_sdk_github
git remote add vastdb_sdk_github https://x-access-token:${GITHUB_TOKEN}@github.com/vast-data/vastdb_sdk.git

# Push current commit to main so we have the relevant commits on GitHub
git push vastdb_sdk_github HEAD:main

echo "--- Pushing tag ${TAG} to GitHub ---"
git push vastdb_sdk_github "${TAG}"

echo "--- Releasing ${VERSION} to PyPI ---"
if curl -s "${PYPI_PACKAGE_URL}/json" | jq -e ".releases | has(\"${VERSION}\")"; then
  echo "Version ${VERSION} already exists in pypi. Skipping release"
else
  docker run --rm \
    -e TWINE_REPOSITORY_URL="$TWINE_REPOSITORY_URL" \
    -e TWINE_USERNAME="$TWINE_USERNAME" \
    -e TWINE_PASSWORD="$TWINE_TOKEN" \
    "$PYSDK_IMAGE" scripts/release.sh
fi

echo "--- Release complete ---"
