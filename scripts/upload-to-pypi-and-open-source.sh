#!/bin/bash

set -euo pipefail

export GIT_USER_NAME="${GIT_USER_NAME:-vastabase}"
export GIT_USER_EMAIL="${GIT_USER_EMAIL:-vastabase@vastdata.com}"

echo "--- Setting up SSH for GitHub ---"
eval $(ssh-agent -s)
chmod 400 $GITHUB_SSH_PRIVATE_KEY
ssh-add $GITHUB_SSH_PRIVATE_KEY

# Allow connections to any host (GitHub and GitLab) without interactive prompts
mkdir -p ~/.ssh
cat <<EOF > ~/.ssh/config
Host *
  StrictHostKeyChecking no
EOF

echo "--- Pre-release checks ---"
VERSION=$(docker run --rm "$PYSDK_IMAGE" python setup.py --version)
echo "Found version: v${VERSION}"

if ! grep -q "v${VERSION}" CHANGELOG.md; then
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
  if [[ "$CI_COMMIT_SHA" != "$(git rev-parse v${VERSION})" ]]; then
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
git push origin v${VERSION}

echo "--- Pushing tag v${VERSION} to GitHub and updating main branch ---"
git remote add github git@github.com:vast-data/vastdb_sdk.git
git push github "v${VERSION}:main" "v${VERSION}"

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
