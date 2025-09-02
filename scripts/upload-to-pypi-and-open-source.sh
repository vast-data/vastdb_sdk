#!/bin/bash

set -euo pipefail

echo "--- Installing dependencies ---"
apk add --no-cache openssh-client git gpg

echo "--- Setting up SSH for GitHub ---"
eval $(ssh-agent -s)
echo "$GITHUB_SSH_PRIVATE_KEY" | tr -d '\r' | ssh-add -
mkdir -p ~/.ssh
chmod 700 ~/.ssh
echo -e "Host github.com\n\tStrictHostKeyChecking no\n" > ~/.ssh/config

echo "--- Pre-release checks ---"
VERSION=$(docker run --rm "$PYSDK_IMAGE" python setup.py --version)
echo "Found version: v${VERSION}"

if ! grep -q "v${VERSION}" CHANGELOG.md; then
  echo "Error: CHANGELOG.md does not contain an entry for v${VERSION}"
  exit 1
fi

if git tag -l "v${VERSION}" | grep -q "v${VERSION}"; then
  echo "Error: Git tag v${VERSION} already exists."
  exit 1
fi

echo "--- Configuring GPG and Git ---"
echo "$GPG_PRIVATE_KEY" | gpg --batch --import
GPG_KEY_ID=$(gpg --list-secret-keys --with-colons | grep 'sec:' | cut -d : -f5)
git config --global user.signingkey "$GPG_KEY_ID"
git config --global user.name "${GITLAB_USER_NAME}"
git config --global user.email "${GITLAB_USER_EMAIL}"

echo "--- Creating and pushing signed tag v${VERSION} to GitLab ---"
git tag --sign "v${VERSION}" -m "Release v${VERSION}"
git push --tags

echo "--- Pushing tag v${VERSION} to GitHub and updating main branch ---"
git remote add github git@github.com:vast-data/vastdb_sdk.git
git push github "v${VERSION}:main" "v${VERSION}"

echo "--- Releasing v${VERSION} to PyPI ---"
docker run --rm \
  -e TWINE_REPOSITORY_URL="$TWINE_REPOSITORY_URL" \
  -e TWINE_USERNAME="$TWINE_USERNAME" \
  -e TWINE_PASSWORD="$TWINE_PASSWORD" \
  "$PYSDK_IMAGE" scripts/release.sh

echo "--- Release complete ---"
