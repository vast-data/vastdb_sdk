#!/bin/bash

set -eux
set -o pipefail
set +o posix
shopt -s inherit_errexit

env

git clean -fdx
git status --ignored
python3 --version
python3 setup.py sdist bdist_wheel

sha256sum dist/* | tee SHA256SUMS
twine check --strict dist/*
twine upload --non-interactive dist/*
