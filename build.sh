#!/bin/bash
set -eux
export VASTDB_APPEND_VERSION_SUFFIX=true

python3 --version
python3 setup.py sdist bdist_wheel
ls -l dist/

pip3 install dist/*.whl
pip3 freeze
