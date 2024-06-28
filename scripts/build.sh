#!/bin/bash
set -eux
export VASTDB_APPEND_VERSION_SUFFIX=true

python3 --version
python3 setup.py sdist bdist_wheel
cd dist

ls -lh
sha256sum * | tee SHA256SUMS

pip3 install *.whl
pip3 freeze

cd ../docs
pip3 install -r requirements.txt
rm -rf build/
sphinx-build -b html source/ build/html