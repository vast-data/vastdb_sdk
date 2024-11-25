#!/bin/bash
set -eux
export VASTDB_APPEND_VERSION_SUFFIX=true

python3 --version
python3 setup.py sdist bdist_wheel

pushd dist
pip3 install *.whl
pip3 freeze
unique_build=$(ls -1 *.whl | sed 's/.vast_.*//g')
unique_build=${unique_build%-py3-none-any.whl}
echo "unique_build = $unique_build"
popd

# used to generate https://vastdb-sdk.readthedocs.io/
pushd docs
pip3 install -r requirements.txt
rm -rf build/
sphinx-build -b html source/ build/html
tar cvfz ../dist/${unique_build}-docs-html.tgz -C build html
popd

pushd dist
ls -lh
sha256sum * | tee SHA256SUMS
