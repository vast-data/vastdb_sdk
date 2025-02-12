#!/bin/bash
set -eux

cd $(dirname $0)
rm -rf vast_flatbuf/ # remove top-level package
WORKDIR=$PWD

cd ../..  # change to repo root

# generate Tabular-related flatbuffers' Python code
flatc --python -o $WORKDIR src/tabular/flatbuf/*.fbs

# generate Arrow-vendored flatbuffers' Python code
ARROW_FBS=$(find src/tabular/arrow/flatbuf -name '*.fbs')
flatc --python -o $WORKDIR/vast_flatbuf $ARROW_FBS
# unfortunately, Arrow-vendored flatbuffers are using `org.apache.arrow.*` namespace :(
# so we fixup the generated `import`s by prefixing all imports with the `vastdb.vast_flatbuf.` namespace prefix.
# TODO: a better solution will be to patch flatc to generate relative imports.
ARROW_GENERATED=$(find $WORKDIR/vast_flatbuf/org/apache/arrow -name '*.py')
sed -i 's/from org.apache.arrow./from vastdb.vast_flatbuf.org.apache.arrow./g' $ARROW_GENERATED
