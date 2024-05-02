import pyarrow as pa
import pytest

from .. import errors, util


def test_slices():
    ROWS = 1 << 20
    t = pa.table({"x": range(ROWS), "y": [i / 1000 for i in range(ROWS)]})

    chunks = list(util.iter_serialized_slices(t))
    assert len(chunks) > 1
    sizes = [len(c) for c in chunks]

    assert max(sizes) < util.MAX_RECORD_BATCH_SLICE_SIZE
    assert t == pa.Table.from_batches(_parse(chunks))

    chunks = list(util.iter_serialized_slices(t, 1000))
    assert len(chunks) > 1
    sizes = [len(c) for c in chunks]

    assert max(sizes) < util.MAX_RECORD_BATCH_SLICE_SIZE
    assert t == pa.Table.from_batches(_parse(chunks))


def test_wide_row():
    cols = [pa.field(f"x{i}", pa.utf8()) for i in range(1000)]
    values = [['a' * 10000]] * len(cols)
    t = pa.table(values, schema=pa.schema(cols))
    assert len(t) == 1

    with pytest.raises(errors.TooWideRow):
        list(util.iter_serialized_slices(t))


def _parse(bufs):
    for buf in bufs:
        with pa.ipc.open_stream(buf) as reader:
            yield from reader
