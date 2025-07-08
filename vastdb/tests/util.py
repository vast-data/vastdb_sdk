import logging
from contextlib import contextmanager
from typing import Any, cast, List

import numpy as np
import pandas as pd
import pyarrow as pa
from packaging.version import Version

from vastdb.session import Session

log = logging.getLogger(__name__)


def assert_row_ids_ascending_on_first_insertion_to_table(row_ids, expected_num_rows, sorted_table):
    adjusted_row_ids = [
        int(row_id) & 0xFFFFFFFFFFFFFF for row_id in row_ids
        ] if sorted_table else row_ids

    assert adjusted_row_ids == list(range(expected_num_rows))


@contextmanager
def prepare_data(session: Session,
                 clean_bucket_name: str, schema_name: str, table_name: str,
                 arrow_table: pa.Table, sorting_key: List[str]=[]):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema, sorting_key=sorting_key)
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert_row_ids_ascending_on_first_insertion_to_table(row_ids, arrow_table.num_rows, t.sorted_table)
        yield t
        t.drop()
        s.drop()


def compare_pyarrow_tables(t1, t2):

    def sort_table(table):
        return table.sort_by([(col, 'ascending') for col in table.schema.names])

    def compare_tables(table1, table2):
        if table1.schema != table2.schema:
            raise RuntimeError(f"Schema mismatch. {table1.schema} vs {table2.schema}")

        for t1_col, t2_col in zip(table1.columns, table2.columns):
            if not pa.compute.equal(t1_col, t2_col).to_pandas().all():
                raise RuntimeError(f"Data mismatch in column {t1_col} vs {t2_col}.")
        return True

    sorted_table1 = sort_table(t1)
    sorted_table2 = sort_table(t2)
    return compare_tables(sorted_table1, sorted_table2)


def convert_pandas_df_to_hashable_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all values in the DataFrame to hashable types.
    This is useful for comparing DataFrames or using them as keys in dictionaries.

    :param df: Input DataFrame.
    :return: DataFrame with all values converted to hashable types.
    """

    def _to_hashable(x: Any) -> Any:
        if isinstance(x, (list, set, np.ndarray)):
            return tuple(x)  # type: ignore
        return x

    if Version(pd.__version__) >= Version("2.1.0"):
        return df.map(_to_hashable)  # type: ignore
    else:
        return df.applymap(_to_hashable)  # type: ignore


def assert_pandas_df_equal(a: pd.DataFrame, b: pd.DataFrame, ignore_columns_order: bool = True,
                           ignore_rows_order: bool = True, **kwargs) -> None:
    """ Assert 2 Pandas DataFrames are equal.

    :param a: First DataFrame.
    :param b: Second DataFrame.
    :param ignore_columns_order: Whether to ignore the column order.
    :param ignore_rows_order: Whether to ignore the rows order.
    :param kwargs: Additional keyword arguments to pass to `pd.testing.assert_frame_equal`.
    """
    assert set(a.columns) == set(b.columns), f'unmatched columns {a.columns} {b.columns}'
    # Sort columns. Done instead of using pd.testing.assert_frame_equal(check_like=False) in order to allow the sort
    # of the rows according to the same order, their columns order.
    if ignore_columns_order:
        b = b[a.columns]
    pd.testing.assert_index_equal(a.columns, b.columns)

    # Sort rows.
    if ignore_rows_order:
        a = convert_pandas_df_to_hashable_values(a)
        b = convert_pandas_df_to_hashable_values(b)
        a = a.sort_values(by=a.columns.tolist()).reset_index(drop=True)
        b = b.sort_values(by=b.columns.tolist()).reset_index(drop=True)

    pd.testing.assert_frame_equal(a, b, **kwargs)


def assert_pandas_df_contained(a: pd.DataFrame, b: pd.DataFrame, ignore_value_count=False, ignore_columns_order=True):
    """
    Assert A is contained in B.

    :param a: First DataFrame.
    :param b: Second DataFrame.
    :param ignore_value_count: Succeed even in case a value in A exists in B, but has more occurrences in A.
    :param ignore_columns_order: Whether to ignore the column order.
    :note: Expecting A and B to have the same columns.
    """
    assert set(a.columns) == set(b.columns), f'unmatched columns {a.columns} {b.columns}'
    if ignore_columns_order:
        b = b[a.columns]
    pd.testing.assert_index_equal(a.columns, b.columns)

    a = convert_pandas_df_to_hashable_values(a)
    b = convert_pandas_df_to_hashable_values(b)

    if ignore_value_count:
        merged = a.merge(b, on=a.columns, how='left', indicator=True)
        assert cast(pd.Series, (merged['_merge'] == 'both')).all(), f'values are not contained {merged=}'
    else:
        a_counts = a.value_counts(dropna=False) if Version(pd.__version__) >= Version("1.3.0") else a.value_counts()
        b_counts = b.value_counts(dropna=False) if Version(pd.__version__) >= Version("1.3.0") else b.value_counts()

        # Check that each row in A occurs in B with at least the same frequency.
        count_compare = cast(pd.Series, a_counts <= b_counts.reindex(a_counts.index, fill_value=0))
        assert count_compare.all(), (f'some values have lower frequency, all values with False has higher frequency in '
                                     f'A than B {count_compare=}')
