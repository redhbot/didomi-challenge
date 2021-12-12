import pytest

from chispa import *
from tests.helpers import *
from interview_challenge_app.common.utils import (
    deduplicate_df, lower_df_column_names
)


@pytest.mark.usefixtures("spark")
def test_deduplicate_df(spark):

    window_partition_cols = ["id"]
    window_order_cols = ["datetime"]
    window_order_type = "desc"

    df = deduplicate_df(
        get_test_data(spark, "utils", "deduplicate_df", "input"),
        window_partition_cols,
        window_order_cols,
        window_order_type
    )

    expected_df = get_test_data(spark, "utils", "deduplicate_df", "expected")

    dataframe_comparer.assert_df_equality(df, expected_df)


@pytest.mark.usefixtures("spark")
def test_lower_df_column_names(spark):

    df = lower_df_column_names(get_test_data(spark, "utils", "lower_df_column_names", "input"))

    expected_df = get_test_data(spark, "utils", "lower_df_column_names", "expected")

    dataframe_comparer.assert_df_equality(df, expected_df, ignore_column_order=True)
