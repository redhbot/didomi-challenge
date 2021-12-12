import pytest

from chispa import *
from tests.helpers import *
from interview_challenge_app.jobs.metrics.metrics_job import MetricsJob


@pytest.mark.usefixtures("spark")
def test_prepare_events(spark):

    select_cols = [
        "datetime", "domain", "id", "type", "user", "user_token.vendors.enabled", "user_token.vendors.disabled",
        "user_token.purposes.enabled", "user_token.purposes.enabled"
    ]

    orderby_cols = ["datetime", "domain", "id"]

    metrics_job = MetricsJob(spark, {})

    df = metrics_job._prepare_events(get_test_data(spark, "metrics", "prepare_events", "input")) \
        .select(select_cols) \
        .orderBy(orderby_cols)

    expected_df = get_test_data(spark, "metrics", "prepare_events", "expected") \
        .select(select_cols) \
        .orderBy(orderby_cols)

    dataframe_comparer.assert_df_equality(df, expected_df)


@pytest.mark.usefixtures("spark")
def test_compute_metrics(spark):

    groupby_key = ["datehour", "domain", "user.country"]

    select_cols = [
        "datehour", "domain", "user_country", "pageviews", "pageviews_with_consent", "consents_asked", "consents_given",
        "consents_given_with_consent", "avg_pageviews_per_user"
    ]

    metrics_job = MetricsJob(spark, {})

    df = metrics_job._compute_metrics(get_test_data(spark, "metrics", "compute_metrics", "input"), groupby_key) \
        .select(select_cols)

    expected_df = get_test_data(spark, "metrics", "compute_metrics", "expected") \
        .select(select_cols)

    dataframe_comparer.assert_df_equality(df, expected_df, ignore_nullable=True)
