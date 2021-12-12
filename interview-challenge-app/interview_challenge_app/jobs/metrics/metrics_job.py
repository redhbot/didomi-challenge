from pyspark.sql import DataFrame, Column, functions as F
from pyspark.sql.types import StructField, ArrayType, StringType, StructType
from interview_challenge_app.common.utils import (
    load_json_from_s3_to_df, deduplicate_df, write_parquet_to_s3,
    overwrite_csv_to_s3
)


class MetricsJob():

    def __init__(self, spark, config) -> None:
        
        self.spark = spark
        self.config = config

    def _read_events(self, s3_path: str) -> DataFrame:
        """Reads JSON events from S3 bucket."""

        return load_json_from_s3_to_df(self.spark, s3_path)


    def _prepare_events(self, df: DataFrame) -> DataFrame:
        """Prepares events data for further processing."""

        # Events data deduplication based on the following window spec 
        window_partition_cols = ["id"]
        window_order_cols = ["datetime"]
        window_order_type = "desc"
        df = deduplicate_df(df, window_partition_cols, window_order_cols, window_order_type)
        
        # Define struct schema for user.token column
        schema = StructType([
            StructField(
                "vendors",
                StructType([
                    StructField("enabled", ArrayType(StringType()), True),
                    StructField("disabled", ArrayType(StringType()), True)
                ]),
                True
            ),
            StructField(
                "purposes",
                StructType([
                    StructField("enabled", ArrayType(StringType()), True),
                    StructField("disabled", ArrayType(StringType()), True)
                ]),
                True
            )
        ])

        # Convert user.token JSON string into struct column and return the resulting dataframe
        return df.withColumn("user_token", F.from_json("user.token", schema))


    def _count_metric(self, event_type: str, with_consent: bool = False) -> Column:
        """Defines count metric."""

        if with_consent:
            return F.count(F.when((F.col("type") == event_type) & (F.size("user_token.purposes.enabled") > 0), 1))
        else:
            return F.count(F.when(F.col("type") == event_type, 1))


    def _avg_metric(self, event_type: str, count_distinct_col: str) -> Column:
        """Defines avg metric."""

        return (
            F.count(F.when(F.col("type") == event_type, 1))
            / F.count_distinct(F.when(F.col("type") == event_type, F.col(count_distinct_col)))
        )


    def _compute_metrics(self, df: DataFrame, groupby_key: list) -> DataFrame:
        """Computes metrics and returns the resulting dataframe."""

        return df \
            .groupBy(groupby_key) \
            .agg(
                self._count_metric("pageview").alias("pageviews"),
                self._count_metric("pageview", True).alias("pageviews_with_consent"),
                self._count_metric("consent.asked").alias("consents_asked"),
                self._count_metric("consent.given").alias("consents_given"),
                self._count_metric("consent.given", True).alias("consents_given_with_consent"),
                self._avg_metric("pageview", "user.id").alias("avg_pageviews_per_user")
            ) \
            .withColumnRenamed("country", "user_country") \
            .orderBy("datehour", "domain", "user_country")


    def _write_metrics(
            self, df: DataFrame, s3_output_path_hist: str,
            s3_output_path_current: str
        ) -> None:
        """Saves metrics data on S3."""

        # Log computed metrics
        df.show(truncate=False)

        # Add computed metrics to history
        write_parquet_to_s3(df, s3_output_path_hist)

        # Save current metrics
        overwrite_csv_to_s3(df, s3_output_path_current)


    def execute(self):
        """Executes metrics job."""

        # Apply data preparation on events data
        df = self._prepare_events(self._read_events(self.config["s3_input_path"]))

        # Compute specific aggregations
        df = self._compute_metrics(df, self.config["groupby_key"])

        # Write result on S3
        df = self._write_metrics(
            df.select(self.config["output_cols"]), self.config['s3_output_path_hist'], self.config['s3_output_path_current']
        )
