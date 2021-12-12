import json
import boto3

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Window, functions as F


def get_json_from_s3(s3_endpoint: str, s3_bucket: str, key: str) -> dict:
    """
    Loads JSON file from S3 into a dict.

    Args:
        s3_endpoint: S3 endpoint URL.
        s3_bucket: S3 bucket name.
        key: Object key.

    Returns:
        The JSON file in a dict format.
    """

    s3 = boto3.client("s3", endpoint_url=s3_endpoint)

    response = s3.get_object(Bucket=s3_bucket, Key=key)

    content = response['Body'].read().decode('utf-8')

    return json.loads(content)


def get_spark_session(app_name: list) -> SparkSession:
    """
    Gets or creates a SparkSession based on a given application name and
    returns it.

    Args:
        app_name: A name for the Spark application.

    Returns:
        A Spark session object.
    """

    return SparkSession.builder.appName(app_name).getOrCreate()


def get_job_config(s3_endpoint: str, s3_bucket: str, key: str) -> dict:
    """
    Loads JSON file from S3 into a dict.

    Args:
        s3_endpoint: S3 endpoint URL.
        s3_bucket: S3 bucket name.

    Returns:
        The job config in a dict format.
    """

    return get_json_from_s3(s3_endpoint, s3_bucket, key)


def lower_df_column_names(df: DataFrame) -> DataFrame:
    """
    Applies lower-case to a Spark dataframe column names.

    Args:
        df: Dataframe whose column names are to be lower-cased.

    Returns:
        The same given dataframe with lower-case column names.
    """

    return df.select([F.col(x).alias(x.lower()) for x in df.columns])


def load_json_from_s3_to_df(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Loads JSON data from S3 bucket using a path and into a Spark dataframe.

    Args:
        spark: Current Spark session.
        s3_path: S3 path to data to load.

    Returns:
        Dataframe of loaded data.
    """

    return lower_df_column_names(spark.read.json(s3_path))


def deduplicate_df(
        df: DataFrame, window_partition_cols: list, window_order_cols: list,
        window_order_type: str
    ) -> DataFrame:
    """
    Gives duplicate position to each row of the Spark dataframe based on a
    window spec and keeps only the first row of each partition.

    Args:
        df: Dataframe to be deduplicated.
        window_partition_cols: Defines the columns that represent a partition,
            determining which rows will be in the same partition.
        window_order_cols: Defines the columns by rows in a partition are
            ordered.
        window_order_type: Defines the order type : 'asc' or 'desc'.

    Returns:
        Deduplicated dataframe.
    """

    return df \
        .withColumn(
            'duplicate_position',
            F.row_number().over(
                Window \
                    .partitionBy(window_partition_cols) \
                    .orderBy(
                        [
                            getattr(F, window_order_type)(f)
                            for f in window_order_cols
                        ]
                    )
            )
        ) \
        .where(F.col('duplicate_position') == 1) \
        .drop('duplicate_position')


def write_parquet_to_s3(df: DataFrame, s3_path: str) -> None:
    """
    Write a dataframe in Parquet on S3 by partitioning on year, month, day, and
    time.

    Args:
        df: Datafranme to write.
        s3_path: S3 path where Parquet data will be saved.
    """

    df.write.parquet(f"{s3_path}{datetime.now().strftime('year=%Y/month=%m/day=%d/%H%M%S')}")


def overwrite_csv_to_s3(df: DataFrame, s3_path: str) -> None:
    """
    Write a dataframe in CSV on S3 by overwriting destination object(s).

    Args:
        df: Datafranme to write.
        s3_path: S3 path where CSV data will be saved.
    """

    df.write \
        .save(
            s3_path,
            format = 'csv',
            delimiter = '|',
            header = 'true',
            nullValue = None,
            mode = 'overwrite'
        )
