import os
import importlib

from interview_challenge_app.common.utils import get_spark_session, get_json_from_s3


if __name__ == "__main__":

    # Get app configuration
    config = get_json_from_s3(
        os.environ["AWS_S3_ENDPOINT_URL"], os.environ["AWS_S3_BUCKET_NAME"], "data/config/common/config.json"
    )

    # Get SparkSession
    spark = get_spark_session(config["app_name"])

    # For each Spark job...
    for job in config["jobs"]:
        # ... import class
        job_cls = getattr(
            importlib.import_module(f"interview_challenge_app.jobs.{job}.{job}_job"), f"{job.capitalize()}Job"
        )
        # Get job configuration
        config = get_json_from_s3(
            os.environ["AWS_S3_ENDPOINT_URL"], os.environ["AWS_S3_BUCKET_NAME"], f"data/config/jobs/{job}/config.json"
        )
        # Execute job
        job_instance = job_cls(spark, config)
        job_instance.execute()
