#!/bin/sh

set -x

echo "Creating MinIO mocked S3 creds for bucket: ${AWS_S3_ENDPOINT_URL}"
mc alias set s3 "${AWS_S3_ENDPOINT_URL}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4

echo "Creating S3 bucket on MinIO"
mc mb s3/interview-challenge --ignore-existing

echo "Uploading data to MinIO mocked S3"
mc cp /data/ s3/interview-challenge/data --recursive

set +x
