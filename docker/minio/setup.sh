#!/bin/sh

# We set this so that we see all the executed lines in the console
set -x

# We'll run this function in case we want to upload data to the MinIO container
# This will run unless the `no-data` argument is passed
setup_minio() {
  echo "Creating s3 bucket on minio"
  mc mb s3/didomi-challenge --ignore-existing
  
  echo "Uploading data to s3"
  mc cp /data/events/ s3/didomi-challenge/events --recursive
}

echo "Creating s3 creds bucket on minio ${S3_ENDPOINT}"
mc alias set s3 "${S3_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4

case "$1" in
no-data)
  echo "Skipping setup"
  ;;
"")
  setup_minio
  ;;
esac

set +x
