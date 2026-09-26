#!/usr/bin/env bash
# Wraps the Dataform CLI so environment-specific settings come from environment variables.
# Usage: ./code/scripts/dataform.sh <compile|run|test> [extra dataform args]
set -euo pipefail

: "${GCP_PROJECT:?GCP_PROJECT must be set}"
: "${GCP_LOCATION:?GCP_LOCATION must be set}"
: "${GCS_BUCKET:?GCS_BUCKET must be set}"
: "${BIGLAKE_CONNECTION:?BIGLAKE_CONNECTION must be set}"

cmd="${1:?usage: dataform.sh <compile|run|test> [args]}"
shift

PROJECT_DIR="$(cd "$(dirname "$0")/../dataform" && pwd)"

vars="gcs_bucket=${GCS_BUCKET},biglake_connection=${GCP_PROJECT}.${GCP_LOCATION}.${BIGLAKE_CONNECTION}"
vars="${vars},as_of_date=${AS_OF_DATE:-}"

dataform "${cmd}" "${PROJECT_DIR}" \
  --default-database="${GCP_PROJECT}" \
  --default-location="${GCP_LOCATION}" \
  --vars="${vars}" \
  "$@"
