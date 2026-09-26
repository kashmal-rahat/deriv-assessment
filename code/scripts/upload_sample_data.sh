#!/usr/bin/env bash
# Copies data/ into the landing layout, simulating arrival dates. Creates GCS objects - run only when intended.
set -euo pipefail

: "${GCS_BUCKET:?GCS_BUCKET must be set}"

DATA_DIR="$(cd "$(dirname "$0")/../data" && pwd)"
DEST="gs://${GCS_BUCKET}"

cp_obj() {
  echo "upload $1 -> $2"
  gcloud storage cp "${DATA_DIR}/$1" "${DEST}/$2"
}

for entity in client_signup client_profile client_deposit client_trades; do
  cp_obj "${entity}.json" "seed/${entity}/${entity}.json"
done

cp_obj deposits_vendor_20240301.csv vendor/arrival_date=2024-03-01/deposits_vendor_20240301.csv
cp_obj deposits_vendor_20240302.csv vendor/arrival_date=2024-03-02/deposits_vendor_20240302.csv
# Late delivery: business date 2024-03-03, landed 2024-03-05.
cp_obj deposits_vendor_20240303.csv vendor/arrival_date=2024-03-05/deposits_vendor_20240303.csv

cp_obj client_profile_changes.jsonl cdc/client_profile/arrival_date=2024-11-24/client_profile_changes.jsonl
