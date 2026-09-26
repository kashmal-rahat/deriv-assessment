# Infrastructure setup

These commands create cloud resources. They are documented here and are **not** run automatically.
All environment-specific values come from environment variables; nothing is hardcoded.

## 0. Environment

```bash
export GCP_PROJECT=<project-id>
export GCP_LOCATION=US                        # BigQuery datasets and the BigLake connection must share this location
export GCS_BUCKET=<project-id>-trading-landing
export BIGLAKE_CONNECTION=biglake-landing     # connection id (not the full path)
```

## 1. Landing bucket (immutable, versioned)

```bash
gcloud storage buckets create "gs://${GCS_BUCKET}" \
  --project="${GCP_PROJECT}" --location="${GCP_LOCATION}" \
  --uniform-bucket-level-access --public-access-prevention

# Keep every generation of every object; a re-delivered file becomes a new generation, never an overwrite.
gcloud storage buckets update "gs://${GCS_BUCKET}" --versioning

# Objects cannot be deleted or replaced for 7 years (financial record retention). Lock only after review:
gcloud storage buckets update "gs://${GCS_BUCKET}" --retention-period=7y
# gcloud storage buckets update "gs://${GCS_BUCKET}" --lock-retention-period   # irreversible
```

### Bucket layout

```
gs://${GCS_BUCKET}/
  vendor/arrival_date=YYYY-MM-DD/deposits_vendor_YYYYMMDD.csv     # partitioned by ARRIVAL date
  cdc/client_profile/arrival_date=YYYY-MM-DD/*.jsonl
  seed/client_signup/client_signup.json                           # initial state of target tables
  seed/client_profile/client_profile.json
  seed/client_deposit/client_deposit.json
  seed/client_trades/client_trades.json
```

The business date of a vendor file comes from its name (`YYYYMMDD`); the Hive `arrival_date` is when it landed.

## 2. BigLake connection

```bash
bq mk --connection --project_id="${GCP_PROJECT}" --location="${GCP_LOCATION}" \
  --connection_type=CLOUD_RESOURCE "${BIGLAKE_CONNECTION}"

# Grant the connection's service account read access to the bucket (read only - bronze never writes).
CONN_SA=$(bq show --format=json --connection "${GCP_PROJECT}.${GCP_LOCATION}.${BIGLAKE_CONNECTION}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["cloudResource"]["serviceAccountId"])')
gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${CONN_SA}" --role=roles/storage.objectViewer
```

## 3. BigQuery datasets

```bash
for ds in bronze silver_staging silver quarantine recon audit gold dataform_assertions; do
  bq --location="${GCP_LOCATION}" mk --dataset --project_id="${GCP_PROJECT}" "${GCP_PROJECT}:${ds}"
done
```

| Dataset | Purpose |
|---|---|
| `bronze` | BigLake raw-line tables and the object table |
| `silver_staging` | Dataform views: parsing, typing, DQ rules, matching |
| `silver` | Target tables (`client_signup`, `client_profile`, `client_profile_history`, `client_deposit`, `client_trades`, `vendor_deposit`) |
| `quarantine` | `rejected_records` |
| `recon` | `deposit_match`, `deposit_conflicts`, `deposit_reconciliation` |
| `audit` | `file_manifest`, `expected_files`, `dq_issues`, `run_log` |
| `dataform_assertions` | Assertion views |

Recommended: attach BigQuery policy tags to PII columns (`email`, `full_name`, `date_of_birth`) in `silver`.

## 4. Dataform service account

The Dataform service account needs `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the project,
and `roles/bigquery.connectionUser` on the BigLake connection (to create BigLake tables).

## 5. Load sample data

`code/scripts/upload_sample_data.sh` copies `data/` into the layout above, simulating arrival dates
(0301 on 2024-03-01, 0302 on 2024-03-02, 0303 arriving late on 2024-03-05, CDC on 2024-11-24).

## 6. Run the pipeline

`code/scripts/dataform.sh` passes the environment variables above to the Dataform CLI:

```bash
./code/scripts/dataform.sh compile
AS_OF_DATE=2024-03-05 ./code/scripts/dataform.sh run     # replay the sample as of 2024-03-05
./code/scripts/dataform.sh test                          # unit tests
```

In production, a Dataform release configuration sets the same vars (project, bucket, connection) and a workflow
configuration schedules the run; `AS_OF_DATE` is left empty so it defaults to `CURRENT_DATE()`.
