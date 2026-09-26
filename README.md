# Deriv Trading Data Platform - Data Engineering Assessment

Production-grade design for a trading warehouse on GCP: a vendor deposit feed reconciled against the warehouse, a client-profile CDC log, a Kimball star for analytics, and a real-time fraud extension.

**Stack:** Cloud Storage, BigLake, BigQuery, Dataform (medallion: GCS landing -> bronze -> silver -> gold). GCS only stores files; every transformation, validation, CDC apply and reconciliation runs in BigQuery via Dataform.

## Where to start

| Read | For |
|---|---|
| [`part1_pipeline.md`](part1_pipeline.md) | Part 1: architecture, idempotency, late/missing data, source deletes, edge cases (with real examples from `data/`) |
| [`part2_data_model.md`](part2_data_model.md) | Part 2: Kimball star, ERD, fact grains, late-arriving dimensions, SCD choices, update/delete handling, historical reload |
| [`part3_architecture.md`](part3_architecture.md) | Part 3: unified real-time + batch architecture, build vs buy |
| [`PLAN.md`](PLAN.md) | Step checklist, agreed decisions (D1-D14), decision log, remaining work |
| [`PROMPTS.md`](PROMPTS.md) | All AI prompts used, grouped by part |

## Repository layout

```
README.md                  this file
part1_pipeline.md          Part 1 - Pipeline Design & Reconciliation
part2_data_model.md        Part 2 - Data Model & Historization
part3_architecture.md      Part 3 - TL Extension
PLAN.md                    plan, decisions, remaining work
PROMPTS.md                 AI prompts by part
AGENTS.md                  engineering rules followed by the AI agent
data/                      sample inputs (4 warehouse JSON seeds, 3 vendor CSVs, 1 CDC JSONL)
sql/gold/                  Part 2 gold star schema (placeholders: dims, facts, range reload)
code/
  infra/setup.md           GCS bucket, BigLake connection, datasets (documented commands)
  scripts/dataform.sh      Dataform CLI wrapper; config from environment variables
  scripts/upload_sample_data.sh  lands data/ in GCS with simulated arrival dates
  dataform/                Part 1 pipeline (Dataform project)
    includes/              cfg.js (tolerances, aliases, vars), helpers.js (SQL builders)
    definitions/bronze/    BigLake raw-line tables + object table
    definitions/audit/     file_manifest
    definitions/silver/staging/  parsing, typing, DQ rules, CDC version fold, deposit matching
    definitions/silver/core/     client_signup, client_profile (+history, active view), client_trades,
                                 vendor_deposit, client_deposit (+ vendor-priority apply)
    definitions/recon/     deposit_match, deposit_conflicts, deposit_reconciliation
    definitions/quality/   quarantine.rejected_records
```

## Key design points
- **Bronze** reads every file as raw lines, so vendor columns are mapped by header name and schema drift (0302 `method`) cannot shift data.
- **Idempotency:** file manifest on (uri, generation), MERGE on business keys with `row_hash`, CDC applied only when `lsn > last_applied_lsn`.
- **Late data:** bronze is partitioned by arrival date, so the back-dated 0303 file is always picked up. Reconciliation states self-heal.
- **Vendor priority:** two-pass matching. Conflicts are logged in `recon.deposit_conflicts`. Invalid rows go to `quarantine.rejected_records`.
- **Deletes:** soft delete plus SCD2 history. PII can be erased after the retention period.

## Status
- Part 1 pipeline code is written but **not compiled or run** (time constraint). Remaining items and validation steps: `PLAN.md` steps 8-9.
- Part 2 gold files in `sql/gold/` are placeholders with implementation notes.
- Part 3 is design only.

## Run (after infra is set up)

```bash
export GCP_PROJECT=... GCP_LOCATION=US GCS_BUCKET=... BIGLAKE_CONNECTION=biglake-landing
./code/scripts/upload_sample_data.sh                     # creates GCS objects
./code/scripts/dataform.sh compile
AS_OF_DATE=2024-03-05 ./code/scripts/dataform.sh run     # replay the sample as of 2024-03-05
```
