# PROMPTS

All AI prompts used (Cursor agent chat), grouped by part, in order. Decisions given through the agent's multiple-choice questions are listed as "Answer".

---

## Part 1 - Pipeline Design & Reconciliation

### 1. Initial brief
> Design and document a production-grade data engineering solution for a financial trading platform. We will use GCP to design, document and if get time to implement our solution.
>
> I've provided you via screenshot
> - Details about the final target tables - 4 tables
> - Vendor deposit feed - we need to reconcile
> - A CDC feed for profile
>
> In the part1, we need to do the following:
> **1a. Pipeline design document** - Write a concise design document covering:
> 1. **Architecture overview** - source-to-target flow for both the vendor CSV feed and the CDC log. Name the layers (e.g. landing, staging, target) and what happens at each.
> 2. **Idempotency strategy** - how does the pipeline ensure that re-running it does not create duplicate records? Be specific about the mechanism (e.g. file manifest, hash, watermark, merge key).
> 3. **Late and missing data** - the vendor files do not always arrive on schedule and may contain records dated earlier than you expect. How does the pipeline detect and self-reconcile missing or late data without manual intervention?
> 4. **Source-delete handling** - the CDC log includes delete events. How do you represent a deleted source record in the target warehouse? What are the trade-offs of your approach?
> 5. **Edge cases** - list 2-5 specific edge cases your design explicitly handles (name the edge case and your handling strategy for each). You may include data quality safeguards as part of your design where relevant.
>
> Go through these details and @deriv-assessment/AGENTS.md and come up with the efficient and robust architecture, and then we'll refine it together.
> Don't create the plan.md straightaway, first discuss and finalize the architecture with me and then do it after my approval

Inputs attached:
- Screenshots: core trading warehouse tables (`client_signup`, `client_profile`, `client_deposit`, `client_trades`), vendor deposit feed (3 CSVs, "0302 inspect columns carefully", "0303 inspect dates carefully"), CDC change log (`client_profile_changes.jsonl`: lsn, commit_ts, op, client_id, before, after).
- `AGENTS.md` (project rules: GCP / BigQuery / Cloud Storage / Dataform stack; idempotent pipelines; quarantine invalid rows with a reason; log row counts; explicit time zones, dedup keys and null handling; partition/cluster; MERGE or partition overwrite; dry-run and `maximum_bytes_billed`).

### 2. Design decisions (answers)
- **Vendor feed role - Answer:** "Need your suggestion, give the priority to vendor data, update the row in final table, or load it if it's not in client table, but there should be a separate table, maybe a quarantine table where these conflicting rows should go."
- **Orchestration - Answer:** Cloud Scheduler + Cloud Workflows + Cloud Run loader + Dataform (later replaced, see 5).
- **Sample data - Answer:** "Yes, I'll add them to deriv-assessment/data/".

### 3. Data review
> Go through the data/ files first where we have data and make the improvements or changes if needed

Input: 8 files added to `data/` (4 JSON, 3 CSV, 1 JSONL).
- **Matching (vendor `VDEP*` vs warehouse `DEP*` ids never overlap) - Answer:** Two-pass: exact `deposit_id`, then composite fallback (client, currency, amount +/- tolerance, date +/- N days); unmatched vendor rows inserted with `source_system = 'VENDOR'`.
- **DQ severity - Answer:** WARN: load with `dq_flags` + `audit.dq_issues`; only structural/financial errors go to quarantine.

### 4. Medallion architecture
> Need your opinion on this, what if we follow the medallion architecture here
> all the raw tables in bronze layer, create external or biglake tables for vendor and cdc data of gcs in bigquery,
> in silver layer we have 4 final tables and recon/quarantine tables
> in gold layer in future or maybe in next steps we can do modelling
>
> how do you see this idea?

- **Bronze ingestion - Answer:** BigLake raw-line tables + object-table manifest, all parsing in Dataform SQL; drop the Cloud Run loader.
- **Orchestrator - Answer:** Dataform workflow configs; SLA checks as Dataform assertions.

### 5. Confirmation
> do confirmation
> we are not doing any transformation or recon in gcs, everything is done in BQ using dataform?

- **Seed JSON arrays - Answer:** Also via BigLake raw-line tables, parsed in Dataform with a row-count assertion (100% in BigQuery).

### 6. Implementation
> Implement the plan as specified, it is attached for your reference. Do NOT edit the plan file itself.
> To-do's from the plan have already been created. Do not create them again. Mark them as in_progress as you work, starting with the first one. Don't stop until you have completed all the to-dos.

> you were stuck, do it again

> You don't need to setup dataform or run actual scripts for now. its fine just put the code there. Don't need to run anything due to time constraint. We have limited time, from now do the things quickly

> no need to build anything else on it, its fine, just put it into plan.md as plan

---

## Part 2 - Data Model & Historization

> Next step is to do the data modelling, I'll go with the Kimball star schema, update the plan with the best modelling approach and no need to write any sql, just create empty files and write some text for future implementation. Need to do it quickly
>
> ### Part 2 - Data Model & Historization
> *Model the trading warehouse; support point-in-time history and analytics.*
>
> **2a. Dimensional model / ERD** - Design a dimensional model for the trading warehouse. Address:
> - The **facts and dimensions** you would create, with grain stated for each fact table.
> - Whether you would use a **Kimball star schema, Data Vault, or another approach** - and why that choice fits this dataset.
> - How the model handles **late-arriving dimension records** (e.g. a client's first deposit or trade record arrives before their dimension row has been loaded into the warehouse).
>
> Represent the model as an ERD diagram or as a clearly labelled text schema (table name, key columns, relationships). You do not need to include all 30 columns from the raw files.
>
> **2b. Historization (SCD)** - The `client_profile_changes.jsonl` CDC feed delivers changes to `risk_category`, `account_balance_usd`, and `account_status`. Answer, with justification for every choice:
> 1. Which SCD type would you apply to these client attributes, and why? What are the trade-offs?
> 2. How does your pipeline handle **update and delete events**? Walk through the merge/upsert logic and what happens in the warehouse when a delete arrives.
> 3. You need to **reload data for a specific historical date range** (e.g. re-process November 2024). How do you do this without corrupting the existing history?

---

## Part 3 - TL Extension: Real-Time Architecture & Build vs Buy

> now last step
> we need to do it, give your best suggestion to implement it. be precise and quick
>
> ### Part 3 - TL Extension
> *Architecture and leadership judgment - this is what distinguishes the TL role from Senior.*
>
> **3a. Unified real-time and batch architecture** - The business has a new requirement: a real-time fraud-detection signal must fire within seconds of each deposit event, while the existing batch analytics pipeline continues to serve the C-suite weekly report. Both internal systems and an external partner API will consume data. Design an architecture that satisfies both requirements. Address:
> - Your tooling choice and the reasoning behind it.
> - How the real-time and batch paths coexist without one blocking the other.
> - The latency vs consistency trade-off and where you would accept eventual consistency.
> - How you ensure the external API consumer gets a consistent, secure view.
>
> **3b. Build vs buy** - A new third-party payment processor needs to be onboarded as a data source. Your team could build a custom connector or use an integration platform (e.g. Fivetran, RudderStack).
> - What criteria do you use to make this decision?
> - Walk through when each option wins, with concrete conditions.
> - What is your recommendation for this specific case, and what would change your mind?

---

## Repository packaging

> dont add or remove anything, just refactor the current file hierarchy to this format

Input: screenshot of the deliverable format (`README.md`, `part1_pipeline.md`, `part2_data_model.md`, `part3_architecture.md`, `sql/`, `code/`, `PROMPTS.md`).

> please generate prompts.md as well
> it should contain all my prompts of this chat and input and directions that I gave, be quick
