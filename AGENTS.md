## Project
- Goal: Design and document a production-grade data engineering solution for a financial trading platform. 
- Stack: GCP, Bigquery, Cloud Storage, Dataform


## Rules
- Follow PLAN.md one step at a time. After each step, check it off and record any decisions or changed assumptions in PLAN.md. List the files you'll change before large edits.
- Keep solutions simple but efficient. Minimal, focused changes; no new dependencies without asking.
- Ask when something is unclear, and before any command that creates, modifies, or deletes cloud resources or data.
- No hardcoded secrets, credentials, or environment-specific config; read them from environment variables. Handle errors explicitly; never fail silently.
- Add tests only for core logic (transformations, validation). Run them before claiming it works; never weaken or delete tests to make them pass.
- Don't edit README.md until I ask. When asked, document setup, run, test, and deploy commands.
- After each change, summarize it and flag assumptions and tradeoffs. If a fix fails twice, stop and explain.

## Data
- Pipelines must be idempotent: rerunning must not duplicate or corrupt data.
- Never drop rows silently. Send invalid rows to a quarantine output with a reason.
- Log row counts read, rejected, and loaded at each step.
- Be explicit about time zones, deduplication keys, and null handling.
- Prefer set-based SQL or dataframe operations over row-by-row loops.

## Cloud (only if the task uses a cloud warehouse or storage)
- Partition large tables by date and cluster where useful. Use MERGE or partition overwrite for loads, not blind appends.
- Avoid SELECT * and full scans; filter on partitions. On BigQuery, dry-run expensive queries and set maximum_bytes_billed.