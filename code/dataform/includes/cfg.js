const vars = dataform.projectConfig.vars || {};

function requiredVar(name) {
  const value = vars[name];
  if (!value) {
    throw new Error(`Dataform var '${name}' is required (set it via scripts/dataform.sh)`);
  }
  return value;
}

function optionalDateVar(name) {
  const value = vars[name];
  if (!value) {
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Dataform var '${name}' must be YYYY-MM-DD, got '${value}'`);
  }
  return value;
}

function intVar(name, fallback) {
  const value = vars[name];
  if (value === undefined || value === "") {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed < 0) {
    throw new Error(`Dataform var '${name}' must be a non-negative integer, got '${value}'`);
  }
  return parsed;
}

const asOfDate = optionalDateVar("as_of_date");

module.exports = {
  BUCKET: requiredVar("gcs_bucket"),
  CONNECTION: requiredVar("biglake_connection"),

  // All time-relative logic (lookback, grace, SLA) is evaluated against this date, in UTC.
  AS_OF_DATE: asOfDate ? `DATE '${asOfDate}'` : "CURRENT_DATE('UTC')",

  // Arrival-date window re-read on each incremental run. Raise it (or full refresh) to backfill.
  LOOKBACK_DAYS: intVar("lookback_days", 7),
  GRACE_DAYS: intVar("grace_days", 3),
  VENDOR_SLA_DAYS: intVar("vendor_sla_days", 1),
  PII_RETENTION_DAYS: intVar("pii_retention_days", 2555),

  AMOUNT_TOLERANCE_PCT: 0.005,
  AMOUNT_TOLERANCE_MIN_USD: 0.01,
  DATE_TOLERANCE_DAYS: 3,

  CDC_BASELINE_LSN: intVar("cdc_baseline_lsn", 1000),
  VENDOR_FEED_START_DATE: optionalDateVar("vendor_feed_start_date") || "2024-03-01",
  RECON_START_DATE: optionalDateVar("recon_start_date") || "2024-03-01",

  // Byte that never occurs in the feeds: makes BigLake return each physical line as one STRING column.
  RAW_LINE_DELIMITER: "\\x1f",

  VENDOR_COLUMNS: [
    "deposit_id",
    "client_id",
    "deposit_date",
    "amount_usd",
    "payment_method",
    "currency_original",
    "exchange_rate",
    "status",
    "processing_days",
    "fee_usd"
  ],
  VENDOR_REQUIRED_COLUMNS: [
    "deposit_id",
    "client_id",
    "deposit_date",
    "amount_usd",
    "payment_method",
    "currency_original",
    "exchange_rate",
    "status"
  ],
  // Lower-cased vendor header -> canonical column.
  VENDOR_COLUMN_ALIASES: {
    method: "payment_method",
    pay_method: "payment_method",
    payment_type: "payment_method",
    amount: "amount_usd",
    currency: "currency_original",
    fx_rate: "exchange_rate",
    fee: "fee_usd",
    date: "deposit_date",
    deposit_ref: "deposit_id"
  },

  DEPOSIT_STATUSES: ["completed", "pending", "failed", "rejected", "reversed", "cancelled"],
  PAYMENT_METHODS: ["bank_transfer", "credit_card", "debit_card", "e_wallet", "crypto"],
  TRADE_DIRECTIONS: ["buy", "sell"],
  TRADE_STATUSES: ["open", "closed", "cancelled"],

  // Every staging view exposes source_uri, error_codes, warn_codes, record_status plus these key/payload columns.
  STAGING_SOURCES: [
    { entity: "vendor_deposit", view: "stg_vendor_deposit", key: "deposit_id", payload: "raw_line" },
    { entity: "client_profile_cdc", view: "stg_cdc_client_profile", key: "CAST(lsn AS STRING)", payload: "raw_line" },
    { entity: "client_signup", view: "stg_client_signup", key: "client_id", payload: "raw_payload" },
    { entity: "client_profile_seed", view: "stg_client_profile_seed", key: "client_id", payload: "raw_payload" },
    { entity: "client_deposit_seed", view: "stg_client_deposit_seed", key: "deposit_id", payload: "raw_payload" },
    { entity: "client_trades", view: "stg_client_trades", key: "trade_id", payload: "raw_payload" }
  ],

  // Profile attributes carried by the snapshot and CDC images, with their target types.
  PROFILE_COLUMNS: [
    { name: "full_name", type: "STRING" },
    { name: "date_of_birth", type: "DATE" },
    { name: "nationality", type: "STRING" },
    { name: "risk_category", type: "STRING" },
    { name: "account_balance_usd", type: "NUMERIC" },
    { name: "account_status", type: "STRING" },
    { name: "currency", type: "STRING" },
    { name: "last_login_date", type: "DATE" },
    { name: "preferred_language", type: "STRING" }
  ]
};
