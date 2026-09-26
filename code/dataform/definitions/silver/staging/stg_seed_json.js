// Parses the JSON-array seed files: drop '[' / ']' lines, strip the trailing comma, SAFE.PARSE_JSON the rest.
// Lines that look like objects but fail to parse keep j = NULL and are rejected downstream (never dropped).
["client_signup", "client_profile", "client_deposit", "client_trades"].forEach((entity) => {
  publish(`stg_seed_${entity}_json`, {
    type: "view",
    schema: "silver_staging",
    tags: ["staging", "seed"],
    description: `Parsed JSON objects from seed ${entity}.json.`
  }).query((ctx) => `
WITH lines AS (
  SELECT _FILE_NAME AS source_uri, TRIM(line) AS line
  FROM ${ctx.ref(`seed_${entity}_raw`)}
  WHERE line IS NOT NULL
)
SELECT
  source_uri,
  REGEXP_REPLACE(line, r',\\s*$', '') AS raw_payload,
  SAFE.PARSE_JSON(REGEXP_REPLACE(line, r',\\s*$', '')) AS j
FROM lines
WHERE line NOT IN ('', '[', ']')
`);
});
