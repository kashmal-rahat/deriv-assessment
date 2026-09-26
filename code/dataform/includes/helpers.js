function sqlList(values) {
  return values.map((v) => `'${v}'`).join(", ");
}

// Comma-separated, sorted list of codes whose condition is TRUE; NULL when none apply.
// A NULL condition counts as not triggered, so null-handling must be explicit in the condition.
function codes(rules) {
  const items = rules.map(([condition, code]) => `IF(${condition}, '${code}', NULL)`).join(",\n      ");
  return `NULLIF(ARRAY_TO_STRING(ARRAY(
    SELECT c FROM UNNEST([
      ${items}
    ]) AS c WHERE c IS NOT NULL ORDER BY c
  ), ','), '')`;
}

function concatCodes(...exprs) {
  const parts = exprs.map((e) => `IFNULL(${e}, '')`).join(", ',', ");
  return `NULLIF(ARRAY_TO_STRING(ARRAY(
    SELECT DISTINCT c FROM UNNEST(SPLIT(CONCAT(${parts}), ',')) AS c WHERE c != '' ORDER BY c
  ), ','), '')`;
}

function rowHash(columns) {
  return `TO_HEX(SHA256(TO_JSON_STRING(STRUCT(${columns.join(", ")}))))`;
}

function aliasCase(expr, aliases) {
  const whens = Object.entries(aliases)
    .map(([from, to]) => `WHEN '${from}' THEN '${to}'`)
    .join(" ");
  return `(CASE ${expr} ${whens} ELSE ${expr} END)`;
}

// Typed scalar from a JSON value; NULL for explicit JSON null, missing key, or unparseable value.
function jsonValue(jsonExpr, key, type) {
  const raw = `JSON_VALUE(${jsonExpr}, '$.${key}')`;
  switch (type) {
    case "STRING":
      return `NULLIF(TRIM(${raw}), '')`;
    case "DATE":
      return `SAFE.PARSE_DATE('%Y-%m-%d', ${raw})`;
    case "NUMERIC":
      return `SAFE_CAST(${raw} AS NUMERIC)`;
    case "INT64":
      return `SAFE_CAST(${raw} AS INT64)`;
    default:
      throw new Error(`Unsupported type ${type}`);
  }
}

// TRUE when the key exists in the object, including when its value is JSON null.
function jsonHasKey(jsonExpr, key) {
  return `(JSON_QUERY(${jsonExpr}, '$.${key}') IS NOT NULL)`;
}

function rawLineTableDdl({ connection, uriPrefix, hivePartitioned, delimiter }) {
  const partition = hivePartitioned ? "WITH PARTITION COLUMNS (arrival_date DATE)" : "";
  const hiveOption = hivePartitioned ? `hive_partition_uri_prefix = '${uriPrefix}',` : "";
  return `(line STRING)
${partition}
WITH CONNECTION \`${connection}\`
OPTIONS (
  format = 'CSV',
  uris = ['${uriPrefix}/*'],
  ${hiveOption}
  field_delimiter = '${delimiter}',
  quote = '',
  skip_leading_rows = 0,
  allow_quoted_newlines = FALSE,
  encoding = 'UTF-8'
)`;
}

module.exports = { sqlList, codes, concatCodes, rowHash, aliasCase, jsonValue, jsonHasKey, rawLineTableDdl };
