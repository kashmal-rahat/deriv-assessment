// Raw-line BigLake tables over the four warehouse JSON-array seed files.
["client_signup", "client_profile", "client_deposit", "client_trades"].forEach((entity) => {
  operate(`seed_${entity}_raw`, {
    schema: "bronze",
    hasOutput: true,
    tags: ["bronze", "seed"],
    description: `Seed ${entity}.json (JSON array, one object per line) as raw lines.`
  }).queries((ctx) => `CREATE OR REPLACE EXTERNAL TABLE ${ctx.self()}
${helpers.rawLineTableDdl({
  connection: cfg.CONNECTION,
  uriPrefix: `gs://${cfg.BUCKET}/seed/${entity}`,
  hivePartitioned: false,
  delimiter: cfg.RAW_LINE_DELIMITER
})}`);
});
