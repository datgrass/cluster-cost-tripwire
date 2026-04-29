# Databricks notebook source
# MAGIC %md
# MAGIC # 00_baseline_refresh
# MAGIC
# MAGIC **Notebook Summary:** Builds the reference data `01_monitor` needs to score live clusters
# MAGIC - Current DBU/hr per node type (from the public pricing JSON)
# MAGIC - The customer's contracted `$/DBU` (from `system.billing.account_prices`)
# MAGIC - This workspace's historical `$/cluster` distribution used to detect statistical outliers.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `main` | Catalog where all tables and views live. Auto-created if missing. |
# MAGIC | `schema` | `cluster_early_warning` | Schema under `catalog`. Auto-created if missing. |
# MAGIC | `baseline_lookback_days` | `30` | Window of history from `system.billing.usage` used to calculate the workspace's historical $/cluster statistics (mean, stddev, percentiles) for outlier analysis. |
# MAGIC
# MAGIC This job produces five outputs:
# MAGIC 1. `pricing_rates` — flattened all-purpose pricing snapshot from the public pricing JSON, with `$/DBU` overridden to the SKU this workspace bills against.
# MAGIC 2. `workspace_baseline` — per-day stats of daily `$/cluster` for classic all-purpose in this workspace over the lookback window.
# MAGIC 3. `cluster_scores` — each all-purpose cluster's distance from the daily `$/cluster` mean
# MAGIC 4. `alert_send_log` — append-only audit of Slack messages sent by `03_slack_alerting`; backs the per-(cluster, severity) suppression window.
# MAGIC 5. `cluster_alerts_latest` — the clusters that were flagged as outliers for review

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "cluster_early_warning")
dbutils.widgets.text("baseline_lookback_days", "30")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LOOKBACK_DAYS = int(dbutils.widgets.get("baseline_lookback_days"))

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_ID = _ctx.workspaceId().get()

# Auto-detect cloud from the workspace host URL.
# Casing matches the public pricing JSON filenames (AWS.json, Azure.json, GCP.json).
_host = _ctx.apiUrl().get().lower()
if "azuredatabricks.net" in _host:
    CLOUD = "Azure"
elif "gcp.databricks.com" in _host:
    CLOUD = "GCP"
elif ".databricks.com" in _host:
    CLOUD = "AWS"
else:
    raise RuntimeError(f"Could not determine cloud from workspace host: {_host}")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")

print(f"workspace_id={WORKSPACE_ID}, catalog={CATALOG}, schema={SCHEMA}, cloud={CLOUD} (auto-detected), lookback={LOOKBACK_DAYS}d")

# COMMAND ----------

# MAGIC %md ## 1. Pricing rates from public pricing JSON
# MAGIC - Pulls DBU/hour rate from the databricks published pricing documents for each cloud provider
# MAGIC - Total Cost is calculated as: `Total Cost (USD) = Total DBUs * SKU Price`
# MAGIC - Total DBUs are calculated as: `Total DBUs = DBU/hour * hours of runtime`

# COMMAND ----------

import json
import os
import urllib.request

# Prefer pre-staged JSON in config/ for environments that can't reach databricks.com
# outbound (air-gapped customer subnets). Run `_fetch_pricing.py` to refresh those files.
# Falls back to the live URL when no local file exists.
_pricing_local = os.path.abspath(
    os.path.join(os.path.dirname("/Workspace" + _ctx.notebookPath().get()),
                 "..", "config", f"pricing_{CLOUD}.json")
)
if os.path.exists(_pricing_local):
    print(f"Loading pricing from local file: {_pricing_local}")
    with open(_pricing_local) as _f:
        raw = json.load(_f)
else:
    PRICING_URL = f"https://www.databricks.com/en-pricing-assets/data/pricing/{CLOUD}.json"
    print(f"No local pricing file at {_pricing_local} — fetching {PRICING_URL}")
    with urllib.request.urlopen(PRICING_URL, timeout=30) as resp:
        raw = json.loads(resp.read())

print(f"Total pricing records: {len(raw)}")

# Keep only all-purpose (Photon + non-Photon). DBU/hr per instance is plan-independent,
# so filter to a single plan tier to avoid duplicate rows; $/DBU gets overridden later
# from system.billing.account_prices anyway.
# NOTE: Photon rows in the JSON have `(Photon)` baked into `instance` — e.g.
# "r5d.12xlarge (Photon)". The Clusters API returns the clean "r5d.12xlarge" regardless
# of runtime engine, so strip the suffix before storing or lookup will always miss.
import re
_PHOTON_SUFFIX = re.compile(r"\s*\(Photon\)\s*$")

keep_workloads = {"All-Purpose Compute", "All-Purpose Compute Photon"}
filtered = []
for r in raw:
    if r.get("compute") not in keep_workloads or r.get("plan") != "Premium":
        continue
    r = dict(r)
    r["instance"] = _PHOTON_SUFFIX.sub("", r["instance"])
    filtered.append(r)
print(f"After filter to all-purpose: {len(filtered)} rows")

# AWS Fleet instance types (e.g. `rd-fleet.xlarge`) are Databricks-side abstractions
# that map to multiple underlying EC2 families for availability. The public pricing
# JSON only lists concrete instance types, so fleet names never resolve on their own.
# Synthesize fleet aliases inheriting rates from the canonical base family.
#
# Mapping lives in ../config/fleet_aliases.py — plain Python dict, edit there.
import os, runpy
_notebook_fs_dir = os.path.dirname("/Workspace" + _ctx.notebookPath().get())
_fleet_alias_path = os.path.abspath(os.path.join(_notebook_fs_dir, "..", "config", "fleet_aliases.py"))
FLEET_BASE_MAP = runpy.run_path(_fleet_alias_path).get("FLEET_BASE_MAP", {})
print(f"Loaded fleet alias map from {_fleet_alias_path}: {len(FLEET_BASE_MAP)} entries")

fleet_rows = []
for fleet_prefix, base_prefix in FLEET_BASE_MAP.items():
    for r in list(filtered):
        inst = r["instance"]
        if inst.startswith(base_prefix + "."):
            size = inst.split(".", 1)[1]
            fleet_rows.append({**r, "instance": f"{fleet_prefix}.{size}"})
filtered.extend(fleet_rows)
print(f"After fleet-alias expansion: {len(filtered)} rows (added {len(fleet_rows)})")

# Load excluded clusters config — plain Python module with three list constants.
import runpy
_excluded_path = os.path.abspath(os.path.join(_notebook_fs_dir, "..", "config", "excluded_clusters.py"))
_excluded_module = runpy.run_path(_excluded_path)
EXCLUDED_IDS = set(_excluded_module.get("CLUSTER_IDS", []))
EXCLUDED_NAME_PATTERNS = list(_excluded_module.get("CLUSTER_NAME_PATTERNS", []))
EXCLUDED_CREATORS = set(_excluded_module.get("CREATORS", []))
print(f"Loaded exclusions from {_excluded_path}: "
      f"{len(EXCLUDED_IDS)} IDs, {len(EXCLUDED_NAME_PATTERNS)} name patterns, "
      f"{len(EXCLUDED_CREATORS)} creators")

# Resolve name-pattern + creator exclusion rules to cluster_ids. Important: bound
# the resolution to clusters that ACTUALLY billed under all-purpose during the
# lookback window. Without this bound, a single service-account creator can resolve
# to millions of historical cluster_ids — far too many to feed into a SQL `NOT IN`
# filter and almost all irrelevant to the current run.
EXCLUDED_RESOLVED_IDS = set(EXCLUDED_IDS)
if EXCLUDED_NAME_PATTERNS or EXCLUDED_CREATORS:
    _where_parts = []
    if EXCLUDED_NAME_PATTERNS:
        _where_parts.append(" OR ".join(
            f"c.cluster_name LIKE '{p.replace('*','%')}'" for p in EXCLUDED_NAME_PATTERNS
        ))
    if EXCLUDED_CREATORS:
        _creators_sql = ", ".join(f"'{x}'" for x in EXCLUDED_CREATORS)
        _where_parts.append(f"c.owned_by IN ({_creators_sql})")
    _resolve_sql = f"""
    WITH active_clusters AS (
      SELECT DISTINCT usage_metadata.cluster_id AS cluster_id
      FROM system.billing.usage
      WHERE workspace_id = {WORKSPACE_ID}
        AND billing_origin_product = 'ALL_PURPOSE'
        AND sku_name NOT ILIKE '%SERVERLESS%'
        AND usage_metadata.cluster_id IS NOT NULL
        AND usage_date >= current_date() - INTERVAL {LOOKBACK_DAYS} DAYS
        AND usage_date <  current_date()
    )
    SELECT DISTINCT c.cluster_id
    FROM system.compute.clusters c
    JOIN active_clusters a ON a.cluster_id = c.cluster_id
    WHERE c.workspace_id = {WORKSPACE_ID}
      AND ({" OR ".join(_where_parts)})
    """
    EXCLUDED_RESOLVED_IDS.update(r["cluster_id"] for r in spark.sql(_resolve_sql).collect())
print(f"Resolved {len(EXCLUDED_RESOLVED_IDS)} cluster_ids to exclude (bounded to lookback window)")

# COMMAND ----------

# MAGIC %md ## 2. Historical billing data (single read of system tables)
# MAGIC
# MAGIC Build a cached temp view `dbx_priced_usage__` from `system.billing.usage` joined to
# MAGIC `system.billing.account_prices` over the lookback window. Exclusions were already
# MAGIC resolved to a cluster_id set above, so the filter here is a simple `NOT IN`.

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS dbx_priced_usage__")

# COMMAND ----------

# Build a SQL filter expression for excluded clusters. Patterns and creators were
# already resolved against the API into EXCLUDED_RESOLVED_IDS — single SQL filter.
_exclusion_filter = ""
if EXCLUDED_RESOLVED_IDS:
    _ids_sql = ", ".join(f"'{x}'" for x in EXCLUDED_RESOLVED_IDS)
    _exclusion_filter = f" AND u.usage_metadata.cluster_id NOT IN ({_ids_sql})"

# `CACHE TABLE ... AS SELECT ...` creates a session-scoped cached temp view — the join
# runs once here and both downstream queries hit the cache instead of re-scanning the
# system tables. The cache is explicitly released at the end of the notebook.
#
# `dbx_priced_usage__` is namespaced + suffixed to avoid colliding with anything in the
# customer's catalog or session.
spark.sql(f"""
CACHE TABLE dbx_priced_usage__ AS
SELECT
  u.workspace_id,
  u.usage_date,
  u.usage_start_time,
  u.usage_metadata.cluster_id AS cluster_id,
  u.sku_name,
  CASE WHEN u.sku_name ILIKE '%PHOTON%' THEN true ELSE false END AS photon,
  u.usage_quantity AS dbus,
  lp.pricing.default AS dollars_per_dbu,
  u.usage_quantity * lp.pricing.default AS dollars
FROM system.billing.usage u
JOIN system.billing.account_prices lp
  ON lp.sku_name = u.sku_name
 AND u.usage_start_time >= lp.price_start_time
 AND u.usage_start_time <  COALESCE(lp.price_end_time, TIMESTAMP '9999-01-01')
WHERE u.workspace_id = {WORKSPACE_ID}
  AND u.billing_origin_product = 'ALL_PURPOSE'
  AND u.sku_name NOT ILIKE '%SERVERLESS%'
  AND u.usage_metadata.cluster_id IS NOT NULL
  AND u.usage_date >= current_date() - INTERVAL {LOOKBACK_DAYS} DAYS
  AND u.usage_date <  current_date()
  {_exclusion_filter}
""")
print(f"Cached dbx_priced_usage__: {spark.table('dbx_priced_usage__').count()} rows "
      f"({len(EXCLUDED_RESOLVED_IDS)} cluster_ids excluded via filter)")

# COMMAND ----------

# MAGIC %md ### 2a. Customer SKUs + $/DBU (read from `dbx_priced_usage__`)
# MAGIC
# MAGIC Pull distinct SKUs and their pricing rates

# COMMAND ----------

customer_rates = {
    r["photon"]: {"sku_name": r["sku_name"], "dollars_per_dbu": float(r["dollars_per_dbu"])}
    for r in spark.sql("SELECT DISTINCT sku_name, photon, dollars_per_dbu FROM dbx_priced_usage__").collect()
}

if not customer_rates:
    raise RuntimeError(
        f"No all-purpose SKU usage found for workspace_id={WORKSPACE_ID} in the last {LOOKBACK_DAYS} days. "
        "Run the workspace against some all-purpose compute first, then re-run baseline_refresh."
    )

for photon, info in customer_rates.items():
    print(f"  photon={photon}: ${info['dollars_per_dbu']:.4f}/DBU  sku={info['sku_name']}")

# COMMAND ----------

# MAGIC %md ### 2b. Workspace baseline (read from `dbx_priced_usage__`)
# MAGIC
# MAGIC Daily $/cluster aggregated, then population stats over the lookback window.
# MAGIC
# MAGIC Daily $/cluster is heavily right-skewed (most clusters cost $20-$200/day, a handful
# MAGIC cost $5k+). Z-scores on raw dollars are dominated by the long tail. We compute stats
# MAGIC in **log space** as well — `mean_log_daily_dollars` and `stddev_log_daily_dollars` —
# MAGIC and `01_monitor` uses those for the z-score calculation. The raw-dollar columns
# MAGIC stick around for human reading.

# COMMAND ----------

baseline_df = spark.sql(f"""
WITH cluster_day AS (
  -- Weekday-only baseline: usage drops sharply on weekends, which would widen the
  -- distribution and dampen weekday z-scores. We score weekend clusters against the
  -- weekday baseline anyway — that's slightly aggressive on weekends, which is
  -- generally desired (any unusual weekend activity is worth flagging).
  SELECT workspace_id, usage_date, cluster_id, SUM(dollars) AS daily_dollars
  FROM dbx_priced_usage__
  WHERE WEEKDAY(usage_date) < 5    -- 0=Mon ... 4=Fri
  GROUP BY ALL
)
SELECT
  {WORKSPACE_ID} AS workspace_id,
  current_timestamp() AS computed_at,
  {LOOKBACK_DAYS} AS lookback_days,
  COUNT(*) AS n_cluster_days,
  -- Raw-dollar stats (for human reading; not used for scoring)
  AVG(daily_dollars) AS mean_daily_dollars,
  STDDEV_POP(daily_dollars) AS stddev_daily_dollars,
  PERCENTILE(daily_dollars, 0.50) AS p50_daily_dollars,
  PERCENTILE(daily_dollars, 0.90) AS p90_daily_dollars,
  PERCENTILE(daily_dollars, 0.95) AS p95_daily_dollars,
  PERCENTILE(daily_dollars, 0.99) AS p99_daily_dollars,
  MAX(daily_dollars) AS max_daily_dollars,
  AVG(LN(daily_dollars))         AS mean_log_daily_dollars,
  STDDEV_POP(LN(daily_dollars))  AS stddev_log_daily_dollars
FROM cluster_day
WHERE daily_dollars > 0
""")
baseline_df.show(truncate=False)

(baseline_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_SCHEMA}.workspace_baseline"))

# COMMAND ----------

# MAGIC %md ## 3. Write `pricing_rates`
# MAGIC
# MAGIC Combines the filtered pricing-JSON rows with the customer's `$/DBU` resolved in step 2a.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp

rows = []
for r in filtered:
    photon = r["compute"].endswith("Photon")
    info = customer_rates.get(photon)
    if info is None:
        # Workspace has never used this photon bucket; skip — monitor will flag these clusters UNKNOWN.
        continue
    dbu_hr = float(r["dburate"])
    rows.append(Row(
        cloud=CLOUD,
        node_type=r["instance"],
        workload=r["compute"],
        photon=photon,
        sku_name=info["sku_name"],
        dbu_per_hour=dbu_hr,
        dollars_per_dbu=info["dollars_per_dbu"],
        dollars_per_hour=dbu_hr * info["dollars_per_dbu"],
    ))
pricing_df = spark.createDataFrame(rows).withColumn("refreshed_at", current_timestamp())

(pricing_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_SCHEMA}.pricing_rates"))

print(f"Wrote {pricing_df.count()} rows to {CATALOG_SCHEMA}.pricing_rates")

# COMMAND ----------

# MAGIC %md ## 4. Ensure `cluster_scores` table exists (first run)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.cluster_scores (
  workspace_id BIGINT,
  snapshot_ts TIMESTAMP,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  cluster_id STRING,
  cluster_name STRING,
  creator STRING,
  cluster_source STRING,
  node_type STRING,
  driver_node_type STRING,
  num_workers INT,
  photon BOOLEAN,
  sku_name STRING,
  dbu_per_hour DOUBLE,
  dollars_per_hour DOUBLE,
  projected_daily_dollars DOUBLE,
  baseline_mean DOUBLE,
  baseline_stddev DOUBLE,
  z_score DOUBLE,
  signal STRING,
  reason STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")
print(f"Table {CATALOG_SCHEMA}.cluster_scores ensured")

# COMMAND ----------

# MAGIC %md ## 5. `alert_send_log` table (suppression state for `03_slack_alerting`)
# MAGIC
# MAGIC Append-only audit of Slack messages sent by the router. The router reads this
# MAGIC table to apply per-(cluster, severity) suppression windows so the same owner
# MAGIC doesn't get re-pinged about the same cluster on every 30-min run. Schema is
# MAGIC intentionally narrow: enough to dedupe + audit, not a full alert history.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.alert_send_log (
  sent_at      TIMESTAMP,
  workspace_id BIGINT,
  cluster_id   STRING,
  signal       STRING,                 -- WARNING | CRITICAL
  recipient    STRING,                 -- Slack user ID or channel ID actually messaged
  routing      STRING,                 -- 'dm' | 'fallback_channel' | 'channel_only'
  message_ts   STRING                  -- Slack message ts, for follow-up updates
)
USING DELTA
""")
print(f"Table {CATALOG_SCHEMA}.alert_send_log ensured")

# COMMAND ----------

# MAGIC %md ## 6. `cluster_alerts_latest` view (depends on cluster_scores existing)
# MAGIC
# MAGIC Most recent snapshot per cluster where signal is `WARNING` or `CRITICAL`,
# MAGIC filtered to the last 30 minutes. The recency window auto-resolves alerts:
# MAGIC `01_monitor` only writes rows for `RUNNING`/`RESIZING` clusters, so when a
# MAGIC flagged cluster is terminated its last alert snapshot ages out of this view
# MAGIC instead of persisting forever. Without it, a router wired to this view would
# MAGIC keep firing on long-since-killed clusters.
# MAGIC
# MAGIC Side effect (intentional): if `01_monitor` is broken or backed up for >30 min,
# MAGIC the view goes empty. Pair with a separate "monitor heartbeat" alert to catch that.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.cluster_alerts_latest AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY snapshot_ts DESC) AS rn
  FROM {CATALOG_SCHEMA}.cluster_scores
)
SELECT
  workspace_id,
  snapshot_ts,
  cluster_id,
  cluster_name,
  creator,
  node_type,
  num_workers,
  photon,
  sku_name,
  dbu_per_hour,
  dollars_per_hour,
  projected_daily_dollars,
  z_score,
  signal,
  reason
FROM ranked
WHERE rn = 1
  AND signal IN ('WARNING', 'CRITICAL')
  AND snapshot_ts >= current_timestamp() - INTERVAL 30 MINUTES
""")
print(f"View {CATALOG_SCHEMA}.cluster_alerts_latest refreshed")

# COMMAND ----------

# MAGIC %md ## 7. Release + drop the `dbx_priced_usage__` cache
# MAGIC
# MAGIC We're done with it. Uncache to free executor memory, then drop the temp view so
# MAGIC nothing references it after this notebook exits — important on a long-lived
# MAGIC all-purpose cluster where session state can otherwise linger.

# COMMAND ----------

spark.sql("UNCACHE TABLE IF EXISTS dbx_priced_usage__")
spark.sql("DROP VIEW IF EXISTS dbx_priced_usage__")
print("Released and dropped dbx_priced_usage__")
