# Databricks notebook source
# MAGIC %md
# MAGIC # 02_backtest
# MAGIC
# MAGIC **Notebook Summary:** Validates the monitor's logic against actual history. Replays the last N days of billing data hour-by-hour as if the monitor had been running in real time, then reports catch rate and lead-time distribution against known-bad cluster-days. Use this to tune thresholds (`budget_daily_dollars`, `z_warning`, `z_critical`) *before* going live.
# MAGIC
# MAGIC Replay `system.billing.usage` at hourly granularity to answer:
# MAGIC **"For each cluster-day that actually cost more than the budget, at what hour would the monitor have fired?"**
# MAGIC
# MAGIC Uses a rolling daily baseline (each simulated day is scored against stats built only from days *before* it, so results reflect what the monitor would have known in real time).
# MAGIC Writes per-cluster-day outcomes to `backtest_results` for ad-hoc analysis (catch rate, lead-time distribution, missed spend) — query the table directly however suits the engagement.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `main` | UC catalog where results land. Must match `00_baseline_refresh`. |
# MAGIC | `schema` | `cluster_early_warning` | UC schema under `catalog`. Must match `00_baseline_refresh`. |
# MAGIC | `budget_daily_dollars` | `5000` | Same semantics as in `01_monitor`. Also defines the ground-truth "bad cluster-day" — any cluster-day that actually cost above this is what we're trying to have caught. |
# MAGIC | `z_warning` | `2.0` | Z-score threshold applied during simulated scoring — matches `01_monitor`. |
# MAGIC | `z_critical` | `3.0` | Z-score threshold applied during simulated scoring — matches `01_monitor`. |
# MAGIC | `backtest_lookback_days` | `90` | How far back in `system.billing.usage` to replay. Wider window = more cases, longer runtime. |
# MAGIC | `baseline_lookback_days` | `30` | Size of the rolling baseline window used when scoring each simulated day — must match `00_baseline_refresh` for an apples-to-apples replay. |

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "cluster_early_warning")
dbutils.widgets.text("budget_daily_dollars", "5000")
dbutils.widgets.text("z_warning", "2.0")
dbutils.widgets.text("z_critical", "3.0")
dbutils.widgets.text("backtest_lookback_days", "90")
dbutils.widgets.text("baseline_lookback_days", "30")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BUDGET = float(dbutils.widgets.get("budget_daily_dollars"))
Z_WARN = float(dbutils.widgets.get("z_warning"))
Z_CRIT = float(dbutils.widgets.get("z_critical"))
LOOKBACK = int(dbutils.widgets.get("backtest_lookback_days"))
BASELINE_LOOKBACK = int(dbutils.widgets.get("baseline_lookback_days"))

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_ID = int(_ctx.workspaceId().get())
print(f"workspace_id={WORKSPACE_ID}, lookback={LOOKBACK}d, baseline_lookback={BASELINE_LOOKBACK}d, "
      f"budget=${BUDGET:,.0f}, z_warn={Z_WARN}, z_crit={Z_CRIT}")

# Load the same exclusions config 00 + 01 use, so the backtest's catch-rate metrics
# aren't polluted by clusters we'd never have alerted on in production.
import os, runpy
_excluded_path = os.path.abspath(os.path.join(
    os.path.dirname("/Workspace" + _ctx.notebookPath().get()), "..", "config", "excluded_clusters.py"))
_excluded_module = runpy.run_path(_excluded_path)
EXCLUDED_IDS = set(_excluded_module.get("CLUSTER_IDS", []))
EXCLUDED_NAME_PATTERNS = list(_excluded_module.get("CLUSTER_NAME_PATTERNS", []))
EXCLUDED_CREATORS = set(_excluded_module.get("CREATORS", []))
print(f"Loaded exclusions: {len(EXCLUDED_IDS)} IDs, {len(EXCLUDED_NAME_PATTERNS)} name patterns, "
      f"{len(EXCLUDED_CREATORS)} creators")

# Resolve name-pattern + creator rules to cluster_ids, bounded to clusters that
# actually billed under all-purpose during the backtest lookback window. Without
# the bound, a single service-account creator can resolve to millions of historical
# cluster_ids that would never appear in the simulation anyway.
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
        AND usage_date >= current_date() - INTERVAL {LOOKBACK} DAYS
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

_exclusion_filter = ""
if EXCLUDED_RESOLVED_IDS:
    _ids_sql = ", ".join(f"'{x}'" for x in EXCLUDED_RESOLVED_IDS)
    _exclusion_filter = f" AND u.usage_metadata.cluster_id NOT IN ({_ids_sql})"

# COMMAND ----------

# MAGIC %md ## 1. Build hourly cluster usage (dollars) for the lookback window

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW hourly_spend AS
WITH classic_ap AS (
  SELECT
    u.workspace_id,
    u.usage_date,
    u.usage_start_time,
    date_trunc('HOUR', u.usage_start_time) AS hour_ts,
    HOUR(u.usage_start_time) AS hour_of_day,
    u.usage_metadata.cluster_id AS cluster_id,
    u.usage_quantity AS dbus,
    u.sku_name
  FROM system.billing.usage u
  WHERE u.workspace_id = {WORKSPACE_ID}
    AND u.billing_origin_product = 'ALL_PURPOSE'
    AND u.sku_name NOT ILIKE '%SERVERLESS%'
    AND u.usage_metadata.cluster_id IS NOT NULL
    AND u.usage_date >= current_date() - INTERVAL {LOOKBACK} DAYS
    AND u.usage_date <  current_date()
    {_exclusion_filter}
),
priced AS (
  SELECT
    c.workspace_id,
    c.usage_date,
    c.hour_ts,
    c.hour_of_day,
    c.cluster_id,
    c.dbus,
    c.dbus * COALESCE(lp.pricing.default, 0) AS dollars
  FROM classic_ap c
  LEFT JOIN system.billing.account_prices lp
    ON lp.sku_name = c.sku_name
   AND c.usage_start_time >= lp.price_start_time
   AND c.usage_start_time <  COALESCE(lp.price_end_time, TIMESTAMP '9999-01-01')
)
SELECT workspace_id, usage_date, hour_ts, hour_of_day, cluster_id,
       SUM(dbus) AS dbus, SUM(dollars) AS dollars
FROM priced
GROUP BY ALL
""")

# COMMAND ----------

# MAGIC %md ## 2. Rolling daily baseline (so each day uses only prior days' stats)
# MAGIC
# MAGIC For realism: at the time a cluster was running on day D, the monitor would have compared against a baseline built from days D-N through D-1 — not data including day D itself.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cluster_day_totals AS
-- Weekday-only baseline: matches 00_baseline_refresh.workspace_baseline, which
-- excludes weekends so the weekend drop doesn't widen the distribution and
-- dampen weekday z-scores. The simulation itself (hourly_spend, bad_cluster_days,
-- the per-hour scoring) still processes all days — only the rolling baseline
-- that each simulated day is scored against is weekday-only.
SELECT workspace_id, usage_date, cluster_id, SUM(dollars) AS daily_dollars
FROM hourly_spend
WHERE WEEKDAY(usage_date) < 5    -- 0=Mon ... 4=Fri
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rolling_baseline AS
WITH days AS (
  SELECT DISTINCT usage_date FROM cluster_day_totals
)
SELECT
  d.usage_date AS as_of_date,
  -- Raw-dollar stats kept for human reading on the results table
  AVG(t.daily_dollars) AS baseline_mean,
  STDDEV_POP(t.daily_dollars) AS baseline_stddev,
  -- Log-space stats — these are what the simulated z-score is computed against,
  -- matching what 01_monitor uses in production.
  AVG(LN(t.daily_dollars))         AS baseline_mean_log,
  STDDEV_POP(LN(t.daily_dollars))  AS baseline_stddev_log,
  COUNT(*) AS baseline_n
FROM days d
JOIN cluster_day_totals t
  ON t.usage_date BETWEEN d.usage_date - INTERVAL {BASELINE_LOOKBACK} DAYS
                      AND d.usage_date - INTERVAL 1 DAY
WHERE t.daily_dollars > 0
GROUP BY d.usage_date
""")

# COMMAND ----------

# MAGIC %md ## 3. For each cluster-day, walk forward hour by hour and score

# COMMAND ----------

sim_sql = f"""
WITH bad_cluster_days AS (
  -- Ground truth: the cluster-days we want to have caught.
  SELECT workspace_id, usage_date, cluster_id, SUM(dollars) AS actual_daily_dollars
  FROM hourly_spend
  GROUP BY ALL
  HAVING SUM(dollars) > {BUDGET}
),
hourly_cumulative AS (
  SELECT
    h.workspace_id,
    h.usage_date,
    h.cluster_id,
    h.hour_of_day,
    h.hour_ts,
    SUM(h.dollars) OVER (
      PARTITION BY h.workspace_id, h.usage_date, h.cluster_id
      ORDER BY h.hour_of_day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_dollars,
    SUM(h.dbus) OVER (
      PARTITION BY h.workspace_id, h.usage_date, h.cluster_id
      ORDER BY h.hour_of_day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_dbus,
    ROW_NUMBER() OVER (
      PARTITION BY h.workspace_id, h.usage_date, h.cluster_id
      ORDER BY h.hour_of_day
    ) AS hours_observed
  FROM hourly_spend h
),
simulated AS (
  SELECT
    hc.*,
    rb.baseline_mean,
    rb.baseline_stddev,
    rb.baseline_mean_log,
    rb.baseline_stddev_log,
    rb.baseline_n,
    -- Extrapolate: avg $/hr so far × 24
    (hc.cumulative_dollars / hc.hours_observed) * 24.0 AS projected_daily_dollars,
    -- Z-score in log space, matching 01_monitor's production logic. Projected stays in raw $.
    CASE
      WHEN rb.baseline_stddev_log IS NULL OR rb.baseline_stddev_log = 0 THEN NULL
      WHEN (hc.cumulative_dollars / hc.hours_observed) * 24.0 <= 0 THEN NULL
      ELSE (LN((hc.cumulative_dollars / hc.hours_observed) * 24.0) - rb.baseline_mean_log)
           / rb.baseline_stddev_log
    END AS z_score
  FROM hourly_cumulative hc
  LEFT JOIN rolling_baseline rb ON rb.as_of_date = hc.usage_date
),
tiered AS (
  SELECT
    s.*,
    CASE
      WHEN s.projected_daily_dollars > {BUDGET} THEN 'CRITICAL'
      WHEN s.z_score > {Z_CRIT} THEN 'CRITICAL'
      WHEN s.z_score > {Z_WARN} THEN 'WARNING'
      ELSE 'OK'
    END AS sim_signal
  FROM simulated s
),
first_fire AS (
  SELECT
    workspace_id, usage_date, cluster_id, 
    MAX(z_score) AS z_score,
    MIN(CASE WHEN sim_signal IN ('CRITICAL','WARNING') THEN hour_of_day END) AS first_alert_hour,
    MIN(CASE WHEN sim_signal = 'CRITICAL'              THEN hour_of_day END) AS first_critical_hour,
    MIN(CASE WHEN cumulative_dollars > {BUDGET}        THEN hour_of_day END) AS budget_crossed_hour,
    MAX(hour_of_day) AS last_active_hour
  FROM tiered
  GROUP BY ALL
)
SELECT
  b.workspace_id,
  b.usage_date,
  b.cluster_id,
  b.actual_daily_dollars,
  f.z_score,
  f.first_alert_hour,
  f.first_critical_hour,
  f.budget_crossed_hour,
  f.last_active_hour,
  CASE
    WHEN f.first_alert_hour IS NULL THEN 'MISS'
    WHEN f.budget_crossed_hour IS NULL THEN 'CAUGHT_EARLY'  -- budget never crossed but we still fired (shouldn't happen for bad cluster-days, but defensive)
    WHEN f.first_alert_hour < f.budget_crossed_hour THEN 'CAUGHT_EARLY'
    WHEN f.first_alert_hour = f.budget_crossed_hour THEN 'CAUGHT_AT_CROSSING'
    ELSE 'CAUGHT_LATE'
  END AS outcome,
  CASE
    WHEN f.first_alert_hour IS NOT NULL AND f.budget_crossed_hour IS NOT NULL
      THEN f.budget_crossed_hour - f.first_alert_hour
    ELSE NULL
  END AS lead_hours
FROM bad_cluster_days b
LEFT JOIN first_fire f
  ON f.workspace_id = b.workspace_id
 AND f.usage_date   = b.usage_date
 AND f.cluster_id   = b.cluster_id
"""

# Run the simulation, register as a temp view, then enrich with cluster_name + creator
# via a single late-stage join to system.compute.clusters. backtest_results is a small
# per-cluster-day table; doing this join at the very end keeps the heavy aggregations
# above metadata-free.
spark.sql(sim_sql).createOrReplaceTempView("backtest_results_raw")

results_df = spark.sql(f"""
WITH cluster_meta AS (
  SELECT cluster_id, cluster_name, owned_by AS creator
  FROM system.compute.clusters
  WHERE workspace_id = {WORKSPACE_ID}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
)
SELECT
  br.workspace_id,
  br.usage_date,
  br.cluster_id,
  cm.cluster_name,
  cm.creator,
  br.actual_daily_dollars,
  br.z_score,
  br.first_alert_hour,
  br.first_critical_hour,
  br.budget_crossed_hour,
  br.last_active_hour,
  br.outcome,
  br.lead_hours
FROM backtest_results_raw br
LEFT JOIN cluster_meta cm ON cm.cluster_id = br.cluster_id
""")
(results_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_SCHEMA}.backtest_results"))

print(f"Wrote {results_df.count()} bad-cluster-day rows to {CATALOG_SCHEMA}.backtest_results")
