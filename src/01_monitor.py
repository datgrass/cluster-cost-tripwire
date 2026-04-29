# Databricks notebook source
# MAGIC %md
# MAGIC # 01_monitor
# MAGIC
# MAGIC **Notebook Summary:** Early-warning sweep of running all-purpose clusters. Every 15 minutes it polls the Clusters API, projects each cluster's daily `$` burn from its live config, and flags anything that's either a statistical outlier for this workspace or over an absolute `$`/day budget — catching runaway spend *before* `system.billing.usage` latency would let it accrue.
# MAGIC
# MAGIC Runs every 15 minutes. For each running classic all-purpose cluster:
# MAGIC - Compute synthetic `DBU/hr` and `$/hr` from the live Clusters API config × `pricing_rates`.
# MAGIC - Project daily `$` (× 24h).
# MAGIC - Score vs. `workspace_baseline` (z-score) and the absolute budget threshold.
# MAGIC - Append a row to `cluster_scores`.
# MAGIC
# MAGIC Depends on `00_baseline_refresh` having run at least once.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `main` | UC catalog where the tables live. Must match `00_baseline_refresh`. |
# MAGIC | `schema` | `cluster_early_warning` | UC schema under `catalog`. Must match `00_baseline_refresh`. |
# MAGIC | `budget_daily_dollars` | `5000` | Absolute backstop in USD. Any cluster projected above this flips to **CRITICAL** regardless of z-score — catches "big in absolute dollars" even if the workspace's population is also big. |
# MAGIC | `z_warning` | `2.0` | Z-score threshold for **WARNING** tier. A cluster projected more than this many standard deviations above the workspace's historical mean-daily-`$/cluster` fires a warning. |
# MAGIC | `z_critical` | `3.0` | Z-score threshold for **CRITICAL** tier. Higher bar than `z_warning`; also triggered unconditionally by `budget_daily_dollars` breach. |
# MAGIC
# MAGIC The cloud provider (AWS / Azure / GCP) is auto-detected from the workspace host URL — no parameter needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "cluster_early_warning")
dbutils.widgets.text("budget_daily_dollars", "5000")
dbutils.widgets.text("z_warning", "2.0")
dbutils.widgets.text("z_critical", "3.0")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BUDGET = float(dbutils.widgets.get("budget_daily_dollars"))
Z_WARN = float(dbutils.widgets.get("z_warning"))
Z_CRIT = float(dbutils.widgets.get("z_critical"))

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_ID = int(_ctx.workspaceId().get())

# Auto-detect cloud from the workspace host URL. Casing matches pricing_rates.cloud.
_host = _ctx.apiUrl().get().lower()
if "azuredatabricks.net" in _host:
    CLOUD = "Azure"
elif "gcp.databricks.com" in _host:
    CLOUD = "GCP"
elif ".databricks.com" in _host:
    CLOUD = "AWS"
else:
    raise RuntimeError(f"Could not determine cloud from workspace host: {_host}")

print(f"workspace_id={WORKSPACE_ID}, cloud={CLOUD} (auto-detected), budget=${BUDGET:,.0f}/day, z_warn={Z_WARN}, z_crit={Z_CRIT}")

# COMMAND ----------

# MAGIC %md ## Load pricing lookup + baseline

# COMMAND ----------

pricing_rows = spark.table(f"{CATALOG_SCHEMA}.pricing_rates") \
    .where(f"cloud = '{CLOUD}'") \
    .collect()

pricing = {
    (r["node_type"], r["photon"]): (r["dbu_per_hour"], r["dollars_per_hour"], r["sku_name"])
    for r in pricing_rows
}
# Bucket-level SKU (same sku_name per photon flag) — used to tag UNKNOWN clusters too.
sku_by_photon = {r["photon"]: r["sku_name"] for r in pricing_rows}
print(f"Loaded {len(pricing)} pricing entries for {CLOUD}")
for photon, sku in sku_by_photon.items():
    print(f"  photon={photon}: sku={sku}")

baseline_row = spark.table(f"{CATALOG_SCHEMA}.workspace_baseline").collect()
if not baseline_row:
    raise RuntimeError(
        f"{CATALOG_SCHEMA}.workspace_baseline is empty — run baseline_refresh at least once before monitor."
    )
baseline = baseline_row[0].asDict()
# Raw-dollar stats — kept for human reading on the cluster_scores row, NOT used for scoring.
BASELINE_MEAN = float(baseline["mean_daily_dollars"] or 0.0)
BASELINE_STD = float(baseline["stddev_daily_dollars"] or 0.0)
# Log-space stats — these are what the z-score is computed against. Z-score is dollar-blind;
# `projected_daily_dollars`, `baseline_mean`, etc. continue to be reported in raw dollars.
BASELINE_MEAN_LOG = float(baseline["mean_log_daily_dollars"] or 0.0)
BASELINE_STD_LOG = float(baseline["stddev_log_daily_dollars"] or 0.0)
print(f"baseline: n={baseline['n_cluster_days']}, mean=${BASELINE_MEAN:,.2f}/day, "
      f"stddev=${BASELINE_STD:,.2f}, log-space μ={BASELINE_MEAN_LOG:.3f} σ={BASELINE_STD_LOG:.3f}")

# Load excluded-clusters config — plain Python module with three list constants.
import os, runpy
_excluded_path = os.path.abspath(os.path.join(
    os.path.dirname("/Workspace" + _ctx.notebookPath().get()), "..", "config", "excluded_clusters.py"))
_excluded_module = runpy.run_path(_excluded_path)
EXCLUDED_IDS = set(_excluded_module.get("CLUSTER_IDS", []))
EXCLUDED_NAME_PATTERNS = list(_excluded_module.get("CLUSTER_NAME_PATTERNS", []))
EXCLUDED_CREATORS = set(_excluded_module.get("CREATORS", []))
print(f"Loaded exclusions: {len(EXCLUDED_IDS)} IDs, {len(EXCLUDED_NAME_PATTERNS)} name patterns, "
      f"{len(EXCLUDED_CREATORS)} creators")

import fnmatch
def exclusion_reason(cluster_id, cluster_name, creator):
    if cluster_id in EXCLUDED_IDS:
        return f"cluster_id {cluster_id} in excluded_clusters.yml"
    for pat in EXCLUDED_NAME_PATTERNS:
        if cluster_name and fnmatch.fnmatch(cluster_name, pat):
            return f"cluster_name matches pattern '{pat}'"
    if creator in EXCLUDED_CREATORS:
        return f"creator {creator} in excluded_clusters.yml"
    return None

# COMMAND ----------

# MAGIC %md ## Enumerate live all-purpose clusters

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    State, ClusterSource, RuntimeEngine, ListClustersFilterBy
)

w = WorkspaceClient()

# Push state + source filtering to the API so we don't pull every terminated cluster
# in the workspace's history. In a busy workspace that's orders of magnitude fewer
# rows returned.
live = list(w.clusters.list(
    filter_by=ListClustersFilterBy(
        cluster_states=[State.RUNNING, State.RESIZING],
        cluster_sources=[ClusterSource.UI, ClusterSource.API],
    ),
    page_size=100,
))
print(f"Running classic all-purpose clusters: {len(live)}")

# COMMAND ----------

# MAGIC %md ## Score each cluster

# COMMAND ----------

from datetime import datetime, timezone

def live_worker_count(c):
    if c.executors:
        return len(c.executors)
    if c.num_workers is not None:
        return c.num_workers
    if c.autoscale:
        return c.autoscale.min_workers or 0
    return 0

def is_photon(c):
    if c.runtime_engine == RuntimeEngine.PHOTON:
        return True
    return bool(c.spark_version and "photon" in c.spark_version.lower())

def lookup_rates(node_type, photon):
    hit = pricing.get((node_type, photon))
    if hit:
        return hit
    # Fallback: try the opposite photon flag (pricing table coverage gaps).
    hit = pricing.get((node_type, not photon))
    if hit:
        return hit
    return (None, None, None)

import math

now = datetime.now(timezone.utc)
scored = []

for c in live:
    driver_type = c.driver_node_type_id or c.node_type_id
    worker_type = c.node_type_id
    workers = live_worker_count(c)
    photon = is_photon(c)
    sku = sku_by_photon.get(photon)

    drv_dbu, drv_dph, _ = lookup_rates(driver_type, photon)
    wkr_dbu, wkr_dph, _ = lookup_rates(worker_type, photon)

    excl = exclusion_reason(c.cluster_id, c.cluster_name, c.creator_user_name)
    if excl:
        # Excluded clusters skip pricing/scoring but still get a row written for auditability —
        # makes it easy to answer "why didn't this fire?".
        dbu_per_hr = None
        dollars_per_hr = None
        projected = None
        z = None
        signal = "EXCLUDED"
        reason = excl
    elif drv_dbu is None or wkr_dbu is None:
        dbu_per_hr = None
        dollars_per_hr = None
        projected = None
        z = None
        signal = "UNKNOWN"
        reason = f"Missing pricing for driver={driver_type} worker={worker_type} photon={photon}"
    else:
        dbu_per_hr = drv_dbu + workers * wkr_dbu
        dollars_per_hr = drv_dph + workers * wkr_dph
        projected = dollars_per_hr * 24.0
        # Z-score is computed in log space (workspace baseline distribution is heavily right-skewed,
        # so log-space stats are more meaningful). projected is reported in raw $.
        z = ((math.log(projected) - BASELINE_MEAN_LOG) / BASELINE_STD_LOG
             if (BASELINE_STD_LOG > 0 and projected > 0) else None)

        reasons = []
        if projected > BUDGET:
            reasons.append(f"projected_daily ${projected:,.0f} > budget ${BUDGET:,.0f}")
        if z is not None and z > Z_CRIT:
            reasons.append(f"log-z={z:.1f} > critical {Z_CRIT}")
        elif z is not None and z > Z_WARN:
            reasons.append(f"log-z={z:.1f} > warning {Z_WARN}")

        if projected > BUDGET or (z is not None and z > Z_CRIT):
            signal = "CRITICAL"
        elif z is not None and z > Z_WARN:
            signal = "WARNING"
        else:
            signal = "OK"
        reason = "; ".join(reasons) if reasons else "within thresholds"

    scored.append({
        "workspace_id": WORKSPACE_ID,
        "snapshot_ts": now,
        "cluster_id": c.cluster_id,
        "cluster_name": c.cluster_name,
        "creator": c.creator_user_name,
        "cluster_source": c.cluster_source.value if c.cluster_source else None,
        "node_type": worker_type,
        "driver_node_type": driver_type,
        "num_workers": workers,
        "photon": photon,
        "sku_name": sku,
        "dbu_per_hour": dbu_per_hr,
        "dollars_per_hour": dollars_per_hr,
        "projected_daily_dollars": projected,
        "baseline_mean": BASELINE_MEAN,
        "baseline_stddev": BASELINE_STD,
        "z_score": z,
        "signal": signal,
        "reason": reason,
    })

if not scored:
    print("No running clusters — nothing to write.")
    dbutils.notebook.exit("no-op")

# COMMAND ----------

# MAGIC %md ## Append to `cluster_scores`

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    IntegerType, TimestampType, LongType
)

schema = StructType([
    StructField("workspace_id", LongType()),
    StructField("snapshot_ts", TimestampType()),
    StructField("cluster_id", StringType()),
    StructField("cluster_name", StringType()),
    StructField("creator", StringType()),
    StructField("cluster_source", StringType()),
    StructField("node_type", StringType()),
    StructField("driver_node_type", StringType()),
    StructField("num_workers", IntegerType()),
    StructField("photon", BooleanType()),
    StructField("sku_name", StringType()),
    StructField("dbu_per_hour", DoubleType()),
    StructField("dollars_per_hour", DoubleType()),
    StructField("projected_daily_dollars", DoubleType()),
    StructField("baseline_mean", DoubleType()),
    StructField("baseline_stddev", DoubleType()),
    StructField("z_score", DoubleType()),
    StructField("signal", StringType()),
    StructField("reason", StringType()),
])

df = spark.createDataFrame(scored, schema=schema)
df.write.mode("append").saveAsTable(f"{CATALOG_SCHEMA}.cluster_scores")

summary = (df.groupBy("signal").count().orderBy("signal").collect())
for row in summary:
    print(f"  {row['signal']:10s} {row['count']}")
print(f"Wrote {len(scored)} rows to {CATALOG_SCHEMA}.cluster_scores @ {now.isoformat()}")