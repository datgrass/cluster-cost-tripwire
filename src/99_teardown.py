# Databricks notebook source
# MAGIC %md
# MAGIC # 99_teardown
# MAGIC
# MAGIC **Notebook Summary:** Destructive cleanup. Drops the schema created by this bundle and everything in it — `pricing_rates`, `workspace_baseline`, `cluster_scores`, `cluster_alerts_latest`, `backtest_results`. Use this when you want to wipe history and start fresh (e.g. thresholds changed materially, or tearing down a demo).
# MAGIC
# MAGIC Does **not** delete the bundle's jobs — use `databricks bundle destroy -t <target>` for that.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `main` | Catalog containing the schema to drop. |
# MAGIC | `schema` | `cluster_early_warning` | Schema to drop with CASCADE (removes all tables + views inside). |
# MAGIC | `confirm` | *(empty)* | Safety lock. Must be set to the literal string `YES` before the drop fires. Any other value aborts with no change. |

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "cluster_early_warning")
dbutils.widgets.text("confirm", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
CONFIRM = dbutils.widgets.get("confirm")
CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md ## Preview what will be dropped

# COMMAND ----------

exists = spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE '{SCHEMA}'").count() > 0
if not exists:
    print(f"Schema {CATALOG_SCHEMA} does not exist — nothing to drop.")
    dbutils.notebook.exit("schema-absent")

print(f"Schema:  {CATALOG_SCHEMA}\n")

tables = spark.sql(f"SHOW TABLES IN {CATALOG_SCHEMA}").collect()
if tables:
    print("Objects that will be dropped (tables + views):")
    for t in tables:
        print(f"  - {CATALOG_SCHEMA}.{t['tableName']}")
else:
    print("(schema contains no tables or views)")

# COMMAND ----------

# MAGIC %md ## Drop

# COMMAND ----------

if CONFIRM != "YES":
    print(
        f"Safety lock engaged — `confirm` must be exactly 'YES' to drop {CATALOG_SCHEMA}.\n"
        f"Got: '{CONFIRM}'. No changes made."
    )
    dbutils.notebook.exit("not-confirmed")

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_SCHEMA} CASCADE")
print(f"Dropped schema {CATALOG_SCHEMA} (CASCADE).")
