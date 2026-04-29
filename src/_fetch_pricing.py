# Databricks notebook source
# MAGIC %md
# MAGIC # _fetch_pricing
# MAGIC
# MAGIC **Notebook Summary:** Pre-stages the public Databricks pricing JSONs into the bundle's
# MAGIC `config/` directory for environments that can't reach `databricks.com` outbound
# MAGIC (air-gapped customer subnets). Run this in a workspace that *does* have internet
# MAGIC access then export the
# MAGIC updated `config/` directory to your local repo and commit so the JSONs ship with
# MAGIC the bundle.
# MAGIC
# MAGIC `00_baseline_refresh` reads from these files first, falling back to the live URL
# MAGIC only if the file is absent. Once the JSONs are committed, no outbound network
# MAGIC call is needed at deploy or run time on the customer side.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `clouds` | `AWS,Azure,GCP` | Comma-separated list of clouds to fetch. Pulling all three is cheap (a few MB) and means the bundle is portable across cloud customers. |
# MAGIC
# MAGIC ## Workflow after running
# MAGIC
# MAGIC 1. Run this notebook. JSONs land at `<bundle>/files/config/pricing_<CLOUD>.json` in the workspace.
# MAGIC 2. From your laptop, export that config directory:
# MAGIC    ```
# MAGIC    databricks workspace export-dir \
# MAGIC      /Workspace/Users/<you>/<bundle-root>/files/config \
# MAGIC      <local-repo>/config --overwrite
# MAGIC    ```
# MAGIC 3. `git add config/pricing_*.json && git commit -m "refresh pricing" && git push`
# MAGIC 4. Customer pulls the repo, redeploys the bundle, runs `00_baseline_refresh` — it reads the local JSONs.

# COMMAND ----------

dbutils.widgets.text("clouds", "AWS,Azure,GCP")
CLOUDS = [c.strip() for c in dbutils.widgets.get("clouds").split(",") if c.strip()]

import json
import os
import urllib.request

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_notebook_fs_dir = os.path.dirname("/Workspace" + _ctx.notebookPath().get())
_config_dir = os.path.abspath(os.path.join(_notebook_fs_dir, "..", "config"))
os.makedirs(_config_dir, exist_ok=True)

print(f"Writing to: {_config_dir}\n")

for cloud in CLOUDS:
    url = f"https://www.databricks.com/en-pricing-assets/data/pricing/{cloud}.json"
    print(f"Fetching {url}")
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = resp.read()
    out_path = os.path.join(_config_dir, f"pricing_{cloud}.json")
    with open(out_path, "wb") as f:
        f.write(raw)
    record_count = len(json.loads(raw))
    print(f"  → {out_path} ({len(raw):,} bytes, {record_count} records)\n")

print("Files now present in config/:")
for f in sorted(os.listdir(_config_dir)):
    p = os.path.join(_config_dir, f)
    print(f"  {f}  ({os.path.getsize(p):,} bytes)")