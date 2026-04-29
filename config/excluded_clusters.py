"""
Clusters excluded from baseline statistics AND alert scoring.

Edit the lists below to filter out known-valid large clusters that would either
skew the workspace distribution (inflating mean/stddev so smaller outliers stop
being flagged) or generate noise alerts (e.g. always-on production ETL).

Three filter types — matching any single rule excludes the cluster from
baseline, monitor, and backtest. Excluded clusters in the live monitor still
get a row written to `cluster_scores` with signal='EXCLUDED' and the matching
rule named in `reason`, so the exclusion is auditable rather than silent.
"""

# Exact cluster_id strings (most precise)
CLUSTER_IDS = [
    # "0123-456789-abcdefgh",   # example: shared-prod-etl — known valid 40-worker prod ETL
]

# Glob patterns matched against cluster_name (e.g. "prod-etl-*", "shared-bi-*")
CLUSTER_NAME_PATTERNS = [
    # "prod-etl-*",
    # "shared-bi-*",
]

# Exact match on creator email (cluster owner)
CREATORS = [
    # "service-prod@customer.com",
]
