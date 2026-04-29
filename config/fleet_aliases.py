"""
AWS Fleet instance type aliases.

AWS Fleet instance types (e.g. `rd-fleet.xlarge`) are Databricks-side abstractions
that map to a pool of underlying EC2 families for availability. The public
Databricks pricing JSON only lists concrete instance types (like `r5d.xlarge`),
so fleet names never resolve against pricing on their own.

`00_baseline_refresh` uses this dict to synthesize a pricing row for each
`<fleet>.<size>` combo, inheriting rates from the canonical base family.

Format: `{ "<fleet-prefix>": "<canonical-base-family>" }`
For each entry, every row in the pricing JSON whose instance starts with
`<base>.` (e.g. `r5d.4xlarge`) is cloned into a synthetic row named
`<fleet>.<size>` (e.g. `rd-fleet.4xlarge`) with identical DBU/hr rates.

Adding a new fleet family (e.g. when AWS ships one):
  1. Find the canonical base family — look at Databricks pricing JSON `dburate`
     for candidate concrete types and match specs (CPU, memory, disk profile).
  2. Add one entry below, with a comment noting the workload profile.
  3. `databricks bundle deploy` and re-run `00_baseline_refresh`.
"""

FLEET_BASE_MAP = {
    "rd-fleet": "r5d",    # memory-optimized with local NVMe SSD
    "r-fleet":  "r5",     # memory-optimized
    "md-fleet": "m5d",    # general-purpose with local NVMe SSD
    "m-fleet":  "m5",     # general-purpose
    "c-fleet":  "c5",     # compute-optimized
    "cd-fleet": "c5d",    # compute-optimized with local NVMe SSD
    "i-fleet":  "i3",     # storage-optimized (local NVMe heavy)

    # Examples for future use (commented out until needed):
    # "r7i-fleet": "r7i",   # memory-optimized, 7th-gen Intel
    # "m7g-fleet": "m7g",   # general-purpose Graviton
    # "i4i-fleet": "i4i",   # storage-optimized, 4th-gen Intel
}
