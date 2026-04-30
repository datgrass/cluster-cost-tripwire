# cluster-cost-tripwire

Advanced-warning system for expensive classic all-purpose Databricks clusters. Polls the live Clusters API every 15 min, synthesizes each cluster's projected daily $ from the public pricing JSON, compares against a workspace-wide historical baseline, and posts Slack alerts (channel-wide today, per-owner DMs once validated) for anything outside the norm.

## What gets created

Under `${catalog}.${schema}` (defaults `main.cluster_early_warning`):

| Object | What it is |
|---|---|
| `pricing_rates` | Snapshot of all-purpose DBU rates pulled from `databricks.com/en-pricing-assets/data/pricing/*.json`. Refreshed daily. |
| `workspace_baseline` | One row of population stats (mean / stddev / percentiles) on daily $/cluster over the last 30 days. |
| `cluster_scores` | Append-only. One row per (cluster_id, 15-min snapshot) with projected daily $, z-score, and `signal` ∈ {`OK`, `WARNING`, `CRITICAL`, `UNKNOWN`, `EXCLUDED`}. |
| `cluster_alerts_latest` (view) | Most recent row per cluster **where signal is WARNING or CRITICAL** AND snapshot is within the last 30 minutes. The query target the alert router reads. |
| `alert_send_log` | Append-only audit of Slack messages sent by the router. Backs the per-(cluster, severity) suppression window. |
| `backtest_results` | Output of the backtest notebook — one row per historical bad cluster-day with outcome & lead time. |

## Jobs

| Job | Trigger | What it does |
|---|---|---|
| `baseline_refresh` | daily 2 AM | Rebuild `pricing_rates` + `workspace_baseline`; ensure `cluster_scores`, `alert_send_log`, and `cluster_alerts_latest` exist. |
| `monitor` | every 15 min | List running all-purpose clusters, score each, append to `cluster_scores`. |
| `route_alerts` | every 30 min (PAUSED until validated) | Read `cluster_alerts_latest`, apply per-(cluster, severity) suppression, post Slack alerts. See [Slack alerting](#slack-alerting). |
| `backtest` | manual | Replay last 90d of `system.billing.usage` hour-by-hour, write per-cluster-day rows to `backtest_results` for ad-hoc query. |
| `teardown` | manual | Drops the schema and everything in it. Safety lock: `confirm` widget must be set to `YES` or it no-ops. Does not delete bundle jobs — use `databricks bundle destroy` for that. |

## Deploying to a new workspace

1. **Install + authenticate the Databricks CLI** against the target workspace. Either:
   ```bash
   databricks auth login --host https://<workspace>.cloud.databricks.com
   ```
   or set up a profile in `~/.databrickscfg`.

2. **Review `databricks.yml` and edit the variable defaults** to fit the customer:
   - `catalog` + `schema` — where tables land
   - `budget_daily_dollars` — absolute $/day per cluster alert threshold (default `5000`)
   - `z_warning` / `z_critical` — statistical thresholds (default `2.0` / `3.0`)
   - `baseline_lookback_days` — how much history feeds the workspace baseline (default `30`)
   - `notifications_email` — job-failure emails (default empty — disabled)
   - `slack_secret_scope` / `slack_secret_key` — where the Slack bot token lives (default `databricks-cost-alerts` / `slack-bot-token`)
   - `slack_routing_mode` — `channel_only` (default during testing) or `dm_with_fallback`
   - `slack_alert_channel` — channel name (`#cost-alerts`) or ID (`C0...`); **must be set** before `route_alerts` runs
   - `warning_suppression_hours` / `critical_suppression_hours` — re-nudge cadence per tier (default `24` / `4`)

3. **Review `resources/jobs.yml` → `job_clusters` block** (anchored on the first job). Adjust `node_type_id`, `driver_node_type_id`, and `aws_attributes` / `azure_attributes` / `gcp_attributes` to match the target cloud. The default mirrors a single-node AWS `r5d.2xlarge` Photon cluster.

4. **Deploy**:
   ```bash
   cd cluster-cost-tripwire
   DATABRICKS_TF_EXEC_PATH=$(which terraform) \
   DATABRICKS_TF_VERSION=$(terraform --version | head -1 | awk '{print $2}' | tr -d v) \
   DATABRICKS_TF_CLI_CONFIG_FILE=/dev/null \
     databricks bundle deploy -t dev --profile=<your-profile>
   ```
   (TF env vars work around an expired PGP key in the embedded TF — omit if not on a machine that hits it.)

5. **Run the jobs manually in order** from the workspace Jobs UI:
   1. **`baseline_refresh`** — creates pricing + baseline + scores + alert_send_log + view.
   2. **`monitor`** — scores currently-running clusters against the baseline.
   3. **`backtest`** (may take a few minutes) — replays history, writes outcomes to `backtest_results`.
   4. **`route_alerts`** — sends Slack messages for flagged clusters. Requires Slack token + scopes set up first ([Slack alerting](#slack-alerting)).

   `baseline_refresh` and `monitor` ship `UNPAUSED`. `route_alerts` ships `PAUSED` until you've validated end-to-end (token resolves, channel works, suppression behaves) — flip to `UNPAUSED` in `resources/jobs.yml` and redeploy.

## Config (DAB variables)

| Variable | Default | Notes |
|---|---|---|
| `catalog` | `main` | UC catalog to write to. |
| `schema` | `cluster_early_warning` | UC schema (auto-created). |
| `budget_daily_dollars` | `5000` | Absolute backstop. Any cluster projected above this flips to CRITICAL. |
| `z_warning` | `2.0` | Z-score threshold for WARNING tier. |
| `z_critical` | `3.0` | Z-score threshold for CRITICAL tier. |
| `baseline_lookback_days` | `30` | History window used for the baseline distribution. |
| `slack_secret_scope` | `databricks-cost-alerts` | Databricks secret scope holding the Slack bot token. |
| `slack_secret_key` | `slack-bot-token` | Key inside the scope holding the `xoxb-...` token. |
| `slack_routing_mode` | `channel_only` | `channel_only` posts every alert to `slack_alert_channel`. `dm_with_fallback` DMs cluster owners and falls back to the channel for unmatched emails. |
| `slack_alert_channel` | *(empty — must set)* | Channel name (`#name`) or ID (`C0...`). Required. Bot must be a member unless it has `chat:write.public`. |
| `warning_suppression_hours` | `24` | Hours to suppress repeat WARNING messages for the same cluster. |
| `critical_suppression_hours` | `4` | Hours to suppress repeat CRITICAL messages for the same cluster. |
| `slack_workspace_host` | *(empty — auto-detect)* | Workspace host (e.g. `mycompany.cloud.databricks.com`) used to build cluster deep-link URLs. Leave empty to auto-detect; set explicitly for PrivateLink / custom domains or when auto-detection is wrong. |

Override per deploy, e.g. `databricks bundle deploy -t dev --var="budget_daily_dollars=10000"`.

## Signal logic

At each monitor run, per live cluster:

```
dbu_per_hr = driver_dbu + num_workers * worker_dbu    # from pricing_rates
dollars_per_hr = driver_$/hr + num_workers * worker_$/hr
projected_daily_dollars = dollars_per_hr * 24
z_score = (projected_daily_dollars - baseline_mean) / baseline_stddev

signal = CRITICAL  if projected_daily_dollars > budget_daily_dollars
                   or z_score > z_critical
signal = WARNING   elif z_score > z_warning
signal = OK        else
```

Two independent signals — statistical catches "big for this workspace," budget catches "big in absolute dollars regardless of workspace norms."

## Testing-phase notes

- **Job-failure email notifications are commented out in `resources/jobs.yml`** — no emails will go out during testing. Uncomment the `email_notifications` blocks before prod.
- **`baseline_refresh` and `monitor` schedules are `UNPAUSED`**; **`route_alerts` is `PAUSED`** until you've validated end-to-end. Flip in `resources/jobs.yml` and redeploy.
- **The Slack bot token is read from a Databricks secret scope.** No token is provisioned by the bundle — see [Slack alerting](#slack-alerting) for setup.

## Slack alerting

`03_slack_alerting` reads `cluster_alerts_latest` every 30 minutes, applies per-(cluster, severity) suppression so the same cluster doesn't trigger a message every run, and posts via Slack. Two routing modes:

| Mode | Behavior | When to use |
|---|---|---|
| `channel_only` *(default)* | Every alert posts to `slack_alert_channel`. Owner email is shown in the message body. | Initial rollout — safe, doesn't surprise end users. Validate suppression + format before flipping the switch. |
| `dm_with_fallback` | DM each cluster owner via `users.lookupByEmail`. Owners with no Slack match (or service-principal-owned clusters) fall back to `slack_alert_channel`. | After the alert quality is validated — owners notice DMs more than channel mentions. |

Flip with `slack_routing_mode` in `databricks.yml` (or per-deploy `--var=slack_routing_mode=dm_with_fallback`).

### Suppression model

Each `(cluster_id, signal)` pair has its own suppression window:

| Severity | Default window | Why |
|---|---|---|
| WARNING | 24h | One nudge per day per cluster — owner has time to address; not catastrophic. |
| CRITICAL | 4h | Persistent pressure until the cluster is killed/right-sized. $5k+/day deserves it. |

Tunable via `warning_suppression_hours` / `critical_suppression_hours`. A cluster that escalates from WARNING to CRITICAL still fires a CRITICAL message immediately — windows are independent per tier.

### Provisioning the Slack bot

1. **Create or reuse a Slack app** in the customer's Slack workspace.
2. **Add Bot Token Scopes**:
   - `chat:write` — send messages
   - `users:read`, `users:read.email` — translate cluster owner emails to Slack user IDs (needed for `dm_with_fallback`)
   - `channels:read`, `groups:read`, `im:read`, `mpim:read` — resolve channel names to IDs
   - *(optional)* `chat:write.public` — post to public channels without `/invite`
3. **Install (or reinstall) the app to the workspace** so a token is issued covering the full scope set. Adding scopes to an already-installed app does not retroactively update existing tokens.
4. **Copy the Bot User OAuth Token** (`xoxb-...`) from the OAuth & Permissions page.
5. **Store the token in a Databricks secret scope:**
   ```bash
   databricks secrets create-scope databricks-cost-alerts
   databricks secrets put-secret databricks-cost-alerts slack-bot-token
   # paste xoxb-... into the editor
   ```
6. **Grant the job's run-as principal `READ` on the scope:**
   ```bash
   databricks secrets put-acl databricks-cost-alerts <run-as-sp-application-id> READ
   ```
7. **Invite the bot to `slack_alert_channel`** (unless the bot has `chat:write.public`):
   ```
   /invite @your-bot-name
   ```
8. **Trigger `route_alerts` manually once** to verify end-to-end before unpausing the schedule.

### Operational notes

- **Heartbeat alert**: `cluster_alerts_latest` is empty if `01_monitor` is broken or hasn't run in 30 minutes — which would silently mute alerts. Pair with a separate DBSQL Alert on `(SELECT max(snapshot_ts) FROM cluster_scores) < current_timestamp() - INTERVAL 30 MINUTES`.
- **Audit trail**: every message is logged in `alert_send_log` with `cluster_id`, `signal`, `recipient`, `routing` (`channel_only` / `dm` / `fallback_channel`), and Slack `message_ts`. Query that table to answer "who got pinged about cluster X" or "how often did this fire."
- **Owner ↔ Slack matching** *(only relevant in `dm_with_fallback`)*: the cluster's `creator_user_name` from the Clusters API is used as-is to call `users.lookupByEmail`. Matching only works if the owner's Databricks email matches their Slack email. Service-principal-owned clusters never match and route to the fallback channel.

## Source layout

```
cluster-cost-tripwire/
├── databricks.yml
├── resources/
│   └── jobs.yml
├── config/
│   ├── fleet_aliases.py         # AWS Fleet type → canonical EC2 family mapping (see below)
│   ├── excluded_clusters.py     # Cluster IDs / name patterns / creators to exclude from analysis
│   ├── pricing_AWS.json         # Pre-staged pricing snapshot (see "Air-gapped workspaces" below)
│   ├── pricing_Azure.json
│   └── pricing_GCP.json
├── src/
│   ├── 00_baseline_refresh.py   # run first — builds pricing + baseline + tables
│   ├── 01_monitor.py            # runs every 15 min — scores live clusters
│   ├── 02_backtest.py           # manual — replays history into backtest_results
│   ├── 03_slack_alerting.py     # runs every 30 min — posts Slack alerts
│   ├── 99_teardown.py           # manual — drops the schema (set `confirm=YES`)
│   └── _fetch_pricing.py        # utility — refreshes the pricing JSONs in config/
└── README.md
```

## Air-gapped customer workspaces (no `databricks.com` outbound)

If the target workspace's subnet blocks outbound `databricks.com`, `00_baseline_refresh` can't fetch the pricing JSON at runtime. To handle that, the bundle ships pricing snapshots in `config/pricing_<CLOUD>.json` and `00` reads them directly — no network call needed.

To **refresh** those snapshots when Databricks updates list pricing:

1. Run `_fetch_pricing` (utility notebook) in a workspace that *does* have internet (typically your own FE workspace, not the customer's). It writes `pricing_<CLOUD>.json` files into the bundle's `config/` directory.
2. From your laptop, pull the updated config dir down:
   ```bash
   databricks workspace export-dir \
     /Workspace/Users/<you>/<bundle-deploy-path>/files/config \
     ./config --overwrite --profile=<your-profile>
   ```
3. `git add config/pricing_*.json && git commit -m "refresh pricing snapshots" && git push`
4. Customer pulls the repo, redeploys the bundle, runs `00_baseline_refresh`. It loads the pricing from the local file with no outbound call.

If `config/pricing_<CLOUD>.json` is absent, `00` falls back to fetching from `databricks.com` — so workspaces *with* internet access don't need the snapshots committed.

## `config/excluded_clusters.py`

Customers often have known-large clusters that are valid by design (production ETL, shared BI warehouses, training jobs). Without filtering, these clusters do two unwanted things:

- **Skew the baseline**: their daily $ inflate the workspace mean and standard deviation, making everything else look "normal" and dropping detection sensitivity.
- **Generate noise alerts**: every monitor run flags them as outliers even though they're expected.

The config is a plain Python file with three list constants — edit it directly, no YAML parsing weirdness. All three pipeline notebooks import it via `runpy` and apply the filter consistently. Any single match excludes a cluster:

```python
CLUSTER_IDS = [
    "0123-456789-abcdefgh",   # exact match, most precise
]

CLUSTER_NAME_PATTERNS = [
    "prod-etl-*",             # glob against cluster_name
    "shared-bi-*",
]

CREATORS = [
    "service-prod@customer.com",   # exact match on cluster owner email
]
```

What happens at each layer:

- **`00_baseline_refresh`** — excluded cluster-days drop out of the workspace baseline, so their costs don't pull the mean/stddev up.
- **`01_monitor`** — excluded clusters get a row in `cluster_scores` with `signal='EXCLUDED'` and the matching rule named in `reason`. They never become alerts but stay auditable: `SELECT * FROM cluster_scores WHERE signal = 'EXCLUDED'` answers "what's being filtered and why."
- **`02_backtest`** — same exclusions apply, so the catch-rate numbers reflect the rules you'd actually run with in production.

## Z-score normalization

Daily $/cluster is heavily right-skewed: most clusters cost $20-$200/day, a handful cost thousands. Z-scores on raw dollars get dominated by the long tail and underweight smaller-but-still-anomalous clusters.

The bundle computes z-scores in **log space** (`LN(daily_dollars)`) instead. Concretely:

- `workspace_baseline` carries both raw-dollar stats (`mean_daily_dollars`, `stddev_daily_dollars`, percentiles) for human reading **and** log-space stats (`mean_log_daily_dollars`, `stddev_log_daily_dollars`).
- `01_monitor` and `02_backtest` use the log-space stats to compute `z = (LN(projected_daily_dollars) - mean_log) / stddev_log`.
- All dollar values written to tables (`projected_daily_dollars`, `baseline_mean`, `baseline_stddev`, `actual_daily_dollars`) remain in raw dollars — only the z-score's interpretation changes.

This makes `z_warning=2.0` and `z_critical=3.0` correspond to multiplicative outliers ("~7× and ~20× the workspace mean") rather than absolute-dollar outliers, which is generally what you want for a long-tail distribution.

### Weekday-only baseline

The baseline (and the rolling baseline used by the backtest) is built from **Monday–Friday cluster-days only**. Weekend usage typically drops sharply, which would widen the distribution and dampen weekday z-scores. The monitor scores weekend clusters against the weekday baseline anyway — slightly aggressive on weekends, which is generally desirable since unusual weekend activity is worth flagging. The simulation in `02_backtest` still walks every hour of every day; only the rolling baseline used to score each simulated day is weekday-only.

## `config/fleet_aliases.py`

AWS Fleet instance types (`rd-fleet.xlarge`, `m-fleet.2xlarge`, etc.) are Databricks-side abstractions that map to a pool of underlying EC2 families. The public pricing JSON only lists concrete instance types, so a fleet name never matches on its own — `00_baseline_refresh` synthesizes alias rows using this dict.

Plain Python module loaded via `runpy`. Format is `{ "<fleet-prefix>": "<canonical-base-family>" }`:

```python
FLEET_BASE_MAP = {
    "rd-fleet": "r5d",   # memory-optimized with local NVMe SSD
    "m-fleet":  "m5",    # general-purpose
}
```

For every entry, the baseline job takes each pricing row whose instance starts with `<base>.` (e.g. `r5d.4xlarge`) and produces a synthetic row for `<fleet>.<size>` (e.g. `rd-fleet.4xlarge`) carrying the same DBU/hr rate.

**Adding a new fleet family** (e.g. when AWS ships one):
1. Figure out the canonical base family Databricks prices it against. Start from the pricing JSON — look at `dburate` for the concrete types and match specs.
2. Add one entry to `FLEET_BASE_MAP` with a brief comment on the workload profile.
3. Redeploy (`databricks bundle deploy`) and re-run `00_baseline_refresh`.

The file's own docstring carries the same explanation for anyone who opens it directly.

## Design choices

- **Synthetic DBU rate from pricing JSON, customer $/DBU from `system.billing.account_prices`.** The JSON is authoritative per-node for DBU/hr; `account_prices` captures the customer's actual contracted $/DBU (including negotiated discounts). `baseline_refresh` overrides `dollars_per_dbu` in `pricing_rates` with the rate from SKUs this workspace actually bills against in the last 30d, falling back to plan-name match for brand-new workspaces. The public rate is kept in `public_dollars_per_dbu` for reference.
- **Workspace-wide baseline**, not per-cluster. The customer's problem is people spinning up *new* huge clusters, which by definition have no per-cluster history. A workspace-wide distribution catches absolute outliers on first snapshot.
- **Projection = current $/hr × 24**. Simple, conservative, and consistent with what the backtest replays. If a cluster ramps slow and then explodes, the projection will rise over successive snapshots and the signal will escalate.
- **`system.billing.usage` is only used for historical baseline + backtest** — never for real-time scoring. Its multi-hour latency is the whole reason this system exists.

## Limitations

- **Azure workspaces are not supported today.** The public Databricks pricing JSON (`Azure.json`) contains only serverless / FM-API / SAP rows — no classic all-purpose DBU rates. Deploying against an Azure workspace would leave `pricing_rates` empty and flip every cluster to `UNKNOWN`. Azure all-purpose pricing is published outside this CDN (Azure Marketplace listing). If we need Azure later, options are: hand-maintained `config/azure_rates.json` fallback, deriving DBU/hr from `system.billing.usage` over a long window, or scraping the Azure pricing page.
- **GCP `e2_*` underscore naming mismatch (minor).** A handful of GCP pricing rows use underscores (`e2_standard_4`) where the Clusters API returns hyphens (`e2-standard-4`). Same class of bug as the Photon suffix — lookup would miss for the ~14 affected instance types. Not fixed preemptively since no GCP workspace is in scope yet; trivial to normalize in `00_baseline_refresh` if it ever bites.

## Out of scope

- Jobs / DLT / serverless compute.
- Per-cluster historical baselines.
- Auto-terminate / kill-switch actions — system warns only.
- Multi-workspace fan-out is *structured for* (tables carry `workspace_id`, config can be list-ified) but not wired.
