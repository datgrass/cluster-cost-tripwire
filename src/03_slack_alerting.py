# Databricks notebook source
# MAGIC %md
# MAGIC # 03_slack_alerting
# MAGIC
# MAGIC **Notebook Summary:** Reads `cluster_alerts_latest`, applies per-(cluster, severity) suppression against `alert_send_log`, and sends a Slack message for each remaining flagged cluster. Two routing modes:
# MAGIC
# MAGIC - **`channel_only`** *(default during testing)* — every alert posts to a single configurable channel. Safe to run before owner-DM behavior has been validated; doesn't surprise end users.
# MAGIC - **`dm_with_fallback`** — DM the cluster owner via `users.lookupByEmail`; fall back to the same channel if the email doesn't match a Slack user.
# MAGIC
# MAGIC Runs every 30 minutes. Pairs with `01_monitor` (15 min): worst-case detection-to-DM latency is ~30 min. Re-nudge cadence is governed by suppression windows, not the schedule.
# MAGIC
# MAGIC Depends on `00_baseline_refresh` having run at least once (creates `alert_send_log` and the `cluster_alerts_latest` view).
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `main` | UC catalog where the tables live. Must match `00_baseline_refresh`. |
# MAGIC | `schema` | `cluster_early_warning` | UC schema under `catalog`. Must match `00_baseline_refresh`. |
# MAGIC | `secret_scope` | `databricks-cost-alerts` | Databricks secret scope holding the Slack bot token. |
# MAGIC | `secret_key` | `slack-bot-token` | Key inside the scope holding the `xoxb-...` token. |
# MAGIC | `routing_mode` | `channel_only` | `channel_only` posts every alert to `alert_channel`. `dm_with_fallback` DMs cluster owners and falls back to `alert_channel`. |
# MAGIC | `alert_channel` | *(empty — must be set)* | Channel name (`#name`) or ID (`C0...`). Primary destination in `channel_only` mode; fallback in `dm_with_fallback` mode. Bot must be invited unless it has `chat:write.public`. |
# MAGIC | `warning_suppression_hours` | `24` | Hours to suppress repeat WARNING messages for the same cluster. |
# MAGIC | `critical_suppression_hours` | `4` | Hours to suppress repeat CRITICAL messages for the same cluster. |
# MAGIC | `workspace_host` | *(empty — auto-detect)* | Workspace host (e.g. `mycompany.cloud.databricks.com`) used to build cluster deep-link URLs. Leave empty to auto-detect from runtime context; set explicitly when auto-detection is wrong (PrivateLink, custom domains) or when you want a specific host. |
# MAGIC
# MAGIC ## Required bot scopes
# MAGIC
# MAGIC `chat:write`, `users:read`, `users:read.email`, `channels:read`, `groups:read`, `im:read`, `mpim:read`. Optionally `chat:write.public` to post to channels without invite.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "cluster_early_warning")
dbutils.widgets.text("secret_scope", "databricks-cost-alerts")
dbutils.widgets.text("secret_key", "slack-bot-token")
dbutils.widgets.dropdown("routing_mode", "channel_only", ["channel_only", "dm_with_fallback"])
dbutils.widgets.text("alert_channel", "")
dbutils.widgets.text("warning_suppression_hours", "24")
dbutils.widgets.text("critical_suppression_hours", "4")
dbutils.widgets.text("workspace_host", "")

CATALOG        = dbutils.widgets.get("catalog")
SCHEMA         = dbutils.widgets.get("schema")
SCOPE          = dbutils.widgets.get("secret_scope")
KEY            = dbutils.widgets.get("secret_key")
ROUTING_MODE   = dbutils.widgets.get("routing_mode")
ALERT_CHANNEL  = dbutils.widgets.get("alert_channel").strip()
WARN_HOURS     = int(dbutils.widgets.get("warning_suppression_hours"))
CRIT_HOURS     = int(dbutils.widgets.get("critical_suppression_hours"))
HOST_OVERRIDE  = dbutils.widgets.get("workspace_host").strip()

if not ALERT_CHANNEL:
    raise RuntimeError(
        "alert_channel widget is empty — set it to the channel name (#cost-alerts) "
        "or ID (C0...) where alerts should land."
    )

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"
ALERTS_TABLE   = f"{CATALOG_SCHEMA}.cluster_alerts_latest"
BASELINE_TABLE = f"{CATALOG_SCHEMA}.workspace_baseline"
SEND_LOG_TABLE = f"{CATALOG_SCHEMA}.alert_send_log"

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_ID = int(_ctx.workspaceId().get())

# Resolve the workspace host used to build cluster deep-link URLs in alerts.
# Priority: explicit `workspace_host` widget > browserHostName (PrivateLink-correct
# but only populated in interactive context) > apiUrl as last resort.
from urllib.parse import urlparse
if HOST_OVERRIDE:
    # Strip scheme if user pasted a full URL.
    WORKSPACE_HOST = urlparse(HOST_OVERRIDE).netloc if HOST_OVERRIDE.startswith("http") else HOST_OVERRIDE
else:
    try:
        WORKSPACE_HOST = _ctx.browserHostName().get()
    except Exception:
        WORKSPACE_HOST = None
    if not WORKSPACE_HOST:
        WORKSPACE_HOST = urlparse(_ctx.apiUrl().get()).netloc

print(
    f"workspace_id={WORKSPACE_ID}, host={WORKSPACE_HOST}, mode={ROUTING_MODE}, "
    f"channel={ALERT_CHANNEL}, warn_suppression={WARN_HOURS}h, crit_suppression={CRIT_HOURS}h"
)

# COMMAND ----------

# MAGIC %md ## Slack helpers

# COMMAND ----------

import re
import requests

TOKEN = dbutils.secrets.get(scope=SCOPE, key=KEY)
_AUTH_HEADER = {"Authorization": f"Bearer {TOKEN}"}
_JSON_HEADERS = {**_AUTH_HEADER, "Content-Type": "application/json; charset=utf-8"}

class SlackError(RuntimeError):
    def __init__(self, method, body):
        self.method = method
        self.body = body
        super().__init__(f"Slack API {method} failed: {body.get('error')} (full: {body})")

def slack(method: str, payload: dict) -> dict:
    """For methods that accept JSON bodies (chat.postMessage, auth.test, etc.)."""
    r = requests.post(f"https://slack.com/api/{method}", headers=_JSON_HEADERS, json=payload, timeout=10)
    r.raise_for_status()
    body = r.json()
    if not body.get("ok"):
        raise SlackError(method, body)
    return body

def slack_form(method: str, params: dict) -> dict:
    """For legacy methods that require form-encoded bodies (users.lookupByEmail)."""
    r = requests.post(f"https://slack.com/api/{method}", headers=_AUTH_HEADER, data=params, timeout=10)
    r.raise_for_status()
    body = r.json()
    if not body.get("ok"):
        raise SlackError(method, body)
    return body

def lookup_user_id(email: str):
    """Resolve an email to a Slack user ID. Returns None on `users_not_found` / missing email."""
    if not email or "@" not in email:
        return None
    try:
        return slack_form("users.lookupByEmail", {"email": email})["user"]["id"]
    except SlackError as e:
        if e.body.get("error") == "users_not_found":
            return None
        raise

# Verify the token early so a misconfigured scope/secret fails loud instead of mid-loop.
auth = slack("auth.test", {})
print(f"Authed as bot: {auth['user']} in workspace: {auth['team']}  (bot_id={auth.get('bot_id')})")

# COMMAND ----------

# MAGIC %md ## Message rendering

# COMMAND ----------

_USER_ID_RE = re.compile(r"^U[A-Z0-9]{6,}$")

def is_user_id(s: str) -> bool:
    return bool(s) and bool(_USER_ID_RE.match(s))

def format_dollars(x):
    return f"${x:,.0f}" if x is not None else "?"

def build_cost_alert_blocks(
    recipient,
    cluster_name,
    cluster_id,
    formatted_dollars,
    formatted_percent,
    severity,
    creator,
    creator_user_id=None,
):
    """Block Kit blocks for a single-cluster cost alert.

    `recipient` is a Slack user ID (DM mode) or channel ID/name (channel mode).
    `creator_user_id` (optional) is the owner's Slack user ID for @-mentioning
    them in channel mode. Falls back to the email in inline code when None.
    """
    severity_emoji = ":rotating_light:" if severity == "CRITICAL" else ":warning:"

    if is_user_id(recipient):
        greeting = f"Hi <@{recipient}>, your"
    else:
        # Channel mode — @-mention the owner so they get a real Slack notification.
        # Falls back to inline code when we couldn't resolve the email to a user.
        if creator_user_id:
            owner_tag = f"<@{creator_user_id}>"
        elif creator:
            owner_tag = f"`{creator}`"
        else:
            owner_tag = "_unknown owner_"
        greeting = f"Hi team — cluster owned by {owner_tag}: their"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{severity_emoji} Cluster Cost Alert ({severity})"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{greeting} all-purpose compute cluster *{cluster_name}* "
                    f"is currently projected to cost *{formatted_dollars}/day*, "
                    f"which is *{formatted_percent}* greater than the workspace average."
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "This is well outside the norm and may indicate an oversized "
                    "or long-running cluster. Please review the cluster "
                    "configuration and right-size or terminate if appropriate. "
                    "If you think this is a validly sized workload, reach out to "
                    "your workspace admins to get this cluster exempted from notifications."
                ),
            },
        },
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Review cluster in Databricks"},
                "url": f"https://{WORKSPACE_HOST}/compute/clusters/{cluster_id}",
                # Slack only accepts "primary" or "danger"; omit the key for default styling.
                # Always use "primary" — the button is the call-to-action regardless of tier.
                "style": "primary",
            }],
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    "Sent by `cluster-cost-tripwire` — reach out to your Databricks "
                    "account team or workspace admins if you need help right-sizing."
                ),
            }],
        },
    ]
    return blocks

# COMMAND ----------

# MAGIC %md ## Read alerts + apply suppression

# COMMAND ----------

# Per-(cluster, severity) suppression: skip rows where the most recent send for
# that exact (cluster_id, signal) is younger than the tier's suppression window.
# `WORKSPACE_ID` filter keeps multi-workspace deploys (future) clean.
suppression_sql = f"""
WITH last_send AS (
  SELECT
    cluster_id,
    signal,
    MAX(sent_at) AS last_sent_at
  FROM {SEND_LOG_TABLE}
  WHERE workspace_id = {WORKSPACE_ID}
    AND signal IN ('WARNING', 'CRITICAL')
  GROUP BY cluster_id, signal
)
SELECT a.*
FROM {ALERTS_TABLE} a
LEFT JOIN last_send ls
  ON ls.cluster_id = a.cluster_id
 AND ls.signal     = a.signal
WHERE a.workspace_id = {WORKSPACE_ID}
  AND (
    ls.last_sent_at IS NULL
    OR (a.signal = 'CRITICAL' AND ls.last_sent_at < current_timestamp() - INTERVAL {CRIT_HOURS} HOURS)
    OR (a.signal = 'WARNING'  AND ls.last_sent_at < current_timestamp() - INTERVAL {WARN_HOURS} HOURS)
  )
"""
to_send = spark.sql(suppression_sql).collect()
print(
    f"{len(to_send)} cluster row(s) to alert on after suppression "
    f"(WARNING={WARN_HOURS}h, CRITICAL={CRIT_HOURS}h)"
)

# COMMAND ----------

# MAGIC %md ## Read workspace baseline mean (for "% above average" framing)

# COMMAND ----------

# Most recent baseline row, not "today's" — `00_baseline_refresh` runs at 2 AM and
# we don't want this notebook to crash on early-morning runs that haven't seen a
# fresh baseline yet, or if the baseline job failed.
baseline_rows = spark.sql(
    f"SELECT mean_daily_dollars FROM {BASELINE_TABLE} ORDER BY computed_at DESC LIMIT 1"
).collect()
if not baseline_rows or baseline_rows[0]["mean_daily_dollars"] is None:
    print(
        f"WARNING: {BASELINE_TABLE} has no rows yet — '% above average' will show '?' "
        "until 00_baseline_refresh runs successfully."
    )
    BASELINE_MEAN = None
else:
    BASELINE_MEAN = float(baseline_rows[0]["mean_daily_dollars"])
    print(f"baseline mean: ${BASELINE_MEAN:,.2f}/day")

# COMMAND ----------

# MAGIC %md ## Send

# COMMAND ----------

from datetime import datetime, timezone

now = datetime.now(timezone.utc)
send_log_rows = []

for row in to_send:
    cluster_id   = row["cluster_id"]
    cluster_name = row["cluster_name"]
    creator      = (row["creator"] or "").strip() or None
    projected    = row["projected_daily_dollars"]
    severity     = row["signal"]

    formatted_dollars = format_dollars(projected)
    if BASELINE_MEAN and BASELINE_MEAN > 0 and projected:
        pct = (projected - BASELINE_MEAN) / BASELINE_MEAN * 100.0
        formatted_percent = f"{pct:,.0f}%"
    else:
        formatted_percent = "?"

    # Resolve owner's Slack ID once. Used for routing in dm_with_fallback AND for
    # @-mentioning the owner in channel mode so they get a real notification.
    creator_user_id = lookup_user_id(creator) if creator else None

    # Resolve recipient + routing tag based on mode.
    if ROUTING_MODE == "dm_with_fallback":
        if creator_user_id:
            recipient = creator_user_id
            routing = "dm"
        else:
            recipient = ALERT_CHANNEL
            routing = "fallback_channel"
            if creator:
                print(f"  {creator} → no Slack match, routing {cluster_name} to {ALERT_CHANNEL}")
            else:
                print(f"  (unowned cluster) → routing {cluster_name} to {ALERT_CHANNEL}")
    else:  # channel_only
        recipient = ALERT_CHANNEL
        routing = "channel_only"

    blocks = build_cost_alert_blocks(
        recipient=recipient,
        cluster_name=cluster_name,
        cluster_id=cluster_id,
        formatted_dollars=formatted_dollars,
        formatted_percent=formatted_percent,
        severity=severity,
        creator=creator,
        creator_user_id=creator_user_id,
    )

    plain = (
        f"{severity}: cluster {cluster_name} projected at "
        f"{formatted_dollars}/day ({formatted_percent} above average)"
    )
    resp = slack("chat.postMessage", {
        "channel": recipient,
        "text": plain,
        "blocks": blocks,
        "unfurl_links": False,
        "unfurl_media": False,
    })
    print(f"  → posted {severity} alert for {cluster_name} via {routing} (channel={resp['channel']}, ts={resp['ts']})")

    send_log_rows.append({
        "sent_at": now,
        "workspace_id": WORKSPACE_ID,
        "cluster_id": cluster_id,
        "signal": severity,
        "recipient": resp["channel"],
        "routing": routing,
        "message_ts": resp["ts"],
    })

# COMMAND ----------

# MAGIC %md ## Persist sends to `alert_send_log`

# COMMAND ----------

if send_log_rows:
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType, LongType
    )
    log_schema = StructType([
        StructField("sent_at", TimestampType()),
        StructField("workspace_id", LongType()),
        StructField("cluster_id", StringType()),
        StructField("signal", StringType()),
        StructField("recipient", StringType()),
        StructField("routing", StringType()),
        StructField("message_ts", StringType()),
    ])
    log_df = spark.createDataFrame(send_log_rows, schema=log_schema)
    log_df.write.mode("append").saveAsTable(SEND_LOG_TABLE)
    print(f"Logged {len(send_log_rows)} send(s) to {SEND_LOG_TABLE}")
else:
    print("No sends to log.")
