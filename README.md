# peplink-monitor

Monitors the Peplink B-One router's WAN connections via SNMP (throughput,
link state) and the Peplink local REST API (health check state, failovers,
and per-WAN latency). Polls every 5 minutes, stores results in SQLite, and
provides a CLI for querying current readings, summaries, failover history,
and per-WAN latency.

## Project structure

```
peplink-monitor/
├── collector.py        # SNMP + API poller — run via cron
├── peplink_api.py      # Peplink local REST API client
├── cli.py              # CLI query tool
├── db.py               # All SQLite operations
├── config.yaml         # Local config (gitignored — contains secrets)
├── config.yaml.example # Template — copy this to config.yaml
├── requirements.txt
├── deploy.sh           # Deployment script to Mac Mini
└── README.md
```

## Prerequisites

- Python 3.14.0 via pyenv (both MacBook Air and Mac Mini)
- SNMP enabled on the Peplink B-One with a configured community string
- Peplink local REST API enabled (firmware 8+); client credentials created
  once via the API (see [Peplink REST API setup](#peplink-rest-api-setup))

## Installation

```bash
# Clone the repo
git clone <repo-url>
cd peplink-monitor

# Set Python version (already committed as .python-version)
pyenv local 3.14.0

# Install dependencies into pyenv's Python
~/.pyenv/versions/3.14.0/bin/pip install -r requirements.txt

# Create config from template
cp config.yaml.example config.yaml
# Edit config.yaml and set your community string
```

## Configuration

`config.yaml` (gitignored — never commit this file):

```yaml
host: YOUR_ROUTER_IP
community: YOUR_COMMUNITY_STRING
port: 161
poll_interval_seconds: 300
db_path: data/monitor.db
remote_host: YOUR_REMOTE_HOST   # used by ./cli.py --remote
remote_user: YOUR_REMOTE_USER

# Peplink REST API (for accurate WAN health check / failover tracking)
peplink_api_client_id: YOUR_CLIENT_ID
peplink_api_client_secret: YOUR_CLIENT_SECRET
peplink_api_verify_ssl: false   # router uses a self-signed cert
```

The `db_path` is expanded with `~` so it works unchanged on both machines.
If `peplink_api_client_id` / `peplink_api_client_secret` are omitted, the
collector runs in SNMP-only mode and logs a warning each poll.

## Running the collector manually

```bash
./collector.py
```

On the first run, the collector walks all interfaces via SNMP and caches
the mapping in SQLite. Subsequent runs use the cached OIDs. Output:

```
2025-01-15 14:00:00 INFO No cached interfaces — running discovery...
2025-01-15 14:00:01 INFO Discovered 6 interfaces: ['Eero (LAN 1)', 'LAN 2 (LAN 2)', 'LAN 3 (LAN 3)', 'LAN 4 (LAN 4)', 'Spectrum (WAN 1)', 'Starlink (WAN 2)']
2025-01-15 14:00:01 INFO Eero: first reading recorded
2025-01-15 14:00:01 INFO Spectrum: first reading recorded
2025-01-15 14:00:01 INFO Starlink: first reading recorded
2025-01-15 14:00:01 INFO Poll complete.
```

Run it a second time to get throughput deltas and latency samples:

```
2025-01-15 14:05:00 INFO Eero: in=45.23 Mbps  out=8.41 Mbps  status=up
2025-01-15 14:05:00 INFO Spectrum: in=12.80 Mbps  out=2.15 Mbps  status=up
2025-01-15 14:05:00 INFO Starlink: in=8.34 Mbps  out=1.07 Mbps  status=up
2025-01-15 14:05:03 INFO WAN latency: Spectrum  min=9.0 ms  avg=11.2 ms  max=28.0 ms  (30 samples)
2025-01-15 14:05:03 INFO WAN latency: Starlink  min=14.0 ms  avg=16.4 ms  max=22.0 ms  (30 samples)
2025-01-15 14:05:03 INFO Poll complete.
```

Each poll fetches per-WAN latency from the router's `GET /api/status.wan.latency`
endpoint, which measures both WANs simultaneously via the router's internal health
check engine regardless of which WAN any device happens to be using. This gives
equal, accurate samples for each WAN every poll cycle.

## CLI usage

All commands load `config.yaml` automatically. Global flags apply to
all subcommands:

- `--wan NAME` — filter output to a specific WAN interface by name
- `--remote` — run the command against the remote host configured in
  `config.yaml` (`remote_host`/`remote_user`). If the remote host
  resolves to the local machine, the flag is silently ignored so the
  same command works unchanged on both machines.
- `--show-all` — include interfaces that have never been up (e.g. unused
  LAN ports). By default these are hidden from all output.

### current / status — latest reading for all interfaces

`status` is an alias for `current`.

```bash
./cli.py status
./cli.py current
./cli.py --remote status           # query the Mini from MacBook Air
```

```
Interface    Label    Status     Message              In           Out         Uptime    Last Poll
-----------  -------  ---------  -------------------  -----------  ----------  --------  -----------
Eero         LAN 1    up         —                    45.23 Mbps   8.41 Mbps   —         2m ago
Spectrum     WAN 1    connected  IP: 1.2.3.4          12.80 Mbps   2.15 Mbps   5d 2h     2m ago
Starlink     WAN 2    connected  IP: 100.75.195.71     8.34 Mbps   1.07 Mbps   1h 29m    2m ago

WAN        Min        Avg        Max        Sampled
---------  ---------  ---------  ---------  ---------
Spectrum   9.0 ms     11.2 ms    28.0 ms    2m ago
Starlink   14.0 ms    16.4 ms    22.0 ms    2m ago
  (router-measured, not client ping)
```

When the Peplink API is configured, **Status** and **Message** come from the
router's health check engine (`statusLed`/`message` from
`GET /api/status.wan.connection`). **Uptime** is the router-reported connection
uptime. For interfaces without API health data (e.g. LAN ports), Status falls
back to SNMP `ifOperStatus`.

Filter to one WAN:

```bash
./cli.py --wan Starlink status
```

### summary — statistics over a time period

Periods: `1h`, `24h` (default), `7d`, `30d`

```bash
./cli.py summary --period 24h
./cli.py --remote summary --period 24h
```

```
Summary — last 24h

Interface    Label    Peak In      Peak Out     Avg In       Avg Out      Total In    Total Out      Failovers
-----------  -------  -----------  -----------  -----------  -----------  ----------  -----------  -----------
Eero         LAN 1    120.40 Mbps  34.20 Mbps   38.12 Mbps  10.50 Mbps  39.52 GB    10.89 GB               0
Spectrum     WAN 1     95.10 Mbps  18.70 Mbps   12.80 Mbps   2.15 Mbps  13.27 GB     2.23 GB               1
Starlink     WAN 2     60.30 Mbps  12.40 Mbps    8.34 Mbps   1.07 Mbps   8.65 GB     1.11 GB               0

WAN         Samples    Min        Avg        Max
--------  ---------  ---------  ---------  ---------
Spectrum        288    9.0 ms    11.2 ms    28.0 ms
Starlink        288   14.0 ms    16.4 ms    22.0 ms
  (router-measured, not client ping)
```

```bash
./cli.py summary --period 7d
./cli.py --wan Spectrum summary --period 30d
```

### daily — per-day throughput and ping summary

```bash
./cli.py daily
./cli.py daily --days 30
./cli.py --remote daily
./cli.py --remote daily --days 30
```

```
Date          Interface    Peak In      Peak Out    Avg In       Avg Out     Total In    Total Out      Failovers
----------    -----------  -----------  ----------  -----------  ----------  ----------  -----------  -----------
2026-03-21    Spectrum     26.06 Mbps   1.96 Mbps   0.87 Mbps   0.26 Mbps   8.92 GB     2.52 GB               0
2026-03-21    Starlink     34.22 Mbps   4.42 Mbps   2.54 Mbps   0.19 Mbps  25.75 GB     1.99 GB               0
2026-03-20    Spectrum     ...
2026-03-20    Starlink     ...

Date          WAN       Samples    Min        Avg        Max
----------    --------  ---------  ---------  ---------  ---------
2026-03-21    Spectrum        288   9.0 ms    11.2 ms    28.0 ms
2026-03-21    Starlink        288  14.0 ms    16.4 ms    22.0 ms
2026-03-20    Spectrum  ...
  (router-measured, not client ping)
```

Defaults to the last 7 days. Use `--days N` to go further back.

### ping — WAN latency history

```bash
./cli.py ping
./cli.py ping --period 7d
./cli.py --remote ping
```

```
WAN latency history — last 24h  (router health check)

Timestamp                  WAN       Min        Avg        Max
-------------------------  --------  ---------  ---------  ---------
2025-01-15 14:05:03 UTC    Spectrum   9.0 ms    11.2 ms    28.0 ms
2025-01-15 14:05:03 UTC    Starlink  14.0 ms    16.4 ms    22.0 ms
...
```

### failovers — WAN health check state change history

```bash
./cli.py failovers
./cli.py --remote failovers
./cli.py --wan Spectrum failovers
```

```
WAN        Event      From         To             Message     Timestamp                    Duration
---------  ---------  -----------  -------------  ----------  ---------------------------  ----------
Spectrum   went down  connected    disconnected   Health Check Failed   2025-01-14 09:12:00 UTC   ongoing
Spectrum   came up    disconnected connected      Connected   2025-01-14 09:57:20 UTC      45m 20s
```

Failover events are sourced from the Peplink REST API (health check state),
not SNMP link state. This means a WAN that is physically connected but
failing Peplink's health checks (e.g. DNS/HTTP probe failure) will appear
as a failover here even though SNMP reports the link as up.

The `Source` column shows how each event was detected:

- `poll` — detected at a 5-minute poll boundary by comparing the WAN's
  `statusLed` from `GET /api/status.wan.connection` against the previously
  stored state. Only catches transitions that persist long enough to be
  visible at back-to-back polls.
- `log` — detected by parsing `GET /api/status.log`, which returns a JSON
  object containing the router's internal 50-entry rolling event log array.
  This catches brief outages that occur and recover between two consecutive
  polls (e.g. a Starlink disconnect that lasts under 5 minutes). Each poll
  adds ~2 log entries, so a WAN event stays visible in the log for ~25 poll
  cycles (~2 hours); polling every 5 minutes guarantees every event is seen
  before it scrolls off. Duplicate detection uses a 60-second window so the
  same real event is never counted twice regardless of which method caught
  it first.

The `Failovers` column in `summary` and `daily` counts green → non-green
transitions from both sources. Historical data before the API was configured
will show 0.

### report — long-term HTML trend report

```bash
./cli.py report
./cli.py report --period 90d
./cli.py report --output ~/Desktop/wan_report.html
```

```
WAN report — all time (2026-03-21 to 2026-07-13)

WAN       Availability    Outages  Total Downtime  Longest Outage  Avg Latency
--------  --------------  -------  --------------  --------------  -----------
Spectrum  99.920%              17  2h 12m          1h 0m           10.7 ms
Starlink  99.964%             134  2h 6m           5m 0s           18.9 ms

Storm days (>8 outages in a UTC day):
Day         WAN       Outages
----------  --------  -------
2026-03-29  Starlink       37
2026-04-06  Starlink       13
2026-05-22  Starlink       21

Report written to reports/report_2026-07-13.html
```

Always prints the terminal summary above and writes an HTML report (KPI
tiles, monthly outage/latency/traffic charts, hour-of-day outage pattern,
storm-day list) to `<report_dir>/report_<date>.html`. Reports are
machine-specific and gitignored — they don't get committed.

Reads from the `throughput_daily`/`latency_daily` rollup tables (populated
by `rollup.py`, see below) rather than scanning raw 5-minute samples, so it
stays fast regardless of how many years of history have accumulated.
`--period` accepts `all` (default) or `Nd` (e.g. `90d`).

Unlike every other command, `report` ignores `--remote` and always behaves
the same way: it generates the HTML file on whichever machine you run it on
(so you can actually open it), and always reads current data — if
`remote_host` is configured and you're not already on that machine, it
first pulls a fresh, WAL-safe snapshot of the remote DB over SSH before
generating. Run `./cli.py report` from the MacBook and it'll fetch from the
Mini automatically; run it on the Mini itself and it just reads the local
DB directly.

## Peplink REST API setup

The REST API uses a permanent Client ID + Client Secret for authentication
(tokens expire every 48 hours and are refreshed automatically; credentials
never expire). Create the client once using the router's admin credentials:

```bash
# Step 1: log in and save the session cookie
curl -c /tmp/peplink.txt -sk \
  -X POST https://192.168.50.1/api/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"YOUR_PASSWORD"}'

# Step 2: create the client app
curl -b /tmp/peplink.txt -sk \
  -X POST https://192.168.50.1/api/auth.client \
  -H 'Content-Type: application/json' \
  -d '{"action":"add","name":"peplink-monitor","scope":"api.read-only"}'
```

The response contains `clientId` and `clientSecret` — copy both into
`config.yaml` as `peplink_api_client_id` and `peplink_api_client_secret`.

You only need to do this once. The credentials persist across router
reboots and firmware updates.

**Why REST API instead of SNMP or client-side ping for latency/failovers?**
SNMP `ifOperStatus` reflects physical link state only. Client-side ping can
only measure whichever WAN the pinging device is currently routed to, producing
unequal sample counts when the router load-balances or fails over between WANs.

The Peplink REST API provides two things the other sources cannot:

- `GET /api/status.wan.latency` — the router's health check engine measures
  both WANs simultaneously and independently every 10 seconds, regardless of
  what any client device is doing. This is the authoritative, equal-sample-count
  latency source.
- `GET /api/status.wan.connection` — exposes `statusLed`/`message` from the
  health check engine (DNS lookups, HTTP probes) that drives the router's actual
  failover decisions. A WAN can be physically up while already failed over here.

## Cron setup

Create the log and data directories first:

```bash
mkdir -p YOUR_REPO_PATH/logs
mkdir -p YOUR_REPO_PATH/data
```

### MacBook Air (development / temporary testing only)

Edit your crontab with `crontab -e` and add:

```
*/5 * * * * YOUR_REPO_PATH/collector.py >> YOUR_REPO_PATH/logs/collector.log 2>&1
5 21 * * * YOUR_REPO_PATH/rollup.py >> YOUR_REPO_PATH/logs/rollup.log 2>&1
```

Remove these entries once you've confirmed things work and have deployed to the Mini.

### Mac Mini (production)

SSH into the Mini and edit your crontab with `crontab -e`:

```
*/5 * * * * YOUR_REPO_PATH/collector.py >> YOUR_REPO_PATH/logs/collector.log 2>&1
5 21 * * * YOUR_REPO_PATH/rollup.py >> YOUR_REPO_PATH/logs/rollup.log 2>&1
```

`rollup.py` populates the `throughput_daily`/`latency_daily` tables that
`./cli.py report` reads from, so `report` stays fast no matter how much raw
history accumulates. It runs once a day at 21:05 **local** time on purpose —
day boundaries in the DB are UTC, and 21:05 local is safely after UTC
midnight year-round under US Eastern DST/EST. If deploying somewhere in a
different timezone, adjust the hour so it still lands after UTC midnight.

## Deployment to Mac Mini

```bash
./deploy.sh
```

This script:
1. SSHs into the remote host and runs `git pull` to update the repo
2. Runs `pip install -r requirements.txt` into pyenv 3.14.0
3. Ensures `logs/` and `data/` directories exist on the Mini
4. Prints the exact crontab entry to add

**Note:** `config.yaml` is gitignored and not in the repo. Copy it to the Mini
manually on first deploy:

```bash
scp -A config.yaml user@YOUR_REMOTE_HOST:~/Documents/Code/peplink-monitor/config.yaml
```

## Notes

- Interface OIDs are discovered on first run and cached in SQLite. If the
  router's interface table changes, delete the `interfaces` table rows to
  trigger re-discovery.
- 64-bit HC counters (`ifHCInOctets` / `ifHCOutOctets`) are used to avoid
  32-bit rollover on fast links. Counter rollover is handled correctly.
- The Peplink B-One exposes these interfaces via SNMP: Eero (LAN), LAN 2–4
  (unused), Spectrum (WAN), Starlink (WAN).
- The SQLite database contains three API-sourced tables: `health_events`
  (one row per WAN state change), `wan_health_state` (current health state
  per WAN, updated every poll), and `wan_latency` (per-poll min/avg/max
  latency per WAN from the router health check engine).
- On first run after upgrading from the ping-based version, existing `wan_ping`
  rows are automatically migrated into `wan_latency` with `source='ping'`.
  The `wan_ping` table is preserved as an archive.
- The Peplink API token expires every 48 hours. The client re-authenticates
  automatically on each collector run (each cron invocation is a fresh
  process). No token caching across runs.
- `throughput_daily` and `latency_daily` are one-row-per-day-per-WAN rollup
  caches of the `throughput`/`wan_latency` tables, kept current by `rollup.py`
  and used by `./cli.py report`. On first run after upgrading, existing
  history is backfilled automatically (a one-time pass, guarded so it only
  runs while the rollup tables are empty).
