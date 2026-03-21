# peplink-monitor

SNMP-based monitoring for the Peplink B-One router. Polls WAN interface
throughput and status every 5 minutes, stores results in SQLite, and
provides a CLI for querying current readings, summaries, and failover history.

## Project structure

```
peplink-monitor/
├── collector.py        # SNMP poller — run via cron
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
remote_user: rob
```

The `db_path` is expanded with `~` so it works unchanged on both machines.

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

Run it a second time to get throughput deltas:

```
2025-01-15 14:05:00 INFO Eero: in=45.23 Mbps  out=8.41 Mbps  status=up
2025-01-15 14:05:00 INFO Spectrum: in=12.80 Mbps  out=2.15 Mbps  status=up
2025-01-15 14:05:00 INFO Starlink: in=8.34 Mbps  out=1.07 Mbps  status=up
2025-01-15 14:05:00 INFO Poll complete.
```

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

### current — latest reading for all interfaces

```bash
./cli.py current
./cli.py --remote current          # query the Mini from MacBook Air
```

```
Interface    Label    Status      In           Out         Last Poll
-----------  -------  --------  -----------  ----------  -----------
Eero         LAN 1    up        45.23 Mbps   8.41 Mbps   2m ago
Spectrum     WAN 1    up        12.80 Mbps   2.15 Mbps   2m ago
Starlink     WAN 2    up         8.34 Mbps   1.07 Mbps   2m ago
```

Filter to one WAN:

```bash
./cli.py --wan Starlink current
```

```
Interface    Label    Status     In          Out        Last Poll
-----------  -------  --------  ----------  ---------  -----------
Starlink     WAN 2    up        8.34 Mbps   1.07 Mbps  2m ago
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
```

```bash
./cli.py summary --period 7d
./cli.py --wan Spectrum summary --period 30d
```

### failovers — interface state change history

```bash
./cli.py failovers
./cli.py --remote failovers
```

```
Interface    Label    Event       Timestamp                    Duration
-----------  -------  ----------  ---------------------------  ----------
Spectrum     WAN 1    went down   2025-01-14 09:12:00 UTC      45m 20s
Spectrum     WAN 1    came up     2025-01-14 09:57:20 UTC      —
```

```bash
./cli.py --wan Spectrum failovers
```

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
```

Remove this entry once you've confirmed things work and have deployed to the Mini.

### Mac Mini (production)

SSH into the Mini and edit your crontab with `crontab -e`:

```
*/5 * * * * YOUR_REPO_PATH/collector.py >> YOUR_REPO_PATH/logs/collector.log 2>&1
```

## Deployment to Mac Mini

```bash
./deploy.sh
```

This script:
1. Rsyncs the project (excluding `.git`, `data/`, `logs/`, cache files) to `user@YOUR_REMOTE_HOST:~/peplink-monitor/`
2. SSHs in and runs `pip install -r requirements.txt` into pyenv 3.14.0
3. Ensures `logs/` and `data/` directories exist on the Mini
4. Prints the exact crontab entry to add

**Note:** `config.yaml` is gitignored and excluded from rsync (it may contain
your production community string). Copy it to the Mini manually on first deploy:

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
