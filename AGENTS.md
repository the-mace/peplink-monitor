# Agent instructions: peplink-monitor

Shared rules (pull before work, tests and lint, secrets, commit attribution)
live in the global `~/.config/agents/AGENTS.md`. If that file is not loaded
(cloud or CI sessions), the short version is: `git pull --ff-only` first, run
the tests below before committing, never commit secrets, and end agent commits
with a `Co-Authored-By: <Tool> (<model>) <email>` trailer.

## Which coding agent

Use Claude Code with Sonnet for small, contained fixes driven by an alert or a
bad report (outage detection, availability math, collector gaps). Use Opus 5.5
for multi-file changes to the collector, DB schema, or rollups. Grok Build is
the second opinion.

## Repo specifics

- Monitors a Peplink B-One via SNMP and the local REST API, stores samples in
  SQLite, and exposes a CLI. Details and runbooks are in `README.md`.
- After a pull that touches `requirements.txt`: `pip install -r requirements.txt`
  (pyenv 3.14.0, see `.python-version`).
- Tests: `python3 -m pytest tests/`. Deploy to the Mac mini with `./deploy.sh`.
- `config.yaml` holds the SNMP community and API client secret. It is
  gitignored; edit `config.yaml.example` instead.
