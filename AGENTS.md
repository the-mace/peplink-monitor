# Agent instructions

## Before starting any work

Dependabot auto-merges dependency and GitHub Actions updates to `main` after
CI passes. Your local checkout is often behind without any local commits.

**Always sync before editing, exploring, or running the app:**

```bash
git checkout main
git pull --ff-only
```

If `--ff-only` fails, stop and reconcile with the remote before making changes.
Do not start work on a stale tip.

After a pull that touches `requirements.txt` or other install metadata:

```bash
pip install -r requirements.txt
```
