# Spun

[![GitHub license](https://img.shields.io/github/license/curvedinf/spun)](LICENSE)

Beautiful Python workers.

## What is Spun For?

Spun is for running Wove-shaped Python work durably without making the user
assemble a broker, scheduler, worker fleet, and monitoring stack before the
first useful task runs.

Spun starts as one process with SQLite. Wove describes the workflow. Spun owns
durability, restart recovery, worker-space calls, schedules, and the operational
view of what happened.

Useful properties include:

- **Wove-Native Execution**: Route selected Wove tasks through `executor="spun"`
  while keeping normal `weave()` and `@w.do` code.
- **Durable By Default**: Work is committed to SQLite before execution and can
  recover after process restart.
- **No External Broker**: The MVP does not require Redis, RabbitMQ, Docker,
  Kubernetes, or a separate scheduler process.
- **Worker-Space Calls**: Define project IO in `calls.py` and call it from
  Spun-executed Wove tasks with `from spun import call`.
- **Scheduled Wove Work**: Schedule functions or `Weave` objects from
  `schedule.py` using Python-shaped schedule objects.
- **Shared Python Environment**: The default executor runs in-process through
  sync Wove, keeping latency low and memory shared.
- **Free Threading Compatible**: On Python 3.14t and other free-threaded
  builds, the same execution path can become true multithreading without
  changing the user API.
- **High Visibility**: Inspect queued, running, finished, and failed work with
  built-in CLI commands.

## Install

Install Spun with `uv`:

```bash
uv add spun
```

Or with `pip`:

```bash
pip install spun
```

## The Basics

Start Spun from a project directory:

```bash
spun start
```

Route Wove work to Spun:

```python
import spun
import wove
from wove import weave

from myapp.reports import render_report


wove.config(
    environments={
        # Spun defaults to a localhost:7766
        "spun": {"executor": "spun"},
    },
)


with weave(account_id="acct_123") as w:
    @w.do
    def account(account_id):
        return {"id": account_id}

    @w.do(environment="spun")
    def report(account):
        # This function executes on the Spun worker
        return render_report(account)

print(w.result.report)
```

Importing `spun` registers the Wove executor adapter. Wove still owns the task
graph. Spun stores the routed work, runs it in the local Spun worker, records
events, and returns the result to the weave.

## Calls

Define worker-space IO functions in `calls.py`. These allow remotely defined
weave tasks to use persistent worker-local resources:

```python
from myapp.database import DatabasePool
from spun import call


db = DatabasePool.from_env()


@call
def check_db():
    with db.connection() as conn:
        return conn.fetch_one("select 1 as ok")["ok"] == 1
```

Use them from Spun-executed Wove tasks:

```python
from spun import call
from wove import weave


with weave() as w:
    @w.do(environment="spun")
    def health():
        return call.check_db()
```

Calls are normal Python-shaped handles, but their implementation lives in the
Spun worker context. That keeps database pools, API clients, browser sessions,
and other live resources out of task payloads.

## Schedules

Define scheduled Wove work in `schedule.py`:

```python
from spun import Cron, schedule
from wove import weave


@schedule(Cron(hour=2, minute=0, zone="America/Chicago"))
def refresh_reports():
    with weave() as w:
        @w.do(environment="spun")
        def reports():
            ...
    return w
```

Or schedule a reusable `Weave`:

```python
from datetime import timedelta

from spun import schedule
from wove import Weave


@schedule(timedelta(hours=1))
class NightlySync(Weave):
    @Weave.do(environment="spun")
    def sync(self):
        ...
```

The decorator preserves the object it decorates. A scheduled function is still
that function. A scheduled `Weave` is still that `Weave`.

## Inspecting Work

Spun keeps the default operational surface small:

```bash
spun status
spun work
spun why <work-id>
spun tail <work-id>
```

`status` shows queue counts. `work` lists recent work. `why` explains the local
state of one item. `tail` shows its event history.

## Documentation

Spun is still early. The design plan records the intended shape beyond the MVP:

[View PLAN.md](PLAN.md)

## Topics

The topic path starts with the smallest useful Spun process, then adds the
things real durable work needs as it grows: calls, schedules, visibility,
restart recovery, artifacts, security, and clustering.

- **The Basics**: start one Spun process and route Wove work through
  `executor="spun"`.
- **Calls**: define worker-space IO in `calls.py` and use it through
  `from spun import call`.
- **Schedules**: define scheduled Wove work in `schedule.py` with `Cron`,
  `Calendar`, `timedelta`, or direct datetime objects.
- **Monitoring**: use `spun status`, `spun work`, `spun why`, and `spun tail`
  before adding any external observability stack.
- **Production Shape**: keep the one-process path simple while Spun grows
  toward artifacts, privileges, locality, and clusters.

## Current MVP

The current implementation includes:

- SQLite durable ledger in `.spun/spun.sqlite3`
- `.env`, `calls.py`, and `schedule.py` discovery from the current project
  directory
- Wove adapter for `executor="spun"`
- in-process worker execution through sync Wove
- `@call` and `call.<name>(...)`
- `@schedule(...)` and `schedule(..., work)`
- `Cron` and `Calendar` schedule objects
- restart recovery for work that was running when Spun stopped
- CLI inspection commands

Clustering, peer roles, privilege policy, artifact storage, remote locality,
Postgres, provisioning, and the web UI are intentionally outside the MVP.
