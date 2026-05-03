# Spun Plan

Spun is a durable execution fabric for Python. It is designed to be the
Wove-compatible runtime that owns resilience, placement, live resource access,
cluster coordination, and operational visibility for work that should survive
process boundaries.

The short version:

> Wove describes the work. Spun makes the execution durable, observable, and
> correctly placed.

The user-facing surface should stay small. The complexity belongs inside Spun's
requirements and implementation, not in user configuration.

## What Users See

Start Spun:

```bash
spun start
```

Join another process to a cluster:

```bash
spun join http://host-a:7766
```

Route selected Wove work to Spun:

```python
import wove
from wove import weave

from myapp.accounts import load_account
from myapp.reports import render_report


wove.config(
    environments={
        "spun": {"executor": "spun"},
    },
)


with weave(account_id="acct_123") as w:
    @w.do
    def account(account_id):
        return load_account(account_id)

    @w.do(environment="spun")
    def report(account):
        return render_report(account)

print(w.result.report)
```

Inspect what is happening:

```bash
spun status
spun work
spun why <work-id>
spun tail <work-id>
```

That is the expected common path. Extra configuration should appear only when
the user is naming real project structure, such as a cluster group, not when
they are satisfying Spun's internal plumbing.

## Relationship To Wove

Spun integrates through a Wove adapter. Wove should not need semantic changes.

Spun must not require Wove to change:

- `weave()`
- `@w.do`
- dependency resolution
- task signatures
- task mapping
- result handling
- retry and timeout syntax
- environment routing

Wove owns the workflow shape. Spun owns durable execution after Wove routes a
ready task frame to `executor="spun"`.

## Configuration Rule

Configuration should name intent, identity, and project topology. It should not
force the user to describe Spun's implementation.

Good configuration:

```toml
[cluster]
name = "acme"

[groups.analytics]
calls = ["check_db", "fetch_customer"]

[groups.browser]
calls = ["render_page"]
```

Good peer environment:

```env
SPUN_GROUPS=analytics
ANALYTICS_DATABASE_URL=postgresql://...
```

Bad configuration shape:

```toml
# This is too much user-facing machinery.
pool_size = 12
execution = "process"
subscribe = ["pooling", "health", "slow-query-log"]
```

Spun should infer or choose those implementation details from its own
requirements, runtime observations, the call implementation, and safe defaults.

## Root Defaults

The first `spun start` creates the root authority by default.

The root owns cluster-wide meaning:

- cluster name
- group names
- call names
- default storage choice
- default durability policy
- default placement policy
- default trust model
- default monitoring behavior

Peers discover these defaults when they join. Users should not need to repeat
root-defined meaning on every peer.

## Environment Loading

By default, Spun's project/config directory is the current working directory
where the `spun` command is called. This makes local development and CI
pipelines straightforward: checkout the project, run `spun start`, and Spun
uses that directory as the project context.

Each root and peer must automatically load `.env` from that project directory.
Process environment variables are accepted for values not defined in `.env`. If
both are present, the `.env` value wins.

This applies to root and peers. It is how deployment options, group membership,
and secrets are supplied without command-line sprawl.

Spun may also support explicit config directories and userspace cluster state
for long-lived local clusters, but those should be opt-in rather than the
default. Predictable examples:

```text
./.env
./calls.py
./spun.py
./schedule.py
~/.config/spun/clusters/<cluster-name>/
```

Root config may refer to secrets by environment name. Root config must not
distribute secret values.

## Code As Config

Spun should support code as configuration for Python objects that are better
defined in code than in TOML or environment variables.

This includes:

- call definitions
- scheduled Wove work
- named inline-weave functions
- project-specific resource adapters

The pattern should be conventional first and customizable later. Spun should
look for expected filenames, then allow root config and peer config to add or
replace discovery paths when needed.

Expected project files may include:

```text
spun.py
calls.py
schedule.py
```

The exact names can change, but the requirement is stable: a project should be
able to define Spun's Python-facing resources without writing packaging glue.

Code-as-config should follow the same specificity waterfall as normal config:

1. built-in defaults
2. root-discovered code
3. group-specific code
4. peer-discovered code
5. peer environment values

Peer-specific code is allowed but not required. Most projects should define
calls and schedules once at the root/project level. A peer should only need
local code when it hosts a runtime that is genuinely peer-specific.

### Calls

Calls are exposed to application code through a simple surface:

```python
from spun import call
```

Application and Wove task code can then use project-defined calls:

```python
from spun import call
from wove import weave


with weave() as w:
    @w.do(environment="spun")
    def health():
        return call.check_db()
```

The function behind `call.check_db(...)` is defined by remote code-as-config:

```python
# calls.py
from spun import call


@call
def check_db():
    ...
```

`check_db` can use worker-space IO, such as a database pool, browser session,
API client, vector client, or private service client. The user-facing code does
not receive or serialize those live resources. It calls a normal Python
function-shaped handle; Spun resolves the implementation in worker space.

The decorator name can change later, but the requirement is stable:

```python
from spun import call

call.any_project_function(...)
```

must be usable from Spun-executed Wove tasks, and `any_project_function` must be
user-definable in code-as-config.

Spun must:

- discover `call` definitions from conventional files
- expose discovered definitions through the `call` namespace
- resolve `call.<name>(...)` in the current Spun execution context
- bind worker-space resources used by calls
- avoid serializing live connections through task payloads
- make call availability visible to placement
- make call use visible to monitoring
- allow peer-specific call implementations when needed

### Scheduled Wove Work

Spun should be able to schedule Wove workflows. `schedule` should accept the
work directly: either a `Weave` object or a function that creates inline Wove
work. The same API should work as `@schedule(...)` and as
`schedule(..., work)`.

Examples:

```python
# schedule.py
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

Or:

```python
# schedule.py
from datetime import timedelta

from spun import schedule
from wove import Weave


@schedule(timedelta(hours=1))
class NightlySync(Weave):
    @Weave.do(environment="spun")
    def sync(self):
        ...
```

The direct call form should accept the same work:

```python
# schedule.py
from datetime import timedelta

from spun import schedule
from myapp.workflows import NightlySync


schedule(timedelta(hours=1), NightlySync)
```

Schedule definitions should be Python objects, not preset strings. Use the
stdlib when it is already clean. Where the stdlib is verbose, Spun should model
its naming and simplify the surface.

The required shape is:

- `timedelta(...)` for fixed intervals
- `time(...)`, `date(...)`, and `datetime(...)` when they naturally describe
  the schedule
- `Cron(...)` for cron-shaped recurring schedules
- `Calendar(...)` for recurring wall-clock schedules when it reads cleaner than
  cron fields
- `zone="America/Chicago"` as the simple form of `ZoneInfo("America/Chicago")`

`Cron(...)` should be a first-class schedule object, not just a string parser.
It should accept obvious keyword arguments such as `minute`, `hour`, `weekday`,
`monthday`, `month`, and `zone`. A cron string can exist as an escape hatch, but
not as the documented path.

`Calendar(...)` should use obvious keyword arguments such as `at`, `weekday`,
`monthday`, and `zone`.

The exact schedule object names can still change. The requirement is that Spun
schedules Wove-shaped work, not a separate Spun task language.

The decorator must preserve the decorated object. A scheduled function should
still be a normal function. A scheduled `Weave` should still be the same
`Weave` object the rest of the project can import, test, subclass, or pass to
`weave(...)`.

Spun must:

- discover scheduled work from conventional files
- accept functions and `Weave` objects in both decorator and direct-call form
- allow root and peer discovery overrides
- persist schedule state in the durable ledger
- apply idempotency so missed or duplicated scheduler ticks do not create
  unsafe duplicate work
- run scheduled work through normal Spun placement, call, artifact,
  privilege, and monitoring behavior

## Groups

Groups are named project contexts. They are a compact way to say that certain
work and certain peers belong to the same operational area.

Groups should stay declarative:

```toml
[groups.analytics]
calls = ["check_db", "fetch_customer"]

[groups.browser]
calls = ["render_page"]
```

A peer joins groups through local environment:

```env
SPUN_GROUPS=analytics,browser
```

Spun must use groups to derive placement, privileges, call availability,
monitoring, and resource locality. Users should not have to configure those
mechanics directly.

Wove can reference a group through an environment when the project needs to
name a non-default execution context:

```python
wove.config(
    environments={
        "analytics": {
            "executor": "spun",
            "executor_config": {"group": "analytics"},
        },
    },
)
```

Then Wove code stays normal:

```python
@w.do(environment="analytics")
def customer_context(customer_id):
    ...
```

## Core Requirements

Spun must:

- accept Wove task frames through a Wove adapter
- discover code-as-config from conventional project files
- expose project-defined remote functions through `from spun import call`
- commit work to a durable ledger before execution
- execute default local work through sync Wove in one shared Python process
- route durable writes to the correct authority
- lease work to execution peers
- heartbeat active attempts
- expire leases when peers disappear
- retry eligible work on another compatible peer
- pause unsafe writes when an unavailable authority owns the only durable store
- track attempts, events, logs, progress, cancellation, errors, and results
- track orphaned call results separately from normal result delivery
- preserve idempotency across retries and client resubmission
- infer peer capabilities from runtime state
- infer call availability from group membership and peer environment
- place work near required live resources and large artifacts
- enforce authentication and privileges internally
- explain placement, blocking, retry, and failure decisions

These are Spun requirements, not user configuration requirements.

## Execution Model

The default executor should run in-process through sync Wove execution. Spun
should keep one shared Python environment for the common path, with no forked
worker fleet and no external runtime required.

That matters for Python 3.14t and other free-threaded builds: the same default
path can become true multithreaded execution without changing user code. On
normal GIL builds, it remains the minimum-latency shared-memory path for IO
heavy work.

Process isolation, subprocess pools, and remote peers can exist for privilege,
crash isolation, or capacity later. They should not be the default MVP
execution shape.

## Durable Ledger

The durable ledger is Spun's source of truth.

It must track:

- work items
- attempts
- leases
- peer heartbeats
- task events
- logs and progress
- small serialized values
- artifact references for large values
- idempotency keys
- call health and capacity
- orphaned call results
- group and principal
- placement and authorization decisions

SQLite is the default storage provider because it gives the smallest useful
system. SQLite remains valid in a cluster when one authority owns that ledger
and other peers route durable writes to it.

Postgres is the first scalable shared storage provider.

## Orphan Queue

The orphan queue is for recovery, not normal execution.

The orphan queue is always on because Spun's implicit contract is reliability.
It should have a short default recovery window so projects that never need to
recover lost return handles do not accumulate long-lived operational state.

Normal result delivery should go directly back to the live caller. The orphan
queue exists for the failure window where Spun has durably accepted or completed
a call, but the Wove/web-side process died before it recorded the promise id,
acknowledged the result, or returned the handle to its own caller.

Spun should treat that as an orphaned call result:

```text
call accepted by Spun
caller dies before recording the call id
call finishes
result waits in the orphan queue
restarted caller or operator can discover and reconcile it
```

This is not a general message queue and should not be taught as the normal way
to consume results. It is a recovery surface for lost return ownership.

To make this possible, durable calls need enough return identity to be
discoverable without the original in-memory promise object:

- call id
- call name
- arguments hash or idempotency key
- submitter principal
- return scope
- parent Wove delivery id or parent Spun call id when available
- status, result, error, and event history
- ack state

The return scope can be supplied by a framework adapter, request context,
session, thread id, user id, explicit idempotency key, or parent Spun call. If
Spun has no stable return scope, it can still expose the work operationally, but
it cannot know which restarted process should own the result.

The orphan queue must support:

- listing orphaned results by return scope
- claiming an orphaned result
- acknowledging a recovered result
- replaying missed events when useful
- expiring old unclaimed or acknowledged entries by policy
- explaining why a result became orphaned

## Offline Peers

Spun must expect peers to disappear.

If an execution peer goes offline, Spun should:

- detect missed heartbeats
- expire active leases
- record lost attempts
- retry eligible work on another compatible authorized peer
- explain what moved and what could not move

If an authority peer goes offline, Spun must distinguish safe recovery from
unsafe recovery. If a SQLite authority owns the only durable copy of a namespace,
that namespace pauses until the authority or durable store returns. Spun should
not create split-brain by accepting writes elsewhere.

For higher availability, users can move a namespace to Postgres or a future
replicated provider.

## Placement

Placement is Spun's responsibility.

Spun must consider:

- peer liveness
- runtime compatibility
- group membership
- call availability
- artifact locality
- trust boundaries
- current load
- historical behavior
- privilege requirements

The user should not have to manually describe all of this. Spun should choose
the broadest safe execution set by default and explain the decision.

Example explanation:

```text
executed on analytics-a
reason: check_db and fetch_customer available, artifact local, worker-a offline
```

## Security

Spun must authenticate cluster communication and client operations.

Spun must:

- create a local identity automatically for a one-node cluster
- require an invite or bootstrap credential to join a networked cluster
- authenticate peer-to-peer communication after join
- encrypt cross-peer traffic
- authenticate durable writes, leases, heartbeats, artifact access, call
  advertisements, and event delivery
- record the principal that submitted work
- enforce privileges internally based on group, call, peer, and operation

The default local user should not need to learn a policy language to run useful
work.

## Locality And Artifacts

Spun must not push large blobs through task payloads by default.

Small values may be serialized inline. Large values should become artifacts
with references. Placement should prefer peers near artifacts and the
worker-space resources used by calls.

This matters for:

- reports
- documents
- screenshots
- browser traces
- embeddings
- model outputs
- agent artifacts

## Monitoring And Debugging

Spun must own monitoring and debugging tools.

Built-in tools should answer:

```text
What is queued?
What is running?
Where is it running?
Why was it placed there?
What call or artifact is it waiting on?
Which authority committed it?
Which peer leased it?
Which principal submitted it?
Was it blocked on queue capacity, call availability, rate limits, auth,
artifact delivery, stale leases, or worker failure?
```

Initial commands:

```bash
spun status
spun work
spun peers
spun groups
spun calls
spun why <work-id>
spun tail <work-id>
spun ui
```

Monitoring should be useful without setup. More detail can appear when Spun
detects richer resources, groups, calls, or artifacts.

## Local Multi-Peer

Spun must support multiple peers under one OS user.

This is required for:

- testing cluster behavior
- local development
- demos
- running separate roles on one machine
- browser/resource isolation without Docker
- authority and offline-peer testing

One command should create a local fixture cluster:

```bash
spun dev cluster
```

Manual local peers should also be easy:

```bash
spun start --name root
spun join root --name worker-a
spun join root --name analytics-a
spun join root --name browser-a
```

Spun must provide:

- automatic port assignment
- per-peer runtime directories
- per-peer `.env` views
- clear peer names
- local peer discovery shortcuts
- cluster-wide status
- clean shutdown
- no root or admin privileges

## Provisioning

Dynamic spin-up is useful, but it is not required for the core.

Spun should expose unmet demand signals internally:

- no live peer can run eligible work
- backing capacity for a call is saturated
- artifact locality is poor
- queue delay is high
- memory pressure is high

Provisioner plugins can later react to those signals and start containers,
virtual machines, Kubernetes pods, cloud workers, or SSH workers. New workers
join the cluster like any other peer.

## Early Milestones

1. Wove adapter for `executor="spun"`.
2. `spun start` one-node cluster with SQLite ledger.
3. CWD project directory and `.env` loading.
4. Root defaults discovered by peers.
5. Durable ledger with attempts, leases, heartbeats, retry, cancellation,
   offline peer recovery, and idempotency.
6. Basic CLI monitoring: `status`, `work`, `peers`, `why`, and `tail`.
7. Local multi-peer dev cluster under one OS user.
8. Peer join, identity, authentication, and authority-routed writes.
9. Simple group definitions.
10. Code-as-config discovery for calls and schedules.
11. Worker-hosted call detection and health reporting.
12. Scheduled Wove workflows.
13. Capability, privilege, and locality-aware placement.
14. Artifact references for large payloads.
15. Postgres storage provider.
16. Web UI for execution visibility.
17. Provisioner plugin interface.

## Minimum Viable Spun

The MVP is the smallest version that proves Spun is useful without becoming a
smaller Celery clone.

It must have:

- `spun start`
- a SQLite durable ledger in the current project context
- automatic `.env` loading from the current project context
- a Wove adapter for `executor="spun"`
- durable submission, attempts, retry, cancellation, and restart recovery
- in-process execution through sync Wove in one shared Python environment
- true multithreading on Python 3.14t/free-threaded builds without a different
  user API
- code-as-config discovery for `calls.py` and `schedule.py`
- `from spun import call`
- `@call` definitions available inside Spun-executed Wove tasks
- promise-like call handles with `id`, `status`, `events`, `result()`,
  `cancel()`, and `stream()`
- always-on short-window orphan queue for unacknowledged call handles
- `schedule(...)` and `@schedule(...)` for functions and `Weave` objects
- `timedelta(...)` and `Cron(...)` schedule objects
- `spun status`
- `spun work`
- `spun why <work-id>`
- `spun tail <work-id>`

That MVP should let a user run:

```bash
spun start
```

and then route Wove work through Spun, schedule Wove work, call worker-space
IO, kill the process, restart it, and understand what happened.

It should not require:

- an external broker
- Redis
- RabbitMQ
- Docker
- Kubernetes
- a separate scheduler process
- a separate monitoring stack
- a config file for the default path
- explicit root mode
- explicit default groups
- Wove changes beyond the Spun adapter

It should defer:

- clustering of any kind
- networked peer join
- peer roles
- cluster identity and authentication
- placement policy
- privilege policy
- multi-authority storage
- Postgres support
- artifact storage
- remote locality scoring
- dynamic provisioning
- web UI
- advanced policy language
- group overrides beyond the shape needed by local calls

The MVP is successful when Spun can be installed into a normal Python project
and make Wove-routed work durable, inspectable, callable, and schedulable with
one running process.
