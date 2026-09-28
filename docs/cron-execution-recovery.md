# Cron execution recovery and owner identity

Cron execution rows are an attempt ledger, not a retry queue. When a scheduler
restarts, it may mark an attempt `unknown` only after it has evidence that the
owning process is gone. It may then release only the exact fire/run lease owned
by that attempt. An `unknown` result means the job may have produced side
effects: recurring jobs follow their normal next-fire policy, while one-shot
jobs remain subject to their durable `claim_dispatch` limit.

## Deployment identity prerequisite

For the production Hermes gateway rollout associated with issue #1657, the
owner has selected `HERMES_MACHINE_ID=hermes-prod-boston-01`. Set that stable
identity in the production gateway container only after the migration checks
below, before promoting the image, and keep it stable across container recreation.
If multiple independent schedulers share the same execution ledger, each needs
a unique identity. Never copy this
production identity to a concurrent replica.

This is a Tier-2 deployment prerequisite only. Merging the code does not apply
the environment setting or deploy/restart the service. Recovery compares the
execution's stored owner identity with the recovering service's current identity:

| Stored owner / recovering service | Recovery evidence |
| --- | --- |
| Both identified with the same identity | Local PID/start-time checks may prove the owner gone. |
| Neither identified | Legacy single-PID-namespace compatibility checks apply. |
| Both identified with different identities | Preserve the execution; a local PID miss cannot disprove a remote owner. |
| Exactly one identified | Preserve the execution without a local PID lookup; its namespace is unproven. |

A live PID with missing/unreadable start-time metadata is indeterminate, not
dead. The neither-identified compatibility case is not a safe ownership model
for independent PID namespaces sharing a ledger.

## Pre-identity ledger migration: drain, verify, or stop

Opening an older `cron/executions.db` adds the nullable `owner_host_id` column;
it does **not** infer or backfill ownership. Existing `claimed` and `running`
rows therefore retain `NULL`. An identified gateway or dashboard deliberately
preserves them, even if that service cannot find their PID locally. An earlier
recovered attempt's legacy lease-cleanup retry also must not clear a lease
while an unresolved active execution remains for that job.

Before introducing a stable identity, a separately owner-authorized rollout
must:

1. Inventory every ledger writer/reaper and its PID namespace, including the
   gateway, dashboard, CLI and any scheduler replica. Identify the shared profile
   and ledger and assign distinct stable identities to independent namespaces.
   A shared profile's fallback identity must not make different services appear
   to own each other's processes.
2. Stop new dispatch and let existing attempts reach genuine durable terminal
   results through their owning service's normal completion path. Draining or
   stopping services is an operational action requiring separate authorization.
3. Verify there are no active untagged rows in the exact shared ledger, after
   dispatch has been held and before identities/image promotion change. Recheck
   at cutover; a sample taken while writers still dispatch is insufficient.
4. If any untagged active attempt remains, **stop the rollout**. Require a
   separately authorized, evidence-bound reconciliation for that exact attempt
   and its side effects. Do not copy the current service identity onto it,
   delete it, force-clear its leases by TTL, or treat a PID miss in the new
   container as proof that the old owner died. `unknown` is not permission to
   replay a side-effecting operation.
5. Verify each service's actual identity, shared profile and replica topology.
   Confirm new attempts record the intended identity and old terminal history
   remains untagged. If this prerequisite cannot be demonstrated, remain on the
   current release rather than weakening the asymmetric-identity guard.

These checks describe a rollout prerequisite, not authorization to inspect or
modify the production ledger. They cover execution-ledger recovery and its
matching-lease cleanup, not distributed fencing of every dispatch entry point.
The ordinary `claim_job_for_fire` path can replace an expired fire lease without
consulting the execution ledger; `get_due_jobs` also handles one-shot run-claim
TTL independently. An unresolved active row is **not** a fleet-wide dispatch
stop. The separately authorized rollout hold/drain must cover all entry points;
verify the actual production dispatch path independently before promotion.

One-shot jobs retain their durable dispatch limit through `claim_dispatch`;
releasing a dead process's lease does not authorize another side-effecting
dispatch beyond that limit. Before any production rollout, verify the exact
image, the stable identity value, and the configured scheduler replicas as a
separate owner-authorized operation.
