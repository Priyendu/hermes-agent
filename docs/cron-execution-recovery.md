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
identity in the production gateway container before promoting the image, and
keep it stable across container recreation. If multiple independent schedulers
share the same execution ledger, each needs a unique identity. Never copy this
production identity to a concurrent replica.

This is a Tier-2 deployment prerequisite only. Merging the code does not apply
the environment setting or deploy/restart the service. The current recovery
path must continue to operate when the identity is absent: it retains the
legacy PID/start-time check for an owner that cannot be matched by identity,
and treats inability to establish process liveness as live/indeterminate.

One-shot jobs retain their durable dispatch limit through `claim_dispatch`;
releasing a dead process's lease does not authorize another side-effecting
dispatch beyond that limit. Before any production rollout, verify the exact
image, the stable identity value, and the configured scheduler replicas as a
separate owner-authorized operation.
