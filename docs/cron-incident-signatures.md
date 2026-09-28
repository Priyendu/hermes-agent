# Cron incident identity and legacy acknowledgements

Failure identity must not be based on a display prefix. A changed error after
the first 200 characters previously inherited the same incident and could be
silenced by its acknowledgement, even when the suffix described a worse
failure. Concatenating job ID and error without framing could also confuse two
different jobs with the same six-character ID prefix.

New incidents use the complete existing case/whitespace-normalized error,
JSON-framed with the complete job ID and a version domain, and the full SHA256
digest. Their identity is separate from redacted, bounded display text. Two
incidents may have identical stored 500-character display prefixes while still
having different identities and acknowledgement states.

## Compatibility and rollout behavior

- Existing incident rows and acknowledgement timestamps are left untouched.
  The CLI can still list and acknowledge their existing IDs.
- New versioned IDs do not inherit legacy prefix-based acknowledgements,
  including for short errors. The old stored/redacted text is not sufficient
  evidence to infer the full original error and safely transfer an ack.
- This can produce a one-time new alert for a previously acknowledged standing
  failure after a separately approved rollout. Reviewers and operators must
  account for that conservative reclassification; do not bulk-ack new rows to
  silence it or backfill an inferred old/new match.
- Acknowledging a new ID continues to suppress that same full normalized error.
  A changed normalized error receives a distinct incident, including changes
  beyond the display bound. New IDs are longer; copy the complete CLI-displayed
  ID when acknowledging. No schema or job-config migration is required.
- Before rollout, check any external consumer that stores/parses incident IDs
  for an old fixed-length assumption. The repository CLI has no such limit;
  that does not prove an out-of-repository integration is compatible.
- Rolling back restores the old unsafe signature classifier and must not be
  mistaken for a safe cadence/ack migration. Source merge alone does not change
  a running gateway or authorize rollout, restart or live incident mutation.

## Boundaries: #1597 / #1594 remain open

This corrects raw failure identity, not the complete delivery policy. The digest
is an identifier, not producer authentication or owner approval. Existing
case/whitespace normalization and the lifetime acknowledgement model remain.
It does not introduce semantic severity/resource signatures, remove volatile
timestamps or counters, add a six-hour reminder, announce silent recovery, or
rearm an acknowledged same-signature recurrence after success. Those require
the separately reviewed episode/delivery policy. Execution exit codes,
`last_status`, output and failed attempt records remain unchanged.

The actual normal and outer-exception scheduler paths share the corrected
incident API. Behavioral tests use real temporary SQLite records, legacy rows,
fresh-process reopening, CLI list/ack, real failing no-agent scripts and
in-memory delivery capture; no production notification or live job is needed.
