# Security Policy

## Supported versions

MatrixObject is pre-1.0. Security fixes target the latest `main` and the most recent
published `0.x` release.

## Reporting a vulnerability

Please report suspected vulnerabilities **privately** via GitHub Security Advisories
— use **"Report a vulnerability"** on the Security tab of
<https://github.com/matrixarkai/MatrixObject/security/advisories/new> — rather than
opening a public issue or pull request.

Include, if possible:

- a description of the issue and its impact,
- the affected version or commit,
- steps to reproduce or a proof of concept.

We aim to acknowledge reports within a few business days and will coordinate a fix
and a disclosure timeline with you.

## Scope

Because MatrixObject is an object store that acknowledges writes and reconstructs
reads, **defects in durability or in the proofs of it are treated as security
issues, not merely bugs**: a write acknowledged and not durable, a read served
bytes that do not reconstruct from the fragments the store holds, a replica set that
reports complete while short, a write receipt that attests to a write that did not
happen, or a rolled-back mutation that leaves state a validator calls corrupt.
Retention and eviction belong here too: a structure that drops entries other than
the ones it claims to drop is a correctness defect, not a tuning question.

Defects in a deployment's own operating decisions are out of scope, but a
configuration this project ships as a **default** is in scope: if a default is
unsafe, the default is the bug.
