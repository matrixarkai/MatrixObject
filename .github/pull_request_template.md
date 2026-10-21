<!--
Describe the change and what you measured. If part of it is unverified, say which part --
an honest gap is useful, a silent one is not. Delete anything below that does not apply.
-->

## What this changes

## What proves it

<!--
A test that would fail without the change. If the change claims a number, the test
measures that number rather than restating it.
-->

- [ ] Added or changed a test that fails without this change
- [ ] Watched that test fail with the change reverted, then pass with it

## Checks

- [ ] `RUSTFLAGS="-D warnings" cargo build --all-targets --locked` -- green
- [ ] `tools/suite_gate.sh` -- no new failure, and none of the 21 recorded ones has gone
      missing (CI does not run the suite; this is the only place it runs)
- [ ] Any new `.rs` file opens with the two licence header lines
- [ ] If this adds a guard worth keeping, `.github/workflows/ci.yml` pins it by name

## Durable formats

- [ ] This changes no format constant that is hashed into a stored proof

<!--
If it does, say so here and say what reads the old form: renaming such a constant changes
every checksum derived from it, which makes the change a migration rather than a rename.
-->
