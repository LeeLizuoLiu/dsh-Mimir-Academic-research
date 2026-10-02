# Preserved custom Mimir for Web DSH 0.1.6-alpha.2

This fork preserves LeeLizuoLiu's working local Mimir customization on 2026-10-02.
The package version remains upstream `0.20.0`; identify this customization by its Git commit/tag, not that version alone.

## Baseline

- Web DSH: **0.1.6-alpha.2**; Node >=22.
- Source base: `a84cf80b4a7e2993070bb924f308948a62b0f390`.
- Includes all 26 modified tracked files and 3 previously untracked source/test/script files.
- Native SSH/Slurm transport, shared SSH connections, manual refresh, server/job UI changes.
- `scripts/patch-local-dsh-codecs.mjs` converts generated Typert schema fields into factories expected by this DSH version. Root `build` invokes it.
- The installed Mimir package's complete `lib/` files were compared byte-for-byte with the local checkout's compiled files: no differences.
- This preservation task did not rebuild the plugin or contact a research cluster. Newer DSH versions and fresh-source reproducibility have not been validated here.

## Frozen snapshot

Use branch `backup/dsh-0.1.6-alpha.2-2026-10-02` and tag `mimir-custom-dsh-0.1.6-alpha.2-20261002` as the recovery baseline.
The locally saved installable package `dsh-mimir-0.20.0-custom-dsh-0.1.6-alpha.2.tgz` contains the exact installed compiled plugin, without rebuilding.
The source checkout and Git bundle are also saved in the local backup.

To restore the plugin while running the pinned old Web DSH, stop Web DSH and run:

```sh
# First verify your dsh symlink still selects 0.1.6-alpha.2.
dsh plugin --profile web add /absolute/path/dsh-mimir-0.20.0-custom-dsh-0.1.6-alpha.2.tgz
```

Restart Web DSH, check that Mimir opens, check session selection, and check saved servers/jobs before attempting cluster actions. An install command alone does not establish successful recovery.
Package dependencies may still need the pnpm store/network; the .tgz is not a complete offline DSH installation.
The local backup also holds the exact old DSH installation for recovery on this Mac; retain its original path if restoring symlinks.

For source development use the checked-in pnpm lockfile, Node >=22 and the pinned pnpm version in package.json. Build with `pnpm run build`; keep the codec patch step. The older local installer script contains machine-specific absolute paths and is a historical artifact, not a portable installer.

## Future DSH adaptation

Create a new branch from this snapshot for each target DSH version. Test in a separate DSH profile/home; preserve this known baseline and production profile. Start by checking Typert factories, injected client services, session-controller APIs, storage schema compatibility, and the SSH/Slurm manual-refresh contract. Compare against the upstream remote rather than replacing this customization with its latest package.

Mimir's broad upstream peer ranges do not establish compatibility with a newer major DSH version. Only widen support after build, plugin boot/UI and bounded integration checks pass. Run cluster checks only when separately authorized.

## Contents and boundaries

The public fork/release contains source, tests, documentation and the compiled Mimir package. It excludes DSH credentials/settings, SSH keys, conversation history, research data, server configurations and jobs. Those need a separate private data backup if desired.
