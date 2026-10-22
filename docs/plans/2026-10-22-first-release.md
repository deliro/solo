# First usable release

**Goal:** Publish a verified Solo v0.1.0 with downloadable Linux/macOS binaries and reproducible installation instructions.

**Architecture:** Keep the existing lease protocol and shared PostgreSQL test harness. Correct demonstrated test-environment assumptions without relaxing behavioral assertions. Use the existing tag-triggered release workflow.

**Tech stack:** Rust, PostgreSQL 17, testcontainers, GitHub Actions, gh.

**Constraints:** Preserve ownership, shutdown, timeout, and commit guarantees. Keep all supported release targets. Publish only after CI succeeds.

**Non-goals:** New scheduling features, schema changes, or a package-manager distribution channel.

## Task 1: Diagnose and stabilize concurrent-start tests

Files: `tests/cli/scenarios.rs:30`, `tests/cli/common/mod.rs:56`; development guidance in `CONTRIBUTING.md`.

1. RED: `cargo test --locked --test cli`; compare with CI run 39679073731, which failed contention during connect at 200ms and first acquisition while waiting for busy outcomes.
2. Run the two scenarios separately and rerun the failed Linux CI job without changes. Inspect logs to distinguish timeout assumptions from duplicate ownership.
3. If connection budgets are the cause, give only concurrent-start scenarios realistic budgets while retaining simultaneous launch, one winner, five successful skips, and no unexpected handlers. Preserve tight deadlines in tests that assert timeout behavior.
4. GREEN: run the focused scenarios, formatting, Clippy, all pure tests, and the complete Linux integration suite. Document any durable test-harness distinction in `CONTRIBUTING.md`.
5. Run the complete suite locally on macOS as an additional platform check. The existing retry-shutdown scenarios reproduce `kill_group` when SIGTERM leaves an unreaped zombie leader; reap the exited child before SIGKILL and retain the group signal for descendants. Use `/usr/bin/true`, present on both platforms, for native-handler tests. Rerun the original failure scenarios and all checks.

## Task 2: Document installation from the actual release

Files: `README.md:9`, `README_ru.md:9`, `docs/usage.md:1`.

1. Confirm that the current release download is absent using `gh release view v0.1.0 --repo deliro/solo`.
2. Add concrete download, checksum, extraction, installation, and version-check commands using the existing target table. Keep both README languages consistent and correct stale guide labels.
3. Verify commands against published artifacts in Task 3. Review the actual diff for security, reliability, and rule routing.

## Task 3: Publish and verify

Files: existing `.github/workflows/ci.yml` and `.github/workflows/release.yml`; change them only for demonstrated release blockers.

1. Commit and push the verified fix and documentation, then wait for all CI jobs to pass.
2. Create annotated `v0.1.0` matching Cargo's version and push it. Wait for the release workflow to finish successfully.
3. Download all four archives and `SHA256SUMS`; verify checksums and contents. Run the native downloaded binary's version/help and an end-to-end local PostgreSQL attempt, deduplication, and status check.
4. Ensure the worktree is clean and `CLAUDE.md` still points to `AGENTS.md`. Update release notes with practical installation and guarantee boundaries if generated notes are insufficient.

Rollback: stop publication on failing checks. Before publication, repair a failed workflow and retry its existing tag without replacing a published release or moving a public version tag.
