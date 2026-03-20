# Branch Rulesets

This document defines the recommended GitHub rulesets for the protected
`main` and `develop` branches when ETLPlus is operated with GitFlow.

## Purpose

These rulesets exist to enforce three repository policies:

- No direct pushes to `main`
- No direct pushes to `develop`
- No merge into either protected branch unless the required CI checks pass

Local hooks in `.pre-commit-config.yaml` complement this policy, but GitHub rulesets are the
authoritative enforcement layer.

## Recommended Required Checks

Use one simplified required-check baseline for both `main` and `develop`.

Because `.github/workflows/ci.yml` uses matrices for Python versions, docs builders, and operating
systems, GitHub exposes expanded matrix job names in the ruleset UI rather than the template names
shown in the YAML. Select those expanded names when configuring required checks.

### Policy Categories

- Lint on the primary supported Python line
- Tests on the primary supported Python line
- Type-checking on the primary supported Python line
- HTML docs build
- One non-Linux smoke install job
- Distribution build validation

These categories define the minimum merge gate for protected branches.

### Current Resolved Check Names

In the current CI workflow, the baseline above resolves to:

- `Lint on Python 3.13`
- `Test on Python 3.13`
- `Type-check on Python 3.13`
- `Build docs (html)`
- `Smoke install on macos-latest`
- `Build distributions`

Additional CI jobs are still useful, but they should usually stay advisory unless you intentionally
want a stricter gate.

### Advisory Categories

- Lint on additional supported Python lines
- Tests on additional supported Python lines
- Docstring linting
- Non-HTML docs builders
- The second cross-platform smoke install job

### Current Advisory Examples

In the current CI workflow, those advisory categories resolve to:

- `Lint on Python 3.14`
- `Test on Python 3.14`
- `Doclint on Python 3.13`
- `Build docs (epub)`
- `Build docs (linkcheck)`
- `Smoke install on windows-latest`

If you want a stricter protected-branch gate, the natural next checks to add are:

- Lint on the next supported Python line
- Tests on the next supported Python line
- The second cross-platform smoke install job

That keeps the matrix-driven coverage intact without abandoning the DRY CI layout.

## Shared Ruleset Baseline

Apply this baseline to both protected branches:

- Require a pull request before merging
- Dismiss stale approvals when new commits are pushed
- Require conversation resolution before merging
- Require status checks to pass before merging
- Require branches to be up to date before merging
- Add the required checks listed above
- If merge queue is enabled, keep the `merge_group` trigger in CI so the same checks run for queued
  merges

In GitHub, these controls are typically split across pull request rules, status check rules, and
branch protections.

### Branch Protections

- Block force pushes
- Block branch deletion
- Keep bypass actors empty if possible
- If bypass cannot be empty, restrict it to a very small maintainer/admin set

## Ruleset Checklist For `main`

Target:

- Branch name pattern: `main`
- Enforcement: `Active`

Branch-specific additions:

- Require `2` approvals
- Require Code Owners review if `CODEOWNERS` is later added

Optional hardening:

- Require signed commits
- Require merge queue

## Ruleset Checklist For `develop`

Target:

- Branch name pattern: `develop`
- Enforcement: `Active`

Branch-specific additions:

- Require `1` approval

Optional hardening:

- Require Code Owners review for sensitive paths if `CODEOWNERS` is later added
- Require merge queue if `develop` receives enough concurrent PR traffic to make merge-order
  conflicts common

## How To Disallow Direct Pushes

The reliable way to disallow direct pushes is to protect the branch and require pull requests. CI
alone cannot block a normal direct push after the fact, because GitHub Actions runs only after the
push exists.

In GitHub:

1. Open repository `Settings`.
2. Open `Rules`.
3. Open `Rulesets`.
4. Create one ruleset for `main` and one for `develop`.
5. Target the corresponding branch name.
6. Enable `Require a pull request before merging`.
7. Enable `Require status checks to pass before merging`.
8. Enable `Require branches to be up to date before merging`.
9. Enable `Block force pushes`.
10. Enable `Block deletions`.
11. Remove bypass actors unless there is a strict operational need.

With that configuration in place:

- Contributors push to feature, bugfix, hotfix, release, chore, ci, or docs branches
- Pull requests carry the CI results
- `main` and `develop` cannot be updated directly by ordinary pushes
- Merges remain blocked until the required checks pass

## Maintenance Notes

- GitHub required checks are tied to the exact job names emitted by the CI workflow after matrix
  expansion. In this repository, that means the ruleset should reference concrete names such as
  `Lint on Python 3.13`, not the template string shown in the YAML.
- Treat version-specific and OS-specific names in this document as current examples, not permanent
  policy. When the support matrix changes, refresh the exact examples here and in the GitHub ruleset
  UI to match the emitted checks.
- The `Build distributions` job is conditionally skipped for pull requests from forks. If
  fork-origin pull requests are a normal part of your workflow, confirm how your GitHub ruleset
  treats skipped required checks before making `Build distributions` mandatory, or relax that
  requirement for branches that regularly accept external fork PRs.
- The local `no-commit-to-branch` pre-commit hook should protect `main` and `develop`, but it is
  only a contributor convenience. GitHub rulesets remain authoritative.
