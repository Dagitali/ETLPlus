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

Choose the required-check baseline that matches how the repository accepts pull requests.

Because `.github/workflows/pr.yml` and `.github/workflows/ci.yml` use matrix-expanded job names,
GitHub exposes the expanded names in the ruleset UI rather than the template names shown in the
YAML. Select those expanded names when configuring required checks.

The heavier post-merge validation now lives in `.github/workflows/ci.yml`, where it
chains from successful push completions of `PR Gates`. Those checks should usually stay advisory
for pull-request rulesets because they do not run on `pull_request`.

### Pull Request Baseline

Use this baseline for protected-branch merge gates. It covers the checks that run for both
`pull_request` and `merge_group` in `pr.yml`.

#### Policy Categories

- Linting on the primary supported Python line
- Docstring linting on the primary supported Python line
- Tests on the primary supported Python line
- Type-checking on the primary supported Python line
- HTML docs build

These categories define the minimum merge gate for protected branches.

#### Current Resolved Check Names

In the current PR-gates workflow, the baseline above resolves to:

- `Lint on Python 3.13`
- `Test on Python 3.13`
- `Doclint on Python 3.13`
- `Type-check on Python 3.13`
- `Build docs (html)`

Additional CI jobs are still useful, but they should usually stay advisory unless you intentionally
want a stricter gate.

### Advisory Categories

- Lint on additional supported Python lines
- Tests on additional supported Python lines
- Non-HTML docs builders
- Cross-platform smoke install jobs
- Distribution build validation

### Current Advisory Examples

In the current PR-gates workflow, those advisory categories resolve to:

- `Lint on Python 3.14`
- `Test on Python 3.14`

In the current CI workflow, those advisory categories also include:

- `Build docs (epub)`
- `Build docs (linkcheck)`
- `Smoke install on macos-latest`
- `Smoke install on windows-latest`
- `Build distributions`

If you want a stricter protected-branch gate, the natural next checks to add are:

- Lint on the next supported Python line
- Tests on the next supported Python line
- One non-Linux smoke install job from `ci.yml`
- The post-merge `Build docs (epub)` job

That keeps the staged PR-gates and CI layout intact without collapsing everything back into a single
required workflow.

## Shared Ruleset Baseline

Apply this baseline to both protected branches:

- Require a pull request before merging
- Dismiss stale approvals when new commits are pushed
- Require conversation resolution before merging
- Require status checks to pass before merging
- Require branches to be up to date before merging
- Add the required checks listed above
- If merge queue is enabled, keep the `merge_group` trigger in PR Gates so the same checks run for queued
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
- Require Code Owners review

Recommended baseline:

- Require the full pull-request baseline from `pr.yml`.
- Keep `Build distributions` and the cross-platform smoke jobs advisory unless you intentionally
  want post-merge validation to block promotion decisions outside the ruleset.

Optional hardening:

- Require signed commits
- Require merge queue

## Ruleset Checklist For `develop`

Target:

- Branch name pattern: `develop`
- Enforcement: `Active`

Branch-specific additions:

- Require `1` approval

Recommended baseline:

- Require the full pull-request baseline from `pr.yml`.
- Keep the heavier CI workflow advisory so `develop` stays fast enough for normal GitFlow
  integration traffic.

Optional hardening:

- Require Code Owners review for sensitive paths
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
7. For `main`, enable `Require review from Code Owners`.
8. Enable `Require status checks to pass before merging`.
9. Enable `Require branches to be up to date before merging`.
10. Enable `Block force pushes`.
11. Enable `Block deletions`.
12. Remove bypass actors unless there is a strict operational need.

With that configuration in place:

- Contributors push to feature, bugfix, hotfix, release, chore, ci, or docs branches
- Pull requests carry the CI results
- `main` and `develop` cannot be updated directly by ordinary pushes
- Merges remain blocked until the required checks pass

## Maintenance Notes

- GitHub required checks are tied to the exact job names emitted by the PR-gates workflow after matrix
  expansion. In this repository, that means the ruleset should reference concrete names such as
  `Lint on Python 3.13`, not the template string shown in the YAML.
- Treat version-specific and OS-specific names in this document as current examples, not permanent
  policy. When the support matrix changes, refresh the exact examples here and in the GitHub ruleset
  UI to match the emitted checks.
- The heavier CI jobs run only after successful push completions of `PR Gates` on `main`, `develop`,
  `release/*`, and `hotfix/*`. Do not configure those job names as required PR checks unless you
  also change their trigger model.
- The local `no-commit-to-branch` pre-commit hook should protect `main` and `develop`, but it is
  only a contributor convenience. GitHub rulesets remain authoritative.
