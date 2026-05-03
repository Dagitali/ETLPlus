# Branch Protection

This document defines the recommended GitHub branch protection configuration for the protected
`main` and `develop` branches when ETLPlus is operated with GitFlow.

- [Branch Protection](#branch-protection)
  - [Purpose](#purpose)
  - [Recommended Required Checks](#recommended-required-checks)
    - [Pull Request Baseline](#pull-request-baseline)
      - [Policy Categories](#policy-categories)
      - [Current Resolved Check Names](#current-resolved-check-names)
      - [Current Required Check Names To Select In GitHub](#current-required-check-names-to-select-in-github)
    - [Advisory Categories](#advisory-categories)
    - [Current Advisory Examples](#current-advisory-examples)
  - [Shared Protection Baseline](#shared-protection-baseline)
    - [Branch Protections](#branch-protections)
  - [Branch Protection Checklist For `main`](#branch-protection-checklist-for-main)
  - [Branch Protection Checklist For `develop`](#branch-protection-checklist-for-develop)
  - [How To Disallow Direct Pushes](#how-to-disallow-direct-pushes)
  - [How To Update Required Checks In GitHub](#how-to-update-required-checks-in-github)
  - [Maintenance Notes](#maintenance-notes)

## Purpose

These protections exist to enforce three repository policies:

- No direct pushes to `main`
- No direct pushes to `develop`
- No merge into either protected branch unless the required CI checks pass

Local hooks in `.pre-commit-config.yaml` complement this policy, but GitHub branch protection is
the authoritative enforcement layer in the current repository configuration.

## Recommended Required Checks

Choose the required-check baseline that matches how the repository accepts pull requests (PRs).

Because `.github/workflows/pr.yml` and `.github/workflows/ci.yml` both use matrices for Python
versions and docs builders, GitHub exposes expanded matrix job names in the branch protection UI
rather than the template names shown in the YAML. Select those expanded names when configuring
required checks.

The current workflow model is staged rather than post-merge:

- `.github/workflows/pr.yml` provides the lighter required pull-request baseline and the
  `merge_group` coverage used by merge queue.
- `.github/workflows/ci.yml` provides heavier pre-merge validation for pull requests targeting the
  protected `main` and `develop` branches.

That means the GitHub branch-protection policy should distinguish between the minimum merge gate
that always runs and the heavier pre-merge checks that you may choose to require for protected
branches.

### Pull Request Baseline

Use this baseline for protected-branch merge gates. It covers the checks that run for both
`pull_request` and `merge_group` in `pr.yml`.

#### Policy Categories

- GitFlow target-branch enforcement for pull requests
- Linting on the primary supported Python line
- Docstring linting on the primary supported Python line
- Tests on the primary supported Python line
- Type-checking on the primary supported Python line
- HTML docs build

These categories define the minimum merge gate for protected branches.

#### Current Resolved Check Names

In the current PR-gates workflow, the baseline above resolves to:

- `Guard PR target branch`
- `Lint on Python 3.13`
- `Test on Python 3.13`
- `Doclint on Python 3.13`
- `Type-check on Python 3.13`
- `Build docs (html)`

Additional CI jobs are still useful, and they can be made required for normal pull-request merges
into `main` and `develop` if you want the heavier pre-merge workflow to block those merges.

#### Current Required Check Names To Select In GitHub

When configuring `main` or `develop` branch protection rules in the GitHub UI, select these PR-gate
job names as the minimum required checks:

- `Guard PR target branch`
- `Lint on Python 3.13`
- `Test on Python 3.13`
- `Doclint on Python 3.13`
- `Type-check on Python 3.13`
- `Build docs (html)`

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

If you want the heavier pre-merge workflow to block normal pull-request merges into `main` and
`develop`, the natural next checks to add are:

- Lint on the next supported Python line
- Tests on the next supported Python line
- `Build docs (epub)`
- `Build docs (linkcheck)`
- `Smoke install on macos-latest`
- `Smoke install on windows-latest`
- `Build distributions`

That keeps the staged PR-gates and heavier-CI layout intact without collapsing everything back into
a single workflow. If merge queue is enabled, keep in mind that `ci.yml` currently runs only on
`pull_request`; add equivalent `merge_group` coverage there before making those heavier CI checks a
hard requirement for queued merges.

## Shared Protection Baseline

Apply this baseline to both protected branches:

- Require a PR before merging
- Dismiss stale approvals when new commits are pushed
- Require conversation resolution before merging
- Require status checks to pass before merging
- Require branches to be up to date before merging
- Add the required checks listed above
- If merge queue is enabled, keep the `merge_group` trigger in PR Gates so the same checks run for queued
  merges

In GitHub, these controls are typically split across PR rules, status check rules, and
branch protections.

### Branch Protections

- Block force pushes
- Block branch deletion
- Keep bypass actors empty if possible
- If bypass cannot be empty, restrict it to a very small maintainer/admin set

## Branch Protection Checklist For `main`

Target:

- Branch name pattern: `main`
- Enforcement: `Active`

Branch-specific additions:

- Require `2` approvals
- Require Code Owners review

Recommended baseline:

- Require the full pull-request baseline from `pr.yml`.
- Consider also requiring the heavier `ci.yml` jobs on `main` if you want release-oriented pull
  requests into `main` to satisfy the extended docs, smoke, and distribution validation before
  merge.

Optional hardening:

- Require signed commits
- Require merge queue

## Branch Protection Checklist For `develop`

Target:

- Branch name pattern: `develop`
- Enforcement: `Active`

Branch-specific additions:

- Require `1` approval

Recommended baseline:

- Require the full pull-request baseline from `pr.yml`.
- Consider also requiring the heavier `ci.yml` jobs on `develop` if you want the extended docs,
  smoke, and distribution validation to block feature integration into `develop`.

Optional hardening:

- Require Code Owners review for sensitive paths
- Require merge queue if `develop` receives enough concurrent PR traffic to make merge-order
  conflicts common

## How To Disallow Direct Pushes

The reliable way to disallow direct pushes is to protect the branch and require PRs. CI alone cannot
block a normal direct push after the fact, because GitHub Actions runs only after the push exists.

This repository currently applies these controls with classic branch protection on `main` and
`develop`, not repository rulesets.

In GitHub:

1. Open repository `Settings`.
2. Open `Branches`.
3. Open the branch protection rule for `main` or `develop`.
4. Enable `Require a PR before merging`.
5. For `main`, enable `Require review from Code Owners`.
6. Enable `Require status checks to pass before merging`.
7. Enable `Require branches to be up to date before merging`.
8. Enable `Block force pushes`.
9. Enable `Block deletions`.
10. Remove bypass actors unless there is a strict operational need.

## How To Update Required Checks In GitHub

After the workflow split and the later `ci.yml` trigger redesign, update the protected-branch
protections in GitHub so the minimum required checks come from `pr.yml`, then add the heavier
`ci.yml` checks if you want that extended pre-merge validation to be blocking on `main` and
`develop`.

In GitHub:

1. Open repository `Settings`.
2. Open `Branches`.
3. Open the branch protection rule for `main`.
4. Under `Require status checks to pass`, remove any stale checks emitted by the heavier CI or old
   workflow filenames.
5. Add these minimum required checks from `pr.yml`:
  - `Guard PR target branch`
  - `Lint on Python 3.13`
  - `Test on Python 3.13`
  - `Doclint on Python 3.13`
  - `Type-check on Python 3.13`
  - `Build docs (html)`
6. If you want the heavier pre-merge `ci.yml` workflow to block ordinary PR merges into `main` and
   `develop`, also add these checks:
  - `Build docs (epub)`
  - `Build docs (linkcheck)`
  - `Smoke install on macos-latest`
  - `Smoke install on windows-latest`
  - `Build distributions`
7. Save the `main` branch protection rule.
8. Repeat the same status-check set for the `develop` branch protection rule unless you
  intentionally want a different protected-branch policy.

Do not mark the heavier `ci.yml` checks as required if you rely on merge queue and have not added
matching `merge_group` coverage there.

With that configuration in place:

- Contributors push to feature, bugfix, hotfix, release, chore, ci, or docs branches
- Feature branches cannot merge into `main` while `Guard PR target branch` is required
- Pull requests into `main` and `develop` carry both the PR-gates results and the heavier CI
  results
- `main` and `develop` cannot be updated directly by ordinary pushes
- Merges remain blocked until the required checks pass

## Maintenance Notes

- GitHub required checks are tied to the exact job names emitted by the PR-gates workflow after
  matrix expansion. In this repository, that means branch protection should reference concrete names
  such as `Guard PR target branch` and `Lint on Python 3.13`, not the template strings shown in the
  YAML.
- Treat version-specific and OS-specific names in this document as current examples, not permanent
  policy. When the support matrix changes, refresh the exact examples here and in the GitHub branch
  protection UI to match the emitted checks.
- The heavier CI jobs now run on `pull_request` into `main` and `develop`. If you depend on merge
  queue and want those jobs to remain required there, add equivalent `merge_group` coverage before
  enforcing them for queued merges.
- The PR-target guard intentionally enforces these GitFlow merge paths: `feature/* -> develop`,
  `release/* -> main`, and `hotfix/* -> main`.
- The local `no-commit-to-branch` pre-commit hook should protect `main` and `develop`, but it is
  only a contributor convenience. GitHub branch protection remains authoritative.
