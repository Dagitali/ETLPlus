# Maintainer Runbooks

Maintainer-facing runbooks for working with protected branches, GitHub pull requests, and
tag-triggered releases.

This file is intentionally kept outside `docs/source/` so it lives with the repository without
being published to [Read the Docs][Read the Docs].

- [Maintainer Runbooks](#maintainer-runbooks)
  - [Operating Model](#operating-model)
  - [Feature Branch Runbook](#feature-branch-runbook)
  - [Release Branch Runbook](#release-branch-runbook)
  - [Hotfix Branch Runbook](#hotfix-branch-runbook)
  - [Sync Main Back To Develop](#sync-main-back-to-develop)
  - [Tagging And CD](#tagging-and-cd)
  - [Solo-Maintainer Notes](#solo-maintainer-notes)
  - [Keep Private Elsewhere](#keep-private-elsewhere)

## Operating Model

ETLPlus uses GitFlow-style branch names together with GitHub-protected `develop` and `main`
branches.

The important consequence is:

- working happens on `feature/*`, `release/*`, and `hotfix/*`
- authoritative integration happens through GitHub pull requests
- `develop` and `main` should be treated as GitHub-managed integration branches, not as branches
  that are finished locally and pushed afterward

Local `git flow feature finish` and `git flow release finish` may still be useful as personal
shortcuts for branch cleanup or experimentation, but they should not be treated as the final source
of truth for protected branches.

This file stays at the policy and high-level workflow layer. Sensitive operator details should live
outside the public repository.

## Feature Branch Runbook

Use for normal development work.

1. Create a feature branch from `develop`.
2. Commit and push the branch.
3. Open a pull request from `feature/*` into `develop`.
4. Let `.github/workflows/pr.yml` satisfy the required PR checks.
5. Merge the PR in GitHub.
6. Delete the feature branch after merge if it is no longer needed.

## Release Branch Runbook

Use for release stabilization and promotion.

1. Create `release/<version>` from `develop`.
2. Stabilize the release branch with only release-targeted changes.
3. Open a pull request from `release/*` into `main`.
4. Merge the PR in GitHub after the required checks pass.
5. Create or move the annotated release tag so it points at the merged `main` commit.
6. Push the tag.
7. Confirm `.github/workflows/cd.yml` runs from that tag.
8. Sync the resulting `main` state back into `develop` explicitly.

## Hotfix Branch Runbook

Use for production fixes that must land on `main` first.

1. Create `hotfix/<version>` from `main`.
2. Apply and validate the fix.
3. Open a pull request from `hotfix/*` into `main`.
4. Merge the PR in GitHub after the required checks pass.
5. Create or move the annotated hotfix tag so it points at the merged `main` commit.
6. Push the tag.
7. Sync the resulting `main` state back into `develop` explicitly.

## Sync Main Back To Develop

After a release or hotfix lands on `main`, update `develop` deliberately rather than assuming both
protected branches are already aligned.

Preferred sequence:

1. Pull the latest `main` and `develop` locally.
2. Create a temporary sync branch from `develop` if you want a reviewable sync PR.
3. Merge `main` into that sync branch or directly into local `develop`.
4. If branch protection or review policy requires it, open a PR into `develop`.
5. Merge once checks pass.

## Tagging And CD

`cd.yml` is tag-driven.

- A successful `ci.yml` run does not trigger `cd.yml`.
- `cd.yml` runs only when a `v*.*.*` tag is pushed.
- The tag should point at the authoritative merged `main` commit for the release.
- Use annotated tags for releases.

## Solo-Maintainer Notes

It is normal for a solo maintainer to use pull requests for both feature and release branches once
protected branches are enabled.

The pull request still provides value even without another human reviewer:

- Required checks run on the proposed branch change before the protected branch moves
- GitHub becomes the authoritative merge surface for protected branches
- Release and branch history stay aligned with the repository protection model

For a solo-maintainer repository, it is reasonable to keep the policy lightweight:

- One required approval on `develop`, or an intentionally documented exception path
- Stricter review on `main` only if that is useful for release discipline
- Narrow admin bypass only when necessary and documented in `.github/BRANCH-PROTECTION.md`

## Keep Private Elsewhere

If ETLPlus needs deeper operator documentation, keep it outside this public repository.

Examples that should live in a truly private location include:

- Exact release execution checklists and recovery commands
- Secrets, credential rotation, or trusted-publisher maintenance steps
- Emergency branch-protection bypass procedures
- Security-incident response details
- Account recovery or succession notes

[Read the Docs]: https://readthedocs.io
