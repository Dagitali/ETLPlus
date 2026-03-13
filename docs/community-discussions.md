# GitHub Discussions Guide

This guide describes a recommended GitHub Discussions setup for ETLPlus so the project can actively
support questions, documentation feedback, examples, and other codeless contributions.

- [GitHub Discussions Guide](#github-discussions-guide)
  - [Recommended Categories](#recommended-categories)
    - [Q\&A](#qa)
    - [Docs Feedback](#docs-feedback)
    - [Examples and Showcases](#examples-and-showcases)
    - [Ideas](#ideas)
    - [Announcements](#announcements)
  - [Routing Guidance](#routing-guidance)
  - [Pinned Starter Discussions](#pinned-starter-discussions)
    - [1. Ask ETLPlus Questions Here](#1-ask-etlplus-questions-here)
    - [2. Docs Feedback and Missing Examples](#2-docs-feedback-and-missing-examples)
    - [3. Share Your ETLPlus Pipelines and Examples](#3-share-your-etlplus-pipelines-and-examples)
  - [Operating Routine](#operating-routine)
  - [Suggested First Actions](#suggested-first-actions)

## Recommended Categories

Create the following discussion categories in GitHub:

### Q&A

Use for:

- Usage questions
- Troubleshooting help
- Best-practice advice
- Interpretation of CLI and configuration behavior

Why:

- Questions can be answered and marked as solved
- This keeps support requests out of Issues unless they are confirmed bugs

### Docs Feedback

Use for:

- Unclear README or docs sections
- Missing examples
- Requests for better explanations
- Typo and wording reports that may or may not need a PR

Why:

- Lowers the bar for documentation contributions
- Encourages feedback before someone writes a docs PR

### Examples and Showcases

Use for:

- Real-world ETLPlus pipelines
- Reusable configs and workflow snippets
- Demonstrations of connectors and transforms
- Community examples and implementation notes

Why:

- Gives users a place to contribute practical value without modifying source
- Helps build a searchable knowledge base around usage patterns

### Ideas

Use for:

- Feature suggestions
- Roadmap discussion
- Design questions
- Connector proposals

Why:

- keeps early-stage proposals out of Issues until they become actionable work

### Announcements

Use for:

- Release updates
- Sponsorship/support updates
- Roadmap notes
- Calls for testing or feedback

Why:

- Gives maintainers a durable place for project-wide communication

Recommended restriction:

- Maintainers only for creating announcement threads

## Routing Guidance

Use this rule across the project:

- Discussions: questions, docs feedback, examples, ideas, support conversations
- Issues: confirmed bugs, concrete feature work, tracked tasks
- Security: follow [SECURITY.md](../SECURITY.md)

## Pinned Starter Discussions

Create and pin these three discussions after enabling GitHub Discussions.

### 1. Ask ETLPlus Questions Here

Suggested title:

`Ask ETLPlus questions here`

Suggested body:

```md
Use this thread for ETLPlus questions that do not belong in the issue tracker.

Good examples:

- How to model a pipeline config
- How to choose file formats or connector types
- How to troubleshoot CLI usage
- How to structure transforms or validations

When possible, include:

- The command you ran
- The relevant config or input sample
- The expected result
- The actual result
- Your Python version and platform

If the discussion uncovers a confirmed bug or a specific feature request, we can turn it into an issue.
```

### 2. Docs Feedback and Missing Examples

Suggested title:

`Docs feedback and missing examples`

Suggested body:

```md
Use this thread to report documentation gaps, confusing sections, missing examples, or tutorial ideas.

Helpful feedback includes:

- Which page or section was unclear
- What you expected to find
- What example or explanation would have helped
- Whether you are willing to open a PR or just want to report the gap

Documentation feedback is a valuable contribution even when it does not come
with code.
```

### 3. Share Your ETLPlus Pipelines and Examples

Suggested title:

`Share your ETLPlus pipelines and examples`

Suggested body:

```md
Use this thread to share ETLPlus examples, pipeline snippets, connector usage
patterns, and practical workflows.

Examples that are especially useful:

- File-to-file pipelines
- API extraction and pagination setups
- Transform and validation patterns
- Reusable configuration structures
- Lessons learned from real usage

If a shared example is broadly useful, we may promote it into the official docs or examples folder.
```

## Operating Routine

To keep Discussions active rather than decorative:

1. Review new discussions at least a few times each week.
2. Mark answers in Q&A discussions.
3. Convert confirmed bugs or actionable work into Issues.
4. Fold good answers and examples back into the docs.
5. Use Announcements for release notes, calls for testing, and support updates.

## Suggested First Actions

1. Enable GitHub Discussions in repository settings.
2. Create the five categories above.
3. Publish and pin the three starter discussions.
4. Point users to Discussions from the README, SUPPORT, and CONTRIBUTING docs.
5. Use Discussions for support and examples, and reserve Issues for tracked work.
