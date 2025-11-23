# Contributing

First, thank you for contributing to Iggy! The goal of this document is to provide everything you need to start contributing to Iggy. The following TOC is sorted progressively, starting with the basics and expanding into more specifics.

## Your First Contribution

1. Ensure your change has an issue! Find an [existing issue](https://github.com/apache/iggy/issues) or open a new issue.
2. [Fork the Iggy repository](https://github.com/apache/Iggy/fork) in your own GitHub account.
3. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
4. Make your changes.
5. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the master Iggy repo. An Iggy team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.

## Workflow

### Git Branches

*All* changes must be made in a branch and submitted as [pull requests](#github-pull-requests). Iggy does not adopt any type of branch naming style, but please use something descriptive of your changes.

### GitHub Pull Requests

Once your changes are ready you must submit your branch as a [pull request](https://github.com/apache/Iggy/pulls).

#### Title

The pull request title must follow the format outlined in the [conventional commits spec](https://www.conventionalcommits.org). [Conventional commits](https://www.conventionalcommits.org) is a standardized format for commit messages. Iggy only requires this format for commits on the `master` branch. And because Iggy squashes commits before merging branches, this means that only the pull request title must conform to this format.

The following are all good examples of pull request titles:

```text
fix(ci): add Cross.toml for CI builds
chore(repo): add ASF license header to all the files
refactor(server): remove redundant sha1 print
chore(docs): add contributing guide
refactor(server): remove redundant sha1 print
```

#### Reviews & Approvals

All pull requests should be reviewed by at least two Iggy committers.

#### Merge Style

All pull requests are squash merged.
We generally discourage large pull requests that are over 300–500 lines of diff.
If you would like to propose a change that is larger, we suggest
coming onto our [Discussions](https://github.com/apache/Iggy/discussions) and discussing it with us.
This way we can talk through the solution and discuss if a change that large is even needed!
This will produce a quicker response to the change and likely produce code that aligns better with our process.

### CI

Currently, Iggy uses GitHub Actions to run tests. The workflows are defined in `.github/workflows`.

## Setup

### Bring your own toolbox

Iggy is primarily a Rust project. To build Iggy, you will need to set up Rust development first. We highly recommend using [rustup](https://rustup.rs/) for the setup process.

For Linux or macOS, use the following command:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

For Windows, download `rustup-init.exe` from [the Windows installer](https://win.rustup.rs/x86_64) instead.

Rustup will read Iggy's `Cargo.toml` and set up everything else automatically. To ensure that everything works correctly, run `cargo version` under Iggy's root directory:

```shell
$ cargo version
cargo 1.86.0 (adf9b6ad1 2025-02-28)
```

### Pre-commit Hooks (Recommended)

Iggy uses [prek](https://github.com/9999years/prek) for pre-commit hooks to ensure code quality before commits. Setting up hooks is optional but strongly recommended to catch issues early.

#### Setup

```shell
cargo install prek
prek install
```

This will install git hooks that automatically run on `pre-commit` and `pre-push` events.

#### Manual hook execution

You can manually run specific hooks:

```shell
# Run all pre-commit hooks
prek run

# Run specific hook
prek run cargo-fmt
prek run cargo-clippy
```

#### Skip hooks (when necessary)

If you need to skip hooks for a specific commit:

```shell
git commit --no-verify -m "your message"
```

## How to build

See [Quick Start](https://github.com/apache/iggy?tab=readme-ov-file#quick-start)
