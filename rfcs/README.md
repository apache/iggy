<!-- markdownlint-disable MD053 -->

# Iggy RFCs

[Iggy RFCs]: #iggy-rfcs

The "RFC" (request for comments) process is intended to provide a consistent
and controlled path for changes to Iggy (such as new features) so that all
stakeholders can be confident about the direction of the project.

Many changes, including bug fixes and documentation improvements can be
implemented and reviewed via the normal GitHub pull request workflow.

Some changes though are "substantial", and we ask that these be put through a
bit of a design process and produce a consensus among the Iggy community and
the [maintainers].

## Table of Contents

[Table of Contents]: #table-of-contents

- [Opening](#iggy-rfcs)
- [Table of Contents]
- [When you need to follow this process]
- [Before creating an RFC]
- [What the process is]
- [RFC numbering]
- [The RFC life-cycle]
- [Reviewing RFCs]
- [Implementing an RFC]
- [RFC Postponement]
- [Help this is all too informal]
- [License]
- [Contributions]

## When you need to follow this process

[When you need to follow this process]: #when-you-need-to-follow-this-process

You need to follow this process if you intend to make "substantial" changes to
the Iggy server, the wire protocol, the SDKs, the connectors runtime, or the RFC
process itself. What constitutes a "substantial" change is evolving based on
community norms and varies depending on what part of the project you are
proposing to change, but may include the following.

- Any change to the wire protocol or the on-disk format that is not a bugfix.
- A new storage, replication, or consumption model, or a change to how an
  existing one behaves.
- Removing features, including those behind a feature flag.
- Large additions to the SDK surface that every SDK must then implement.

Some changes do not require an RFC:

- Rephrasing, reorganizing, refactoring, or otherwise "changing shape does
  not change meaning".
- Additions that strictly improve objective, numerical quality criteria
  (warning removal, speedup, better platform coverage, more parallelism, trap
  more errors, etc.)
- Additions only likely to be *noticed by* other developers-of-iggy,
  invisible to users-of-iggy.

If you submit a pull request to implement a new feature without going through
the RFC process, it may be closed with a polite request to submit an RFC first.

## Before creating an RFC

[Before creating an RFC]: #before-creating-an-rfc

A hastily-proposed RFC can hurt its chances of acceptance. Low quality
proposals, proposals for previously-rejected features, or those that don't fit
into the near-term roadmap, may be quickly rejected, which can be demotivating
for the unprepared contributor. Laying some groundwork ahead of the RFC can
make the process smoother.

Although there is no single way to prepare for submitting an RFC, it is
generally a good idea to pursue feedback from other project developers
beforehand, to ascertain that the RFC may be desirable; having a consistent
impact on the project requires concerted effort toward consensus-building.

The most common preparations for writing and submitting an RFC include talking
the idea over on our [Discord server], discussing the topic in [GitHub
Discussions], and occasionally posting "pre-RFCs" there. A pre-RFC thread is
separate from the RFC's own Discussion, which is opened later, once the text
exists and has had its initial review. You may file issues on this repo for
discussion, but these are not actively looked at by the maintainers.

As a rule of thumb, receiving encouraging feedback from long-standing project
developers, and particularly from the [maintainers], is a good indication that
the RFC is worth pursuing.

## What the process is

[What the process is]: #what-the-process-is

In short, to get a major feature added to Iggy, one must first get the RFC
merged into the `rfcs/` directory of this repository as a markdown file. At
that point the RFC is "active" and may be implemented with the goal of eventual
inclusion into Iggy.

- Fork the [Iggy repository].
- Copy `rfcs/rfc-template.md` to `rfcs/rfc-0-my-feature.md` (where
  "my-feature" is descriptive). Don't assign an RFC number yet; `0` marks an
  unassigned number, and one is assigned when the RFC is accepted and the file
  is renamed accordingly. See [RFC numbering].
- Fill in the RFC. Put care into the details: RFCs that do not present
  convincing motivation, demonstrate lack of understanding of the design's
  impact, or are disingenuous about the drawbacks or alternatives tend to
  be poorly-received.
- Submit a pull request titled `rfc(<scope>): <subject>`, where the scope is
  one of those accepted by the PR title check. The pull request carries the
  text. A maintainer gives it an initial review within a week: whether the
  change needs an RFC at all, whether the text is complete enough to debate,
  and editorial comments on the writing.
- Now that your RFC has an open pull request, update the "RFC PR" link at the
  top of the file to point at it.
- Once a maintainer marks the RFC ready for discussion, open a GitHub
  Discussion in the RFCs category titled `RFC: <feature>`, linking the pull
  request and quoting the summary, and update the "Discussion" link at the top
  of the file to point at it. The Discussion is where the design is debated;
  the pull request keeps editorial review.
- Build consensus and integrate feedback. RFCs that have broad support are
  much more likely to make progress than those that don't receive any
  comments. Feel free to reach out to the maintainers in particular to get
  help identifying stakeholders and obstacles.
- The maintainers will discuss the RFC, as much as possible in the Discussion
  itself. Offline discussion will be summarized in the Discussion.
- RFCs rarely go through this process unchanged, especially as alternatives
  and drawbacks are shown. You can make edits, big and small, to the RFC to
  clarify or change the design, but make changes as new commits to the pull
  request, and reply in the Discussion with the commit hash and permalinks to
  the changed lines, explaining your changes. **Specifically, do not squash or
  rebase commits after they are visible on the pull request.**
- At some point, a maintainer will propose a "motion for final comment period"
  (FCP) in the Discussion, along with a *disposition* for the RFC (merge,
  close, or postpone).
  - This step is taken when enough of the tradeoffs have been discussed that
    the maintainers are in a position to make a decision. That does not require
    consensus amongst all participants in the Discussion (which is usually
    impossible). However, the argument supporting the disposition on the RFC
    needs to have already been clearly articulated, and there should not be a
    strong consensus *against* that position outside of the maintainers.
    Maintainers use their best judgment in taking this step, and the FCP itself
    ensures there is ample time and notification for stakeholders to push
    back if it is made prematurely.
  - For RFCs with lengthy discussion, the motion to FCP is usually preceded by
    a *summary comment* trying to lay out the current state of the discussion
    and major tradeoffs/points of disagreement.
  - Before actually entering FCP, the maintainers must sign off; this is often
    the point at which many of them first review the RFC in full depth.
- The FCP lasts ten calendar days, so that it is open for at least 5 business
  days. It is also advertised on the [dev mailing list]. This way all
  stakeholders have a chance to lodge any final objections before a decision
  is reached.
- In most cases, the FCP period is quiet, and the RFC is either merged or
  closed. However, sometimes substantial new arguments or ideas are raised,
  the FCP is canceled, and the RFC goes back into development mode.

## RFC numbering

[RFC numbering]: #rfc-numbering

RFC numbers are small sequential integers without zero padding, assigned when
an RFC is accepted, so the first RFC is `rfc-1`. Whoever merges the pull request
renames `rfcs/rfc-0-my-feature.md` to the next free number, for example
`rfcs/rfc-7-my-feature.md`, fills in the "Iggy Issue" link with the tracking
issue opened for the implementation, and marks the Discussion as answered with
links to the merged RFC and that issue.

Pull request numbers are not used as RFC numbers because this repository's
counter is shared with every other pull request, which would make RFC numbers
large, sparse, and meaningless as a sequence. While an RFC is under review it
is referred to by its feature name.

## The RFC life-cycle

[The RFC life-cycle]: #the-rfc-life-cycle

Once an RFC becomes "active" then authors may implement it and submit the
feature as a pull request to the Iggy repository. Being "active" is not a
rubber stamp, and in particular still does not mean the feature will ultimately
be merged; it does mean that in principle all the major stakeholders have
agreed to the feature and are amenable to merging it.

Furthermore, the fact that a given RFC has been accepted and is "active"
implies nothing about what priority is assigned to its implementation, nor does
it imply anything about whether an Iggy developer has been assigned the task of
implementing the feature. While it is not *necessary* that the author of the
RFC also write the implementation, it is by far the most effective way to see
an RFC through to completion: authors should not expect that other project
developers will take on responsibility for implementing their accepted feature.

Modifications to "active" RFCs can be done in follow-up pull requests. We
strive to write each RFC in a manner that it will reflect the final design of
the feature; but the nature of the process means that we cannot expect every
merged RFC to actually reflect what the end result will be at the time of the
next major release.

In general, once accepted, RFCs should not be substantially changed. Only very
minor changes should be submitted as amendments. More substantial changes
should be new RFCs, with a note added to the original RFC. Exactly what counts
as a "very minor change" is up to the maintainers to decide.

## Reviewing RFCs

[Reviewing RFCs]: #reviewing-rfcs

While the RFC is open, the maintainers may schedule a call with the author
and/or relevant stakeholders to discuss the issues in greater detail, or
discuss the topic on Discord. In either case a summary will be posted back to
the RFC's Discussion.

The maintainers make final decisions about RFCs after the benefits and
drawbacks are well understood. These decisions can be made at any time, but the
maintainers will regularly issue decisions. When a decision is made, the RFC
pull request will either be merged or closed. In either case, if the reasoning
is not clear from the Discussion, the maintainers will add a comment there
describing the rationale for the decision.

## Implementing an RFC

[Implementing an RFC]: #implementing-an-rfc

Some accepted RFCs represent vital features that need to be implemented right
away. Other accepted RFCs can represent features that can wait until some
arbitrary developer feels like doing the work. Every accepted RFC has an
associated issue tracking its implementation in the Iggy repository; thus that
associated issue can be assigned a priority via the triage process that the
maintainers use for all issues in the Iggy repository.

The author of an RFC is not obligated to implement it. Of course, the RFC
author (like any other developer) is welcome to post an implementation for
review after the RFC has been accepted.

If you are interested in working on the implementation for an "active" RFC, but
cannot determine if someone else is already working on it, feel free to ask
(e.g. by leaving a comment on the associated issue).

### Help this is all too informal

[Help this is all too informal]: #help-this-is-all-too-informal

The process is intended to be as lightweight as reasonable for the present
circumstances. As usual, we are trying to let the process be driven by
consensus and community norms, not impose more structure than necessary.

## License

[License]: #license

The contents of this directory are licensed under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0),
like the rest of the repository.

### Contributions

[Contributions]: #contributions

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
licensed as above, without any additional terms or conditions.

[Discord server]: https://discord.gg/apache-iggy
[GitHub Discussions]: https://github.com/apache/iggy/discussions
[dev mailing list]: mailto:dev@iggy.apache.org
[Iggy repository]: https://github.com/apache/iggy
[maintainers]: https://github.com/apache/iggy/blob/master/.asf.yaml
