<!-- markdownlint-disable MD053 -->

- Feature Name: (fill me in with a unique ident, `my_awesome_feature`)
- Start Date: (fill me in with today's date, YYYY-MM-DD)
- RFC PR: [apache/iggy#0000](https://github.com/apache/iggy/pull/0000)
- Discussion: [apache/iggy#0000](https://github.com/apache/iggy/discussions/0000)
- Iggy Issue: [apache/iggy#0000](https://github.com/apache/iggy/issues/0000)

## Summary

[summary]: #summary

One paragraph explanation of the feature.

## Motivation

[motivation]: #motivation

Any changes to Iggy should focus on solving a problem that users of Iggy are having.
This section should explain this problem in detail, including necessary background.

It should also contain several specific use cases where this feature can help a user, and explain how it helps.
This can then be used to guide the design of the feature.

This section is one of the most important sections of any RFC, and can be lengthy.

## Guide-level explanation

[guide-level-explanation]: #guide-level-explanation

Explain the proposal as if it was already included in Iggy and you were teaching it to another Iggy user. That generally means:

- Introducing new named concepts.
- Explaining the feature largely in terms of examples.
- Explaining how Iggy users should *think* about the feature, and how it should impact the way they use Iggy. It should explain the impact as concretely as possible.
- If applicable, provide sample error messages, deprecation warnings, or migration guidance.
- If applicable, describe the differences between teaching this to existing Iggy users and new Iggy users.
- Discuss how this impacts the ability to read, understand, and maintain Iggy code. Code is read and modified far more often than written; will the proposed feature make code easier to maintain?

For implementation-oriented RFCs (e.g. for server internals), this section should focus on how Iggy contributors should think about the change, and give examples of its concrete impact. For policy RFCs, this section should provide an example-driven introduction to the policy, and explain its impact in concrete terms.

## Reference-level explanation

[reference-level-explanation]: #reference-level-explanation

This is the technical portion of the RFC. Explain the design in sufficient detail that:

- Its interaction with other features is clear.
- It is reasonably clear how the feature would be implemented.
- Corner cases are dissected by example.

The section should return to the examples given in the previous section, and explain more fully how the detailed proposal makes those examples work.

## Drawbacks

[drawbacks]: #drawbacks

Why should we *not* do this?

## Rationale and alternatives

[rationale-and-alternatives]: #rationale-and-alternatives

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not choosing them?
- What is the impact of not doing this?

## Prior art

[prior-art]: #prior-art

Discuss prior art, both the good and the bad, in relation to this proposal.
A few examples of what this can include are:

- For server, SDK, protocol, and tooling proposals: Does this feature exist in other streaming or storage systems and what experience have their community had?
- For community proposals: Is this done by some other community and what were their experiences with it?
- For other teams: What lessons can we learn from what other communities have done here?
- Papers: Are there any published papers or great posts that discuss this? If you have some relevant papers to refer to, this can serve as a more detailed theoretical background.

This section is intended to encourage you as an author to think about the lessons from other systems, provide readers of your RFC with a fuller picture.
If there is no prior art, that is fine - your ideas are interesting to us whether they are brand new or if it is an adaptation from other systems.

Note that while precedent set by other systems is some motivation, it does not on its own motivate an RFC.
Please also take into consideration that Iggy sometimes intentionally diverges from common streaming system features.

## Unresolved questions

[unresolved-questions]: #unresolved-questions

- What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?
- What can be covered by the deterministic simulator, and what can only be tested in integration against real infrastructure or not at all?
