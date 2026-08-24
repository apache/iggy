# Security Policy

Apache Iggy follows the [Apache Software Foundation security process](https://www.apache.org/security/).
Please read this before reporting anything you believe is a security issue.

## Reporting a Vulnerability

**Do not report security vulnerabilities through public channels.** That means no GitHub issues, pull
requests or discussions, no Discord, and no social media. Public disclosure before a fix is available
puts users at risk.

Send reports to **[security@apache.org](mailto:security@apache.org)**.

Iggy does not currently have its own project security list, so `security@apache.org` is the correct
address. The ASF Security Team will forward your report to the Iggy PMC's private list and confirm to
you that they have done so.

When reporting, please:

- send one plain-text, unencrypted email per vulnerability
- describe the issue in the body rather than attaching images, video, HTML or PDF
- include the affected version or commit, the component (server, SDK and language, CLI, connector,
  MCP server, web UI), the transport in use where relevant, and the configuration required to hit it
- include reproduction steps and your assessment of the impact

## What Happens Next

1. The ASF Security Team acknowledges receipt and forwards the report to the Iggy PMC.
2. We acknowledge the report and investigate. We will tell you whether we accept or reject it, and why.
3. If accepted, a CVE ID is allocated. The ASF Security Team is the CNA for all Apache projects and is
   the only body that can assign CVE IDs to Apache software.
4. We develop the fix in private. There will be no public issue, and the commit message will not
   indicate that the change is security related.
5. The fix ships in a release. The advisory is published at or after the release announcement and is
   sent to the reporter, the project's announcement destinations, the ASF Security Team, and
   [oss-security](https://www.openwall.com/lists/oss-security/).

We will share the draft advisory with you before publication and credit you unless you prefer
otherwise. Please keep the report confidential until the announcement.

## Out of Scope

The following are not treated as vulnerabilities in Apache Iggy:

- automated scanner or dependency-checker output with no demonstrated exploit
- reports of vulnerabilities in third-party dependencies with no demonstrated exploitable path through
  Iggy itself; see the ASF guidance on
  [dependency advisories](https://security.apache.org/report-dependency/)
- behaviour available to a user who has legitimately been granted the necessary privileges, such as an
  administrator reconfiguring the server
- missing hardening or defence-in-depth suggestions with no accompanying exploit

Iggy does not yet publish a formal security model describing the trust boundaries between server
operators, authenticated clients, and connector plugins. Until it does, treat the list above as
guidance rather than a firm boundary. If you are unsure whether something qualifies, report it.

## References

- [ASF Security Team](https://www.apache.org/security/)
- [Reporting security issues in ASF code](https://security.apache.org/report-code/)
- [Vulnerability handling for committers](https://www.apache.org/security/committers.html)
