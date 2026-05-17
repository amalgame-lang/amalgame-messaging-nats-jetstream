# NOTICE — amalgame-messaging-nats-jetstream

## Authorship

Copyright 2026 Bastien Mouget. Original work — see
`runtime/Amalgame_Messaging_NATS_JetStream.h`.

Part of the Amalgame ecosystem
([github.com/amalgame-lang/Amalgame](https://github.com/amalgame-lang/Amalgame)).
External contributions are paused at the ecosystem level; see the
main repo's `CONTRIBUTING.md` for the policy.

AI tools (Anthropic Claude) were used during development. Per
the project's authorship policy, AI is treated as a tool, not a
co-author at law.

## Licence

Apache License 2.0. See `LICENSE` for the full text.

## Third-party content

**None vendored.** This package implements the NATS Core wire
protocol (PUB / SUB / MSG / +OK / -ERR / PING / PONG) inline in
C, and layers the JetStream API ($JS.API.STREAM.*, $JS.API.CONSUMER.*)
on top using ordinary NATS requests with JSON bodies. No code from
the official `nats.c` client, no `dr_libs`, no header from
[github.com/nats-io](https://github.com/nats-io) ships with this
package.

NATS itself is a CNCF graduated project under Apache 2.0. The
wire protocol is documented at
[docs.nats.io/reference/reference-protocols](https://docs.nats.io/reference/reference-protocols/nats-protocol)
and stable across server versions.

## Trademarks

None claimed. "NATS" and "JetStream" are trademarks of Synadia
Communications Inc., used here descriptively to identify
compatibility with the upstream protocol.
