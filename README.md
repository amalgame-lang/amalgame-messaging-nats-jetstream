# amalgame-messaging-nats-jetstream

NATS JetStream client for [Amalgame](https://github.com/amalgame-lang/Amalgame).
Persistent streams, durable pull consumers, at-least-once delivery — built directly on the NATS Core wire protocol over TCP, no vendored client lib.

Independent of [amalgame-messaging-nats](https://github.com/amalgame-lang/amalgame-messaging-nats) (NATS Core): you can take JetStream alone without pulling the Pub/Sub package. The two share zero code, only the wire protocol.

## Prerequisites

A running NATS server **with JetStream enabled** (`-js` flag). The package itself has no build-time system deps beyond what amc already requires.

### Runtime server

| Setup | Command |
|---|---|
| Docker | `docker run --rm -p 4222:4222 nats:2-alpine -js -sd /tmp` |
| Linux native | `nats-server -js -sd /tmp` (after [installing nats-server](https://docs.nats.io/running-a-nats-service/introduction/installation)) |
| macOS | `brew install nats-server && nats-server -js -sd /tmp` |
| Windows | Download `nats-server.exe` from [nats.io](https://nats.io/download) and run `nats-server.exe -js -sd .` |

`-js` activates JetStream, `-sd /tmp` is the storage directory for stream data (any writable path works).

### Client side (Amalgame toolchain)

| OS / distro | Install |
|---|---|
| Debian / Ubuntu | `apt install gcc libgc-dev libcurl4-openssl-dev` |
| Fedora / RHEL | `dnf install gcc gc-devel libcurl-devel` |
| Arch / Manjaro | `pacman -S gcc gc curl` |
| Alpine | `apk add build-base gc-dev curl-dev` |
| macOS | `brew install bdw-gc curl` (Xcode CLT supplies the compiler) |
| Windows (MSYS2) | `pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-gc mingw-w64-x86_64-curl` |

## Install

```bash
amc package add messaging-nats-jetstream                                            # via index
amc package add github.com/amalgame-lang/amalgame-messaging-nats-jetstream@v0.1.0
```

Requires **amc 0.5.4+** (precompile-on-install — though JetStream's runtime is header-only, so the `[stdlib].precompile=true` toggle wouldn't help here).

## Surface (v0.1)

```amalgame
import Amalgame.Messaging.NATS.JetStream

public class Program {
    public static void Main() {
        let js = JetStream.Open("127.0.0.1", 4222)
        if (!JetStream.IsOpen(js)) {
            Console.WriteLine("connect failed: " + JetStream.LastError(js))
            return
        }

        // Create a stream that captures any subject under orders.>
        let subjects = new List<string>()
        subjects.Add("orders.>")
        JetStream.StreamCreate(js, "ORDERS", subjects)

        // Publish — blocks until the server confirms with PubAck.
        JetStream.Publish(js, "orders.created",  "{\"id\":42}")
        JetStream.Publish(js, "orders.shipped",  "{\"id\":42}")
        Console.WriteLine("last seq: " + String_FromInt(JetStream.LastSeq(js)))

        // Pull consumer — durable, at-least-once, explicit ack.
        JetStream.ConsumerCreate(js, "ORDERS", "WORKER")
        let n = JetStream.ConsumerPull(js, "ORDERS", "WORKER", 10, 2000)
        for i in 0..n {
            Console.WriteLine("got: " + JetStream.MessageData(js, i)
                            + " on " + JetStream.MessageSubject(js, i))
        }
        JetStream.AckAll(js)
        JetStream.Close(js)
    }
}
```

### Method table

| Method | Returns | Notes |
|---|---|---|
| **Lifecycle** | | |
| `JetStream.Open(host, port)` | `AmalgameJetStream*` | TCP connect + CONNECT handshake |
| `JetStream.Close(js)` | `void` | Shuts the TCP fd |
| `JetStream.IsOpen(js)` | `bool` | False after Close / on connect failure |
| `JetStream.LastError(js)` | `string` | Empty when no error |
| **Stream** | | |
| `JetStream.StreamCreate(js, name, subjects)` | `bool` | File storage, default limits |
| `JetStream.StreamDelete(js, name)` | `bool` | Idempotent — error if absent |
| `JetStream.StreamInfo(js, name)` | `string` | Raw JSON from `$JS.API.STREAM.INFO.<name>` |
| **Publish** | | |
| `JetStream.Publish(js, subject, payload)` | `bool` | Blocks until PubAck arrives |
| `JetStream.LastSeq(js)` | `int` | Sequence number from last PubAck |
| **Pull consumer** | | |
| `JetStream.ConsumerCreate(js, stream, name)` | `bool` | Durable, AckExplicit, AckPolicy=explicit |
| `JetStream.ConsumerPull(js, stream, name, batch, timeoutMs)` | `int` | Message count (0 on timeout) |
| `JetStream.MessageSubject(js, idx)` | `string` | 0 ≤ idx < pull-count |
| `JetStream.MessageData(js, idx)` | `string` | Payload as-is |
| `JetStream.MessageReply(js, idx)` | `string` | Ack inbox subject |
| `JetStream.AckAll(js)` | `void` | ACKs every message from last pull |

## NATS Core vs JetStream — which package to pick

| Need | Package |
|---|---|
| Ephemeral pub/sub, fire-and-forget | [amalgame-messaging-nats](https://github.com/amalgame-lang/amalgame-messaging-nats) |
| Request/Reply (sync RPC over subjects) | amalgame-messaging-nats |
| Queue groups (load-balanced fanout) | amalgame-messaging-nats |
| Persistent streams, replay from sequence | **this package** |
| Durable consumers (re-delivery after restart) | **this package** |
| At-least-once delivery semantics | **this package** |

Both can run against the same server (`nats-server -js -sd /tmp` accepts Core and JetStream traffic simultaneously). The two packages don't share a header — pick one, both, or both with distinct connections.

## Deferred to v0.2+

- KV store (`kv_bucket_create`, `put`, `get`, `watch`)
- ObjectStore (chunked object IO)
- Push consumers (`deliver_subject`, more complex flow control)
- AckNak / AckTerm / AckProgress (only AckAck for now)
- JS domain prefix (multi-tenant deployments)
- TLS, JWT / NKEY / user-pass auth

## Tests

```bash
./tests/run_tests.sh /path/to/amc
```

Tests probe `127.0.0.1:4222` for a JetStream-enabled NATS server. If reachable, the end-to-end flow runs (create stream → publish 3 → pull 3 → ack → delete stream). If not, every case SKIPs (no infrastructure surprise).

The CI workflow at [.github/workflows/ci.yml](.github/workflows/ci.yml) runs the suite against `nats:2-alpine -js` in a service container on every PR.

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md). The
package contains no vendored third-party code (NATS protocol is text on TCP).
