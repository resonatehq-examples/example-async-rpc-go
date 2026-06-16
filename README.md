# Async RPC | Resonate Go SDK

Three async-RPC dispatch patterns side-by-side against a shared `echo` leaf function: await-chain, detached, and fan-out.

> Heads up — `resonate-sdk-go` is pre-release. The SDK has no semver tag yet, so this example pins to a specific commit. Expect API changes until `v0.1.0`.

## What this demonstrates

### Await-chain

`ctx.RPC` dispatches a remote function and returns a `*Future`. Calling `future.Await(&out)` blocks the workflow execution on that promise before moving to the next step. The chain proceeds step-by-step and the caller receives the final result.

Key API: `ctx.RPC(funcName, args) → (*Future, error)` + `future.Await(&out)`

### Detached

`ctx.Detached` fires a remote function and returns only the spawned promise ID. The parent workflow returns without waiting. The child's lifecycle is logically independent of the parent — on a live Resonate server the child promise persists and can be claimed by any worker in the group even after the parent process exits.

Key API: `ctx.Detached(funcName, args, opts...) → (string, error)`

### Fan-out

Multiple `ctx.RPC` calls are dispatched in a first loop without calling `Await` between them — each call is non-blocking at dispatch time. A second loop awaits every future in order to aggregate results. The remote calls execute concurrently server-side; the two-loop structure keeps the dispatch and collect phases cleanly separated.

Key API: collect `[]*Future` in one pass, `future.Await(&out)` in a second pass.

## Prerequisites

- Go 1.22+
- The `resonate` server CLI (only required for `-url` mode — the example defaults to in-process localnet). Install with Homebrew on macOS or Linux:
  ```
  brew install resonatehq/tap/resonate
  ```
  Other install paths: <https://docs.resonatehq.io/get-started/install>.

## Setup

```sh
git clone https://github.com/resonatehq-examples/example-async-rpc-go.git
cd example-async-rpc-go
go mod download
```

## Run it

Run all three patterns in sequence (uses in-process localnet, no external server):

```sh
go run . -mode=all
```

Run a single pattern:

```sh
go run . -mode=chain
go run . -mode=detached
go run . -mode=fanout
```

Run against a live Resonate server:

```sh
resonate dev                              # in another terminal
go run . -mode=all -url=http://localhost:8001
```

## What to look for

Expected output (`-mode=all`):

```
=== await-chain ===
[chain] starting workflow id=chain-<ns>
  [echo] echo("hello (step 1)") from chain
  [chain] step 1 result: echo("hello (step 1)") from chain
  [echo] echo("hello (step 2)") from chain
  [chain] step 2 result: echo("hello (step 2)") from chain
  [echo] echo("hello (step 3)") from chain
  [chain] step 3 result: echo("hello (step 3)") from chain
[chain] done: chain complete: ...

=== detached ===
[detached] starting workflow id=detached-<ns>
  [detached] dispatched child promise id=<id>
[detached] parent done: detached child dispatched (id=<id>)
  [echo] echo("fire and forget") from detached

=== fan-out ===
[fanout] starting workflow id=fanout-<ns> recipients=[alice bob carol dave]
  [echo] echo("hello -> alice") from fanout
  [echo] echo("hello -> bob") from fanout
  [echo] echo("hello -> carol") from fanout
  [echo] echo("hello -> dave") from fanout
[fanout] done
  echo("hello -> alice") from fanout
  ...
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-mode` | `all` | Pattern to run: `chain`, `detached`, `fanout`, or `all` |
| `-url` | (empty) | Resonate server URL. Empty uses in-process localnet. |

## Notes on `ctx.Detached` and localnet

On localnet the child promise is dispatched and executed within the same in-process server state machine. The parent returning before the child completes is observable in the log (parent prints "done" before `[echo]` prints), but the true cross-process durability story requires a live server.

When using `ctx.Detached` on localnet, pass `DetachedOpts{Target: "default"}` explicitly to match the group name passed to `localnet.NewLocal`. Omitting the target causes the default resolver to return an empty string; the localnet `accepts()` filter passes an empty address, so the dispatch still works — but explicit is clearer.

## File structure

```
example-async-rpc-go/
├── main.go        program entry point with all three patterns
├── go.mod         module declaration + SDK pin
├── go.sum         checksums
├── LICENSE        Apache-2.0
└── README.md
```

## Next steps

- **Coming from Temporal?** See [MIGRATING-FROM-TEMPORAL.md](MIGRATING-FROM-TEMPORAL.md) — a side-by-side port of the matching `temporalio/samples-go` example.
- [Get started](https://docs.resonatehq.io/get-started) — install paths + first-program walkthrough.
- [Durable execution concepts](https://docs.resonatehq.io/concepts) — what makes invocations durable + how the runtime resumes them.
- [`example-fan-out-fan-in-go`](https://github.com/resonatehq-examples/example-fan-out-fan-in-go) — the fan-out pattern with a typed aggregation step.

## Community

- Discord: <https://resonatehq.io/discord>
- X: <https://x.com/resonatehqio>
- LinkedIn: <https://linkedin.com/company/resonatehq>
- YouTube: <https://youtube.com/@resonatehq>
- Journal: <https://journal.resonatehq.io>

## License

[Apache-2.0](./LICENSE)
