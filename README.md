# Async RPC

**Resonate Go SDK**

This example demonstrates Resonate's async RPC capabilities with three dispatch patterns side-by-side against a shared `echo` leaf function.

- [Await-chain](#await-chain)
- [Detached](#detached)
- [Fan-out](#fan-out)

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

- Go 1.22 or later
- No external server required — the example uses the in-process `localnet` transport by default

To run against a live Resonate server instead:

```shell
brew install resonatehq/tap/resonate
resonate serve --storage-type=sqlite
```

## Clone

```shell
git clone https://github.com/resonatehq-examples/example-async-rpc-go
cd example-async-rpc-go
```

## Run it

Run all three patterns in sequence (uses in-process localnet):

```shell
go run . -mode=all
```

Run a single pattern:

```shell
go run . -mode=chain
go run . -mode=detached
go run . -mode=fanout
```

Run against a live Resonate server:

```shell
go run . -mode=all -url=http://localhost:8001
```

### Expected output (`-mode=all`)

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

## Learn more

- [Resonate Documentation](https://docs.resonatehq.io)
- [Go SDK](https://docs.resonatehq.io/develop/go)
