# Coming from Temporal: Async RPC Dispatch (Await-Chain, Detached, Fan-Out)

This example maps to three patterns from `temporalio/samples-go`: the sequential child-workflow await-chain from [`child-workflow`](https://github.com/temporalio/samples-go/tree/main/child-workflow), fire-and-forget via `ParentClosePolicy: ABANDON` from [`batch-sliding-window`](https://github.com/temporalio/samples-go/tree/main/batch-sliding-window), and parallel dispatch from [`splitmerge-future`](https://github.com/temporalio/samples-go/tree/main/splitmerge-future). The goal is to help you port any or all three patterns to Resonate.

## The pattern

All three patterns are variations on dispatching durable function calls from inside a running workflow:

- **Await-chain** — dispatch a child call, block until it finishes, then dispatch the next one. Sequential by design.
- **Detached** — fire a child call and return immediately. The parent does not hold a future; the child's lifecycle is independent of the parent's.
- **Fan-out** — dispatch all child calls in one loop without blocking, then collect results in a second loop. The children run concurrently; both loops are required to avoid serialising execution.

In Temporal, all three involve `workflow.ExecuteChildWorkflow` (or `workflow.ExecuteActivity`); the differences live in `ChildWorkflowOptions` and whether you call `.Get` immediately. In Resonate, the same distinctions live in `ctx.RPC` vs `ctx.Detached` and whether you call `f.Await` before the next dispatch.

## Side by side

### Await-chain

#### Temporal (`samples-go/child-workflow`)

```go
// parent_workflow.go
func SampleParentWorkflow(ctx workflow.Context) (string, error) {
    logger := workflow.GetLogger(ctx)

    cwo := workflow.ChildWorkflowOptions{
        WorkflowID: "ABC-SIMPLE-CHILD-WORKFLOW-ID",
    }
    ctx = workflow.WithChildOptions(ctx, cwo)

    var result string
    err := workflow.ExecuteChildWorkflow(ctx, SampleChildWorkflow, "World").Get(ctx, &result)
    if err != nil {
        logger.Error("Parent execution received child execution failure.", "Error", err)
        return "", err
    }

    logger.Info("Parent execution completed.", "Result", result)
    return result, nil
}
```

#### Resonate (this example)

```go
// main.go — awaitChain
func awaitChain(ctx *resonate.Context, args ChainArgs) (string, error) {
    var results [3]string
    for i := 0; i < 3; i++ {
        f, err := ctx.RPC("echo", EchoArgs{
            Message: fmt.Sprintf("%s (step %d)", args.Message, i+1),
            From:    "chain",
        })
        if err != nil {
            return "", fmt.Errorf("RPC step %d: %w", i+1, err)
        }
        // Await blocks the workflow execution on this promise.
        // When the promise is pending, Await suspends the workflow internally
        // (via panic/recover) and the runtime replays once the promise settles.
        if err := f.Await(&results[i]); err != nil {
            return "", fmt.Errorf("Await step %d: %w", i+1, err)
        }
        fmt.Printf("  [chain] step %d result: %s\n", i+1, results[i])
    }
    return fmt.Sprintf("chain complete: %s | %s | %s", results[0], results[1], results[2]), nil
}
```

---

### Detached (fire-and-forget)

#### Temporal (`samples-go/batch-sliding-window`)

In Temporal, a child workflow that must outlive its parent uses `ChildWorkflowOptions` with `ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON`. In `batch-sliding-window`, the parent accumulates dispatched children across iterations and calls `ContinueAsNew` once a sliding-window limit is reached; ABANDON ensures those children keep running when the parent re-starts. The parent calls `.GetChildWorkflowExecution().Get(ctx, nil)` on each child to confirm it has started before moving on — waiting for start, not completion — then proceeds to accumulate more dispatches before eventually calling `ContinueAsNew`.

```go
// batch-sliding-window/sliding_window_workflow.go (excerpt)
options := workflow.ChildWorkflowOptions{
    // Use ABANDON as child workflows have to survive the parent calling continue-as-new
    ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
    WorkflowID:        fmt.Sprintf("%s/%d", workflowId, record.Id),
}
childCtx := workflow.WithChildOptions(ctx, options)
child := workflow.ExecuteChildWorkflow(childCtx, RecordProcessorWorkflow, record)
// child.GetChildWorkflowExecution().Get(ctx, nil) — wait for start, not completion
```

The `PARENT_CLOSE_POLICY_ABANDON` constant comes from `go.temporal.io/api/enums/v1`.

#### Resonate (this example)

```go
// main.go — detachedWorkflow
func detachedWorkflow(ctx *resonate.Context, args DetachedArgs) (string, error) {
    // ctx.Detached returns the child promise ID only — there is no Future.
    // The parent workflow can complete before the child does.
    // Pass DetachedOpts{Target: "default"} to route the child to the correct
    // worker group; omitting Target uses the Context's resolver default.
    childID, err := ctx.Detached("echo", EchoArgs{
        Message: args.Message,
        From:    "detached",
    }, resonate.DetachedOpts{Target: "default"})
    if err != nil {
        return "", fmt.Errorf("Detached: %w", err)
    }
    fmt.Printf("  [detached] dispatched child promise id=%s\n", childID)
    return fmt.Sprintf("detached child dispatched (id=%s)", childID), nil
}
```

---

### Fan-out

#### Temporal (`samples-go/splitmerge-future`)

```go
// splitmerge_workflow.go
func SampleSplitMergeFutureWorkflow(ctx workflow.Context, processorCount int) (ChunkResult, error) {
    ao := workflow.ActivityOptions{
        StartToCloseTimeout: 10 * time.Second,
    }
    ctx = workflow.WithActivityOptions(ctx, ao)

    var results []workflow.Future
    for i := 0; i < processorCount; i++ {
        // ExecuteActivity returns Future that doesn't need to be awaited immediately.
        future := workflow.ExecuteActivity(ctx, ChunkProcessingActivity, i+1)
        results = append(results, future)
    }

    var totalItemCount, totalSum int
    for i := 0; i < processorCount; i++ {
        var result ChunkResult
        // Blocks until the activity result is available.
        err := results[i].Get(ctx, &result)
        if err != nil {
            return ChunkResult{}, err
        }
        totalItemCount += result.NumberOfItemsInChunk
        totalSum += result.SumInChunk
    }

    workflow.GetLogger(ctx).Info("Workflow completed.")
    return ChunkResult{totalItemCount, totalSum}, nil
}
```

#### Resonate (this example)

```go
// main.go — fanout
func fanout(ctx *resonate.Context, args FanoutArgs) (FanoutResult, error) {
    // Loop 1: dispatch — collect futures without blocking.
    futures := make([]*resonate.Future, 0, len(args.Recipients))
    for _, r := range args.Recipients {
        f, err := ctx.RPC("echo", EchoArgs{
            Message: fmt.Sprintf("%s -> %s", args.Message, r),
            From:    "fanout",
        })
        if err != nil {
            return FanoutResult{}, fmt.Errorf("RPC for %s: %w", r, err)
        }
        futures = append(futures, f)
    }

    // Loop 2: await — block on each future in order.
    result := FanoutResult{Acks: make([]string, 0, len(futures))}
    for i, f := range futures {
        var ack string
        if err := f.Await(&ack); err != nil {
            return FanoutResult{}, fmt.Errorf("Await for %s: %w", args.Recipients[i], err)
        }
        result.Acks = append(result.Acks, ack)
    }
    return result, nil
}
```

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.ExecuteChildWorkflow(ctx, fn, args).Get(ctx, &out)` | `f, _ := ctx.RPC("name", args); f.Await(&out)` | Await-chain: dispatch then block immediately |
| `workflow.ExecuteActivity(ctx, fn, args)` → collect `[]workflow.Future` → `.Get(ctx, &r)` | `ctx.RPC("name", args)` → collect `[]*resonate.Future` → `f.Await(&r)` | Fan-out: same two-loop structure |
| `ChildWorkflowOptions{ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON}` + `ExecuteChildWorkflow` | `ctx.Detached("name", args, resonate.DetachedOpts{Target: "..."}) (string, error)` | Detached: no future returned; child promise ID is the only handle |
| `workflow.WithChildOptions(ctx, cwo)` | `resonate.DetachedOpts{Target: "groupName"}` | Routing: Temporal routes via task queue + options context; Resonate routes via `Target` group name |
| `workflow.WithActivityOptions(ctx, ao)` with `StartToCloseTimeout` | `resonate.RPCOpts{Timeout: d}` (in-workflow, per-call) or `resonate.RPCOptions{Timeout: d, Target: "..."}` (top-level, via `r.RPC`) | `RPCOpts` is used with `ctx.RPC` inside a workflow; `RPCOptions` is used with `r.RPC` at the top level and additionally carries a `Target` field. They are distinct, non-interchangeable types. Temporal requires at least one timeout on `ActivityOptions`; both Resonate types make timeout optional. |
| `w.RegisterWorkflow(fn)` + `w.RegisterActivity(fn)` | `resonate.Register(r, "name", fn)` | Single registration for any durable function; no workflow/activity distinction |
| Task queue (routes work to a worker pool) | `group` (set in `localnet.NewLocal` or `httpnet.HTTPOptions`) | Conceptual equivalent; name must match between registration and `DetachedOpts.Target` |
| Workflow ID (stable idempotency key) | Promise ID (passed as `id` to `fn.Run` or generated by SDK for child calls) | Same role: stable ID gates deduplication |

## Porting it, step by step

### Await-chain

1. Remove `workflow.WithChildOptions` and `workflow.ChildWorkflowOptions`. There is no options-context threading.
2. Replace `workflow.ExecuteChildWorkflow(ctx, SampleChildWorkflow, arg)` with `f, err := ctx.RPC("registeredName", arg)`. The function must be registered under that name with `resonate.Register`.
3. Replace `.Get(ctx, &result)` (chained on the future) with a separate `err := f.Await(&result)` call.
4. Keep the same sequential structure: `RPC` → `Await` → next `RPC` → `Await`.

### Detached

1. Replace `workflow.ExecuteChildWorkflow` + `PARENT_CLOSE_POLICY_ABANDON` with `childID, err := ctx.Detached("name", args, resonate.DetachedOpts{Target: "groupName"})`.
2. There is no future to hold. `ctx.Detached` returns `(string, error)` — the string is the child promise ID. The parent can return immediately.
3. If you need to observe the child later, fetch it from outside the workflow via `h, err := r.Get(ctx, childID)` on the client side, then call `err = h.Result(ctx, &out)` to block until it settles. For a typed result you can use `out, err := resonate.ResultOf[T](ctx, h)` instead.
4. On localnet, set `Target` to match the group name passed to `localnet.NewLocal`. Omitting `Target` routes deterministically to the worker's own group (the SDK calls `resolveTarget("")` which returns `network.TargetResolver(network.Group())`). Set an explicit `Target` only when you need to dispatch to a different worker group.

### Fan-out

1. Replace the activity-options context setup (`workflow.WithActivityOptions`) with nothing — per-call options in Resonate are optional and passed as `resonate.RPCOpts{Timeout: d}` if needed.
2. Loop 1: replace `workflow.ExecuteActivity(ctx, fn, i+1)` with `f, err := ctx.RPC("name", args)`. Append each `f` to a `[]*resonate.Future` slice. Do not call `f.Await` inside this loop.
3. Loop 2: replace `results[i].Get(ctx, &result)` with `f.Await(&result)`.
4. "Start all before awaiting any" is the rule in both systems. Calling `Await` inside the dispatch loop turns fan-out into a sequential chain.

## What's different (and why)

**No workflow/activity distinction.** In Temporal, `workflow.ExecuteChildWorkflow` and `workflow.ExecuteActivity` are distinct because child workflows run in a deterministic sandbox (with `workflow.Context`) while activities run with a plain `context.Context` and can do arbitrary I/O. Resonate has one concept: a durable function. Any function registered with `resonate.Register` can be invoked via `ctx.RPC` (remote dispatch) or `ctx.Detached` (fire-and-forget remote dispatch); unregistered inline functions can be run in-process via `ctx.Run`. The SDK handles replay via durable promise caching rather than an event log. The practical effect is that you do not choose at registration time whether something is a "workflow" or "activity" — the SDK decides how to durably track it based on the promise.

**`ctx.Detached` has no future.** In Temporal, even with `PARENT_CLOSE_POLICY_ABANDON`, `workflow.ExecuteChildWorkflow` returns a `ChildWorkflowFuture` and you can call `.GetChildWorkflowExecution().Get(ctx, nil)` to wait for the child to start before the parent returns. `ctx.Detached` returns only the promise ID string. If you need a start-confirmation before the parent returns, use `ctx.RPC` (which does have a future) and simply do not call `Await` before returning — though that prevents the parent from observing the result. The right tool depends on whether you need the promise ID for external observation or true decoupled fire-and-forget.

**Replay model is structurally the same.** Both systems re-execute the workflow function body from the top on resume. In Temporal, completed steps are short-circuited via the event history. In Resonate, they are short-circuited via settled durable promises keyed by a worker-generated child promise ID. Side effects outside `ctx.Run` / `ctx.RPC` / `ctx.Detached` / `ctx.Sleep` / `ctx.Promise` will re-run on every resume in both systems.

**Routing via group, not task queue.** Temporal routes work to workers via a named task queue specified at worker startup and in `StartWorkflowOptions`/`ActivityOptions`. Resonate uses a `group` name, set either in `localnet.NewLocal("groupName", ...)` or in `httpnet.HTTPOptions.Group`, and referenced in `DetachedOpts.Target`. The concepts are equivalent but the wiring is different: in Temporal the task queue is always explicit; in Resonate it defaults to `"default"` and only needs to be named explicitly when you have multiple worker groups.

**Fan-out awaits in dispatch order, not completion order.** `splitmerge-future` awaits in the order activities were dispatched (`results[i].Get`). This is identical to the Resonate two-loop pattern. If you want to process results in completion order, Temporal's `splitmerge-selector` example uses `workflow.NewSelector`; Resonate does not have a built-in selector equivalent yet — you would need to track which futures have settled via repeated `Await` calls or redesign the aggregation step.

## Notes & coverage

**`ctx.Run` vs `workflow.Go`.** Temporal also supports lightweight in-workflow goroutines via `workflow.Go` for concurrent work that runs inside the parent and does not outlive it. `ctx.Run` is the closer structural match: it spawns an in-process goroutine that is joined to the parent via `flushLocalWork` (which calls `wg.Wait`) before the parent suspends or returns — the same lifecycle contract as `workflow.Go`. `ctx.Run` additionally records a durable promise so the result survives replay. `ctx.Detached` is the structural opposite: its promise is not joined to the parent and explicitly outlives it.

**Localnet and child dispatch timing.** On localnet, `ctx.Detached` dispatches the child within the same in-process state machine. The parent returning before the child executes is visible in the log (the parent prints "done" before `[echo]` prints) but cross-process durability requires a live Resonate server. The example adds a `time.Sleep(200 * time.Millisecond)` in `runDetached` as a convenience to let the localnet actor dispatch the child before the process exits — you would not do this in production.

**`resonate.Register` returns two values.** A bare `resonate.Register(r, "name", fn)` call that discards both return values compiles fine but silently drops the registration error and the `*RegisteredFunc` you need to start workflows — so the registration may have failed and you have no handle to call `.Run()` on. Assigning only one of the two return values (`fn := resonate.Register(...)`) is a compile error. Always capture both: `chainFn, err := resonate.Register(r, "name", fn)`. The returned `*RegisteredFunc` is the handle you pass to `.Run(ctx, id, args)` to start a top-level workflow invocation.

**Fan-out completion order.** If you await in loop order and the first child is slow, the second child's result sits idle until the first finishes. This is the same trade-off in `splitmerge-future`. Plan your aggregation shape accordingly.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal `child-workflow` sample: https://github.com/temporalio/samples-go/tree/main/child-workflow
- Temporal `batch-sliding-window` sample (ParentClosePolicy ABANDON): https://github.com/temporalio/samples-go/tree/main/batch-sliding-window
- Temporal `splitmerge-future` sample: https://github.com/temporalio/samples-go/tree/main/splitmerge-future
- This example's README
