// Package main demonstrates three async-RPC dispatch patterns using the
// Resonate Go SDK and an in-process localnet transport (no external server
// required).
//
// Run a single pattern:
//
//	go run . -mode=chain
//	go run . -mode=detached
//	go run . -mode=fanout
//
// Run all three in sequence:
//
//	go run . -mode=all
//
// Point at a live Resonate server instead of localnet:
//
//	go run . -mode=all -url=http://localhost:8001
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// ── Shared leaf function ─────────────────────────────────────────────────────

// EchoArgs is the argument type for the shared echo leaf function.
type EchoArgs struct {
	Message string `json:"message"`
	From    string `json:"from"`
}

// echo is the shared leaf function that every pattern dispatches to. It
// accepts a message, prints it, and returns an acknowledgement string.
func echo(_ *resonate.Context, args EchoArgs) (string, error) {
	ack := fmt.Sprintf("echo(%q) from %s", args.Message, args.From)
	fmt.Printf("  [echo] %s\n", ack)
	return ack, nil
}

// ── Pattern 1: Await-chain ────────────────────────────────────────────────────
//
// The workflow dispatches three RPC calls in series. Each call blocks (via
// Future.Await) before the next one is dispatched. The caller receives the
// result of the final call.

// ChainArgs carries the root message for the await-chain workflow.
type ChainArgs struct {
	Message string `json:"message"`
}

// awaitChain calls echo three times in sequence, blocking on each result
// before dispatching the next call.
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

func runChain(r *resonate.Resonate, chainFn *resonate.RegisteredFunc[ChainArgs, string]) error {
	ctx := context.Background()
	id := fmt.Sprintf("chain-%d", time.Now().UnixNano())
	fmt.Printf("[chain] starting workflow id=%s\n", id)

	h, err := chainFn.Run(ctx, id, ChainArgs{Message: "hello"})
	if err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	result, err := h.Result(ctx)
	if err != nil {
		return fmt.Errorf("Result: %w", err)
	}
	fmt.Printf("[chain] done: %s\n", result)
	return nil
}

// ── Pattern 2: Detached ───────────────────────────────────────────────────────
//
// The workflow fires a remote call with ctx.Detached, which returns only the
// spawned promise ID. The parent workflow returns immediately without waiting
// on the child. On localnet the child is still executed in-process, but the
// lifecycle is logically independent of the parent.
//
// Limitation: on localnet the child executes within the same process so the
// "outlives the parent" property is structural but not observable across a
// process boundary. With a real Resonate server the child promise persists
// server-side and can be picked up by any worker in the group.

// DetachedArgs carries the message for the detached workflow.
type DetachedArgs struct {
	Message string `json:"message"`
}

// detachedWorkflow fires a single echo via ctx.Detached and returns the child
// promise ID immediately, without awaiting the result.
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

func runDetached(r *resonate.Resonate, detachedFn *resonate.RegisteredFunc[DetachedArgs, string]) error {
	ctx := context.Background()
	id := fmt.Sprintf("detached-%d", time.Now().UnixNano())
	fmt.Printf("[detached] starting workflow id=%s\n", id)

	h, err := detachedFn.Run(ctx, id, DetachedArgs{Message: "fire and forget"})
	if err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	result, err := h.Result(ctx)
	if err != nil {
		return fmt.Errorf("Result: %w", err)
	}
	fmt.Printf("[detached] parent done: %s\n", result)

	// Give the localnet actor a moment to dispatch and execute the child before
	// we proceed. In a real-server setup you would poll the child promise ID via
	// r.Get(ctx, childID) instead.
	time.Sleep(200 * time.Millisecond)
	return nil
}

// ── Pattern 3: Fan-out ────────────────────────────────────────────────────────
//
// The workflow dispatches multiple RPC calls without awaiting any of them in
// the dispatch loop (collect futures), then awaits all of them in a second
// pass (collect results). The remote calls run concurrently server-side; the
// two-loop structure is idiomatic for the Go SDK (mirrors example-fan-out-fan-in-go).

// FanoutArgs carries the root message and the list of recipient names.
type FanoutArgs struct {
	Message    string   `json:"message"`
	Recipients []string `json:"recipients"`
}

// FanoutResult holds every individual echo acknowledgement.
type FanoutResult struct {
	Acks []string `json:"acks"`
}

// fanout dispatches one echo per recipient without awaiting between dispatches,
// then awaits every future to collect the combined result.
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

func runFanout(r *resonate.Resonate, fanoutFn *resonate.RegisteredFunc[FanoutArgs, FanoutResult]) error {
	ctx := context.Background()
	id := fmt.Sprintf("fanout-%d", time.Now().UnixNano())
	recipients := []string{"alice", "bob", "carol", "dave"}
	args := FanoutArgs{Message: "hello", Recipients: recipients}
	fmt.Printf("[fanout] starting workflow id=%s recipients=%v\n", id, recipients)

	h, err := fanoutFn.Run(ctx, id, args)
	if err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	result, err := h.Result(ctx)
	if err != nil {
		return fmt.Errorf("Result: %w", err)
	}
	fmt.Printf("[fanout] done\n")
	for _, ack := range result.Acks {
		fmt.Printf("  %s\n", ack)
	}
	return nil
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	mode := flag.String("mode", "all", "which pattern to run: chain | detached | fanout | all")
	url := flag.String("url", "", "Resonate server URL (empty = use in-process localnet)")
	flag.Parse()

	// Build the Resonate instance. Prefer -url when supplied; otherwise use
	// localnet so the example runs without an external server.
	var cfg resonate.Config
	if *url != "" {
		cfg = resonate.Config{URL: *url}
	} else {
		pid := "worker-1"
		cfg = resonate.Config{
			Network:   localnet.NewLocal("default", &pid),
			Heartbeat: resonate.NoopHeartbeat{},
		}
	}

	r, err := resonate.New(cfg)
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	// Register all functions regardless of mode so that ctx.RPC / ctx.Detached
	// dispatches from inside a workflow find the target in the same process
	// (localnet routes Execute messages back to this instance).
	echoFn, err := resonate.Register(r, "echo", echo)
	if err != nil {
		log.Fatalf("Register echo: %v", err)
	}
	_ = echoFn // only invoked as a remote target, not called directly from main

	chainFn, err := resonate.Register(r, "awaitChain", awaitChain)
	if err != nil {
		log.Fatalf("Register awaitChain: %v", err)
	}

	detachedFn, err := resonate.Register(r, "detachedWorkflow", detachedWorkflow)
	if err != nil {
		log.Fatalf("Register detachedWorkflow: %v", err)
	}

	fanoutFn, err := resonate.Register(r, "fanout", fanout)
	if err != nil {
		log.Fatalf("Register fanout: %v", err)
	}

	switch *mode {
	case "chain":
		if err := runChain(r, chainFn); err != nil {
			log.Fatalf("chain: %v", err)
		}
	case "detached":
		if err := runDetached(r, detachedFn); err != nil {
			log.Fatalf("detached: %v", err)
		}
	case "fanout":
		if err := runFanout(r, fanoutFn); err != nil {
			log.Fatalf("fanout: %v", err)
		}
	case "all":
		fmt.Println("=== await-chain ===")
		if err := runChain(r, chainFn); err != nil {
			log.Fatalf("chain: %v", err)
		}
		fmt.Println()
		fmt.Println("=== detached ===")
		if err := runDetached(r, detachedFn); err != nil {
			log.Fatalf("detached: %v", err)
		}
		fmt.Println()
		fmt.Println("=== fan-out ===")
		if err := runFanout(r, fanoutFn); err != nil {
			log.Fatalf("fanout: %v", err)
		}
	default:
		log.Fatalf("unknown mode %q — use chain | detached | fanout | all", *mode)
	}
}
