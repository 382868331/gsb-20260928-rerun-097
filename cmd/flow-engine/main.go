package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"persistent-flow/internal/flow"
	"persistent-flow/internal/simulator"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "demo" {
		if err := runDemo(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) < 2 {
		usage()
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	dataDir := flags.String("data", "data", "runtime data directory")
	runID := flags.String("run", "run-1", "run identifier")
	result := flags.String("result", "", "external result to deliver")
	crashBeforeConfirm := flags.Bool("crash-before-confirm", false, "exit after side effect but before local confirmation")
	_ = flags.Parse(os.Args[2:])

	if command == "history" {
		fatal(printHistory(filepath.Join(*dataDir, "history.jsonl")))
		return
	}
	engine, history, err := newEngine(*dataDir, *crashBeforeConfirm)
	fatal(err)
	defer history.Close()

	var state *flow.RunState
	switch command {
	case "start":
		state, err = engine.StartRun(context.Background(), *runID)
	case "resume":
		state, err = engine.Resume(context.Background(), *runID, flow.ResumeOptions{Result: *result})
	default:
		usage()
	}
	if err != nil {
		fatal(err)
	}
	if state != nil {
		printState(state)
	}
}

func newEngine(dataDir string, crash bool) (*flow.Engine, *flow.History, error) {
	definition, err := flow.LoadDefinition(filepath.Join("defs", "approval.json"))
	if err != nil {
		return nil, nil, err
	}
	history, err := flow.OpenHistory(filepath.Join(dataDir, "history.jsonl"))
	if err != nil {
		return nil, nil, err
	}
	return &flow.Engine{
		Definition: definition,
		History:    history,
		Actions:    simulator.NewExecutor(filepath.Join(dataDir, "external-invocations.jsonl"), crash),
	}, history, nil
}

func printState(state *flow.RunState) {
	deadline := "-"
	if state.DeadlineNS != 0 {
		deadline = time.Unix(0, state.DeadlineNS).Format(time.RFC3339Nano)
	}
	fmt.Printf("run=%s status=%s node=%s waits_for=%s deadline=%s attempts=%v\n",
		state.ID, state.Status, state.Node, state.WaitResult, deadline, state.Attempts)
}

func printHistory(path string) error {
	history, err := flow.OpenHistory(path)
	if err != nil {
		return err
	}
	defer history.Close()
	for _, event := range history.Events() {
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
	}
	return nil
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: flow-engine [start|resume|history|demo] [flags]")
	os.Exit(2)
}
