package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func runDemo(args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	dataDir := flags.String("data", "demo-data", "directory for reviewable history")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.RemoveAll(*dataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}

	fmt.Println("== 1. start approval; exit after external notification, before local confirmation ==")
	err = runStage(executable, *dataDir, "approval", true, "")
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 42 {
		return fmt.Errorf("expected simulated crash exit 42, got %w", err)
	}

	fmt.Println("== 2. restart: replay history; external ledger suppresses duplicate notification ==")
	if err := runStage(executable, *dataDir, "approval", false, ""); err != nil {
		return err
	}
	fmt.Println("== 3. deliver the same external result twice; only the first is accepted ==")
	if err := runStage(executable, *dataDir, "approval", false, "approval"); err != nil {
		return err
	}
	if err := runStage(executable, *dataDir, "approval", false, "approval"); err != nil {
		return err
	}

	fmt.Println("== 4. second run waits, is restarted after persisted deadline, and takes timeout path ==")
	if err := runStart(executable, *dataDir, "timeout", false); err != nil {
		return err
	}
	time.Sleep(350 * time.Millisecond)
	if err := runStage(executable, *dataDir, "timeout", false, ""); err != nil {
		return err
	}

	fmt.Println("== execution history ==")
	if err := printHistory(filepath.Join(*dataDir, "history.jsonl")); err != nil {
		return err
	}
	fmt.Println("== external idempotency ledger ==")
	data, err := os.ReadFile(filepath.Join(*dataDir, "external-invocations.jsonl"))
	if err != nil {
		return err
	}
	_, err = fmt.Print(string(data))
	return err
}

func runStage(executable, dataDir, runID string, crash bool, result string) error {
	if crash {
		return runStart(executable, dataDir, runID, true)
	}
	args := []string{"resume", "--data", dataDir, "--run", runID}
	if result != "" {
		args = append(args, "--result", result)
	}
	cmd := exec.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runStart(executable, dataDir, runID string, crash bool) error {
	args := []string{"start", "--data", dataDir, "--run", runID}
	if crash {
		args = append(args, "--crash-before-confirm")
	}
	cmd := exec.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
