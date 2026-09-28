package flow

type RunState struct {
	ID           string
	DefinitionID string
	Node         string
	Status       string
	Attempts     map[string]int
	WaitResult   string
	DeadlineNS   int64
}

func Replay(definition *Definition, events []Event) map[string]*RunState {
	runs := make(map[string]*RunState)
	for _, event := range events {
		run := runs[event.RunID]
		if run == nil {
			run = &RunState{ID: event.RunID, Attempts: map[string]int{}}
			runs[event.RunID] = run
		}
		applyEvent(run, event)
	}
	return runs
}

func applyEvent(run *RunState, event Event) {
	switch event.Type {
	case "run_started":
		run.DefinitionID = event.Detail
		run.Node = event.Node
		run.Status = "running"
	case "node_entered":
		run.Node = event.Node
		run.WaitResult = ""
		run.DeadlineNS = 0
	case "action_started":
		run.Attempts[event.Action] = event.Attempt
	case "wait_started":
		run.Status = "waiting"
		run.WaitResult = event.Result
		run.DeadlineNS = event.DeadlineNS
	case "external_result_accepted", "wait_timeout":
		run.Status = "running"
		run.Node = event.Detail
		run.WaitResult = ""
		run.DeadlineNS = 0
	case "run_succeeded":
		run.Status = "succeeded"
		run.Node = event.Node
		run.WaitResult = ""
		run.DeadlineNS = 0
	case "action_completed":
		if event.Next != "" {
			run.Node = event.Next
		}
	}
}
