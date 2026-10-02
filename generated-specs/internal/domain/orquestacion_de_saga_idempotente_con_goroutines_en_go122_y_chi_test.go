package domain

import (
	"errors"
	"testing"
)

func TestStartsInTheInitialStateWithNoEvents(t *testing.T) {
	a, err := NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate("agg-1")
	if err != nil {
		t.Fatal(err)
	}
	if a.State != StatePending || a.Version != 0 || len(a.PendingEvents()) != 0 {
		t.Fatalf("unexpected initial aggregate: %+v", a)
	}
}

func TestRejectsAnAggregateWithoutID(t *testing.T) {
	if _, err := NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate(""); !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiRecordsEventAndBumpsTheVersion(t *testing.T) {
	a, _ := NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate("agg-1")
	event, err := a.ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi(Command{ID: "agg-1"})
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != EventProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted || event.Version != 1 || a.Version != 1 || len(a.PendingEvents()) != 1 {
		t.Fatalf("unexpected result: %+v / %+v", event, a)
	}
}

func TestProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiRejectsACommandWithoutID(t *testing.T) {
	a, _ := NewOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiAggregate("agg-1")
	if _, err := a.ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi(Command{}); !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
	if len(a.PendingEvents()) != 0 {
		t.Fatal("no event must be recorded on failure")
	}
}
