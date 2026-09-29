package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/feed"
)

func startAndTransitionPlant(t *testing.T, dataDir string) *Plant {
	t.Helper()
	plant := NewPlant(dataDir)
	if _, err := plant.Start(context.Background(), "grade-a", "A"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := plant.Transition(context.Background(), "grade-b", "B"); err != nil {
		t.Fatalf("transition: %v", err)
	}
	return plant
}

func TestFeedSwitchHistoryQueryableByRevisionAndSource(t *testing.T) {
	plant := startAndTransitionPlant(t, t.TempDir())

	status := plant.Status()
	records, ok := status["feed_switches"].([]feed.SwitchRecord)
	if !ok {
		t.Fatalf("status missing feed_switches: %T", status["feed_switches"])
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 switch records (start + transition), got %d", len(records))
	}
	transition := records[1]
	if transition.OldSource != "A" || transition.NewSource != "B" {
		t.Fatalf("unexpected transition sources: %s -> %s", transition.OldSource, transition.NewSource)
	}
	if transition.OldIsolatedAt.IsZero() || transition.NewActiveAt.IsZero() {
		t.Fatal("both stage timestamps must be queryable")
	}
	if !transition.Resolved() {
		t.Fatal("transition should be resolved")
	}

	byRevision := plant.Feed.Log.List(transition.Revision, "")
	if len(byRevision) != 1 {
		t.Fatalf("revision filter must return exactly its switch, got %d", len(byRevision))
	}
	bySource := plant.Feed.Log.List(uuid.Nil, "A")
	// source 同时匹配旧源与新源：开工记录 ""->A 与切换记录 A->B 都涉及 A。
	if len(bySource) != 2 {
		t.Fatalf("source=A filter must match both the start and the A->B switch, got %d", len(bySource))
	}
	byRevisionAndSource := plant.Feed.Log.List(transition.Revision, "A")
	if len(byRevisionAndSource) != 1 || byRevisionAndSource[0].ID != transition.ID {
		t.Fatalf("revision+source filter must return exactly the A->B switch, got %d", len(byRevisionAndSource))
	}

	outage := plant.Feed.Log.SourceOutage(plant.Feed.Now())
	stat, ok := outage["A"]
	if !ok || stat.Switches != 1 || stat.OutageSeconds < 0 {
		t.Fatalf("expected outage accounting for A, got %+v", outage)
	}
}

func TestFeedHistoryReplayedOnRestore(t *testing.T) {
	dataDir := t.TempDir()
	plant := startAndTransitionPlant(t, dataDir)
	wantRevision := plant.Feed.Current.Revision

	rebuilt := NewPlant(dataDir)
	if err := rebuilt.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if rebuilt.Feed.Current.Source != "B" || rebuilt.Feed.Current.Revision != wantRevision {
		t.Fatalf("current route must be rebuilt as B, got %+v", rebuilt.Feed.Current)
	}
	if rebuilt.Feed.Previous.Source != "A" {
		t.Fatalf("previous route must be rebuilt as A, got %+v", rebuilt.Feed.Previous)
	}
	records := rebuilt.Feed.Log.List(uuid.Nil, "")
	if len(records) != 2 || !records[1].Resolved() {
		t.Fatalf("switch log must be rebuilt from durable events, got %+v", records)
	}
}

func TestStaleReceiptRejectedAtPlant(t *testing.T) {
	plant := startAndTransitionPlant(t, t.TempDir())
	records := plant.Feed.Log.List(uuid.Nil, "")
	startRevision := records[0].Revision
	currentRevision := plant.Feed.Current.Revision

	// 旧切换（开工时修订）的回执迟到：当前路线已推进，必须拒绝且不改写。
	if _, err := plant.AcknowledgeFeedReceipt(startRevision); !errors.Is(err, feed.ErrStaleReceipt) {
		t.Fatalf("expected ErrStaleReceipt, got %v", err)
	}
	if plant.Feed.Current.Source != "B" {
		t.Fatalf("stale receipt rewrote current route: %+v", plant.Feed.Current)
	}
	if _, err := plant.AcknowledgeFeedReceipt(currentRevision); err != nil {
		t.Fatalf("current revision receipt must be accepted, got %v", err)
	}
}

func TestFailedSwitchExposesFailureStage(t *testing.T) {
	plant := NewPlant(t.TempDir())
	if _, err := plant.Start(context.Background(), "grade-a", "A"); err != nil {
		t.Fatalf("start: %v", err)
	}
	// 空来源会让新源 Activate 失败。
	if _, err := plant.Transition(context.Background(), "grade-b", ""); err == nil {
		t.Fatal("expected transition to fail when new source cannot activate")
	}
	records := plant.Feed.Log.List(uuid.Nil, "")
	last := records[len(records)-1]
	if last.FailedStage != feed.StageNewActive {
		t.Fatalf("failure stage must be recorded, got %+v", last)
	}
	if plant.Feed.Current.Source != "A" {
		t.Fatalf("failed new-source activation must not rewrite current route, got %q", plant.Feed.Current.Source)
	}
}
