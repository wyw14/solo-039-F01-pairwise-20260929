package feed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

type fakeClock struct {
	t    time.Time
	step time.Duration
}

// now 每次取值都推进固定步长，模拟阶段间真实的时间流逝。
func (c *fakeClock) now() time.Time {
	c.t = c.t.Add(c.step)
	return c.t
}
func (c *fakeClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func newTestCoordinator() (*Coordinator, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC), step: 250 * time.Millisecond}
	c := NewCoordinator()
	c.Now = clk.now
	return c, clk
}

func mustParse(t *testing.T, value string) uuid.UUID {
	t.Helper()
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("parse uuid %q: %v", value, err)
	}
	return parsed
}

func activateRoute(t *testing.T, c *Coordinator, source string, revision uuid.UUID) {
	t.Helper()
	c.Current.Prepare(source, revision)
	if err := c.Current.Activate(); err != nil {
		t.Fatalf("activate %s: %v", source, err)
	}
}

func campaignWith(revision uuid.UUID) model.Campaign {
	c := model.NewCampaign("grade")
	c.Revision = revision
	return c
}

func TestSwitchRecordsBothStagesAndOutage(t *testing.T) {
	c, clk := newTestCoordinator()
	revA := mustParse(t, "11111111-1111-1111-1111-111111111111")
	revB := mustParse(t, "22222222-2222-2222-2222-222222222222")
	activateRoute(t, c, "A", revA)

	if err := c.Switch(context.Background(), "B", campaignWith(revB)); err != nil {
		t.Fatal(err)
	}

	records := c.Log.List(revB, "")
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	record := records[0]
	if record.OldSource != "A" || record.NewSource != "B" {
		t.Fatalf("unexpected sources: %s -> %s", record.OldSource, record.NewSource)
	}
	if record.OldFlowBefore != 1 {
		t.Fatalf("expected old flow 1 before close, got %v", record.OldFlowBefore)
	}
	if record.OldIsolatedAt.IsZero() || record.NewActiveAt.IsZero() {
		t.Fatal("both stage timestamps must be recorded")
	}
	if !record.NewActiveAt.After(record.OldIsolatedAt) {
		t.Fatal("new source must activate after old isolation")
	}
	if !record.Resolved() {
		t.Fatal("switch should be resolved")
	}
	if c.Previous.Source != "A" || !c.Previous.Isolated() {
		t.Fatal("previous route must be isolated source A")
	}
	if c.Current.Source != "B" || c.Current.Revision != revB || !c.Current.Open {
		t.Fatal("current route must be active source B bound to revision B")
	}

	outage := c.Log.SourceOutage(clk.now())
	if outage["A"].Switches != 1 || outage["A"].OutageSeconds <= 0 {
		t.Fatalf("expected outage for source A, got %+v", outage["A"])
	}
	if _, ok := outage["B"]; ok {
		t.Fatal("new source B should not appear as an outage source")
	}
}

func TestSwitchEmitsTimestampedStages(t *testing.T) {
	c, _ := newTestCoordinator()
	revA := mustParse(t, "11111111-1111-1111-1111-111111111111")
	revB := mustParse(t, "22222222-2222-2222-2222-222222222222")
	activateRoute(t, c, "A", revA)

	var stages []Transition
	c.Observe = func(value Transition) { stages = append(stages, value) }
	if err := c.Switch(context.Background(), "B", campaignWith(revB)); err != nil {
		t.Fatal(err)
	}
	if len(stages) != 2 {
		t.Fatalf("expected 2 stage events, got %d", len(stages))
	}
	if stages[0].Stage != StageOldIsolated || stages[0].OldFlow != 0 || stages[0].OldOpen {
		t.Fatalf("bad isolation event: %+v", stages[0])
	}
	if stages[1].Stage != StageNewActive || stages[1].NewSource != "B" || !stages[1].NewOpen {
		t.Fatalf("bad activation event: %+v", stages[1])
	}
	if stages[0].Revision != revB || stages[1].Revision != revB {
		t.Fatal("stage events must carry the campaign revision of the switch")
	}
}

func TestNewActivationFailureKeepsOldRouteAndMarksStage(t *testing.T) {
	c, clk := newTestCoordinator()
	revA := mustParse(t, "11111111-1111-1111-1111-111111111111")
	revB := mustParse(t, "22222222-2222-2222-2222-222222222222")
	activateRoute(t, c, "A", revA)

	// 用空源名让 Activate 失败（Route.Activate 要求 source 非空）。
	err := c.Switch(context.Background(), "", campaignWith(revB))
	if err == nil {
		t.Fatal("expected new-source activation to fail")
	}
	records := c.Log.List(revB, "")
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	record := records[0]
	if record.FailedStage != StageNewActive || record.Error == "" {
		t.Fatalf("failure must identify the new-active stage, got %+v", record)
	}
	if record.OldIsolatedAt.IsZero() {
		t.Fatal("old isolation must still be timestamped before the new-source failure")
	}
	if record.Resolved() {
		t.Fatal("failed switch must not be resolved")
	}
	// 失败不得把当前路线改写为未启用的新源。
	if c.Current.Source != "A" {
		t.Fatalf("current route must remain A after activation failure, got %q", c.Current.Source)
	}
	outage := c.Log.SourceOutage(clk.now())
	if outage["A"].Open != 1 {
		t.Fatalf("failed switch should remain open against A, got %+v", outage["A"])
	}
}

func TestStaleReceiptCannotRewriteCurrentRoute(t *testing.T) {
	c, _ := newTestCoordinator()
	revA := mustParse(t, "11111111-1111-1111-1111-111111111111")
	revB := mustParse(t, "22222222-2222-2222-2222-222222222222")
	revC := mustParse(t, "33333333-3333-3333-3333-333333333333")
	activateRoute(t, c, "A", revA)

	ctx := context.Background()
	if err := c.Switch(ctx, "B", campaignWith(revB)); err != nil {
		t.Fatal(err)
	}
	if err := c.Switch(ctx, "C", campaignWith(revC)); err != nil {
		t.Fatal(err)
	}

	// 旧切换（修订 B）的回执迟到：当前路线已是 C，必须拒绝且不改写。
	record, err := c.AcknowledgeReceipt(SwitchReceipt{Revision: revB})
	if !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("expected ErrStaleReceipt, got %v", err)
	}
	if record.Revision != revB {
		t.Fatal("stale receipt should still resolve to its own historical record")
	}
	if c.Current.Source != "C" || c.Current.Revision != revC {
		t.Fatalf("current route was rewritten by stale receipt: %+v", c.Current)
	}

	// 当前修订的回执正常接受。
	if _, err := c.AcknowledgeReceipt(SwitchReceipt{Revision: revC}); err != nil {
		t.Fatalf("current revision receipt should be accepted, got %v", err)
	}
	// 从未出现过的修订回执。
	if _, err := c.AcknowledgeReceipt(SwitchReceipt{Revision: uuid.New()}); !errors.Is(err, ErrUnknownSwitch) {
		t.Fatalf("expected ErrUnknownSwitch, got %v", err)
	}
}

func TestReplayRebuildsLogAndRoute(t *testing.T) {
	revA := mustParse(t, "11111111-1111-1111-1111-111111111111")
	revB := mustParse(t, "22222222-2222-2222-2222-222222222222")
	recordID := uuid.New()
	t0 := time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC)

	stages := []StageEvent{
		{RecordID: recordID, CampaignID: uuid.New(), Revision: revB, Stage: StageOldIsolated, OldSource: "A", OldRevision: revA, OldFlow: 1, At: t0},
		{RecordID: recordID, Revision: revB, Stage: StageNewActive, NewSource: "B", NewRevision: revB, At: t0.Add(2 * time.Second)},
	}
	c := NewCoordinator()
	c.Replay(stages)

	records := c.Log.List(revB, "")
	if len(records) != 1 || !records[0].Resolved() {
		t.Fatalf("replay must rebuild resolved record, got %+v", records)
	}
	if c.Current.Source != "B" || c.Current.Revision != revB {
		t.Fatalf("replay must restore current route B, got %+v", c.Current)
	}
	if c.Previous.Source != "A" || c.Previous.Revision != revA {
		t.Fatalf("replay must restore previous route A, got %+v", c.Previous)
	}
}

func TestReplayFailureDoesNotPromoteNewSource(t *testing.T) {
	revA := mustParse(t, "11111111-1111-1111-1111-111111111111")
	revB := mustParse(t, "22222222-2222-2222-2222-222222222222")
	recordID := uuid.New()
	t0 := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)

	stages := []StageEvent{
		{RecordID: recordID, Revision: revB, Stage: StageOldIsolated, OldSource: "A", OldRevision: revA, OldFlow: 1, At: t0},
		{RecordID: recordID, Revision: revB, Stage: StageNewActive, NewSource: "B", FailedStage: StageNewActive, Error: "boom", At: t0.Add(time.Second)},
	}
	c := NewCoordinator()
	c.Replay(stages)

	if c.Current.Source != "A" || c.Current.Open || c.Current.Flow != 0 {
		t.Fatalf("failed replayed switch must leave A isolated, got %+v", c.Current)
	}
	records := c.Log.List(uuid.Nil, "")
	if len(records) != 1 || records[0].FailedStage != StageNewActive {
		t.Fatalf("failure stage must survive replay, got %+v", records)
	}
}
