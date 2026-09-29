package feed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

// Switch emits begin/old-isolated/new-active sharing one switch ID and bound to
// the campaign revision, and the log records old flow at zero and the new
// activation time.
func TestSwitchRecordsStageTimeline(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.Current.Prepare("A")
	if err := coordinator.Current.Activate(); err != nil {
		t.Fatal(err)
	}
	log := NewChangeLog()
	var switchID uuid.UUID
	coordinator.Observe = func(value Transition) {
		if value.Stage == StageBegin {
			switchID = value.ID
			log.Begin(value)
		} else {
			log.Apply(value)
		}
	}

	campaign := model.NewCampaign("grade-x")
	if err := coordinator.Switch(context.Background(), "B", campaign); err != nil {
		t.Fatal(err)
	}

	records := log.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 switch record, got %d", len(records))
	}
	record := records[0]
	if record.ID != switchID {
		t.Fatal("stage events do not share the switch id")
	}
	if record.Revision != campaign.Revision || record.Epoch != campaign.Epoch || record.Fencing != campaign.Fencing {
		t.Fatal("switch record is not bound to the campaign revision")
	}
	if record.OldSource != "A" || record.NewSource != "B" {
		t.Fatalf("unexpected sources %q -> %q", record.OldSource, record.NewSource)
	}
	if record.OldIsolatedAt.IsZero() {
		t.Fatal("old-source isolation time not recorded")
	}
	if record.OldFlow != 0 {
		t.Fatalf("old source flow should be zero after isolation, got %v", record.OldFlow)
	}
	if record.NewActiveAt.IsZero() {
		t.Fatal("new-source activation time not recorded")
	}
	if record.Result != ResultComplete || record.FailedStage != "" {
		t.Fatalf("unexpected result %q/%q", record.Result, record.FailedStage)
	}
	if _, ok := record.OutageSeconds(); !ok {
		t.Fatal("completed switch should have a measurable outage window")
	}
}

// When the new source fails to open the record pins the failure to the new-open
// step and the outage stays unmeasured rather than being reported as zero.
func TestSwitchRecordsFailureStage(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.Current.Prepare("A")
	_ = coordinator.Current.Activate()
	log := NewChangeLog()
	coordinator.Observe = func(value Transition) {
		if value.Stage == StageBegin {
			log.Begin(value)
		} else {
			log.Apply(value)
		}
	}

	campaign := model.NewCampaign("grade-x")
	if err := coordinator.Switch(context.Background(), "", campaign); err == nil {
		t.Fatal("expected activation failure for empty source")
	}

	record, ok := log.Latest()
	if !ok {
		t.Fatal("failure was not recorded")
	}
	if record.Result != ResultFailed {
		t.Fatalf("expected failed result, got %q", record.Result)
	}
	if record.FailedStage != "new-open" {
		t.Fatalf("failure should be located at new-open, got %q", record.FailedStage)
	}
	if record.Error == "" {
		t.Fatal("failure reason not recorded")
	}
	if !record.NewActiveAt.IsZero() {
		t.Fatal("new source must not appear active after a failed open")
	}
	if _, measurable := record.OutageSeconds(); measurable {
		t.Fatal("failed switch outage must not be reported as a measured window")
	}
}

func TestOutageBySource(t *testing.T) {
	log := NewChangeLog()
	base := time.Unix(1000, 0)
	rev1 := uuid.New()
	rev2 := uuid.New()

	begin := func(id uuid.UUID, revision uuid.UUID, old, new string, at time.Time) {
		log.Begin(Transition{ID: id, Revision: revision, OldSource: old, NewSource: new, At: at})
	}
	isolate := func(id uuid.UUID, revision uuid.UUID, old string, at time.Time) {
		log.Apply(Transition{ID: id, Revision: revision, Stage: StageOldIsolated, OldSource: old, OldFlow: 0, At: at})
	}
	activate := func(id uuid.UUID, revision uuid.UUID, new string, at time.Time) {
		log.Apply(Transition{ID: id, Revision: revision, Stage: StageNewActive, NewSource: new, NewOpen: true, At: at})
	}

	id1, id2, id3 := uuid.New(), uuid.New(), uuid.New()
	begin(id1, rev1, "A", "B", base)
	isolate(id1, rev1, "A", base)
	activate(id1, rev1, "B", base.Add(8*time.Second))

	begin(id2, rev1, "B", "C", base)
	isolate(id2, rev1, "B", base)
	activate(id2, rev1, "C", base.Add(4*time.Second))

	// Failed switch away from A: new source never comes up.
	begin(id3, rev2, "A", "D", base)
	isolate(id3, rev2, "A", base)
	log.Apply(Transition{ID: id3, Revision: rev2, Stage: StageFailed, FailedStage: "new-open", Err: errors.New("boom"), At: base})

	outages := log.OutageBySource()
	bySource := map[string]SourceOutage{}
	for _, item := range outages {
		bySource[item.Source] = item
	}
	a := bySource["A"]
	if a.Switches != 2 || a.OutageSeconds != 8 || a.Unfinished != 1 {
		t.Fatalf("unexpected A aggregation: %+v", a)
	}
	b := bySource["B"]
	if b.Switches != 1 || b.OutageSeconds != 4 || b.Unfinished != 0 {
		t.Fatalf("unexpected B aggregation: %+v", b)
	}
	if _, present := bySource["C"]; present {
		t.Fatal("destination source must not be charged an outage")
	}

	if bound := log.ForRevision(rev2); len(bound) != 1 || bound[0].ID != id3 {
		t.Fatal("revision lookup must return only switches ordered under it")
	}
}

// The initial source open has no old source and must not contribute an outage.
func TestInitialOpenHasNoOutage(t *testing.T) {
	coordinator := NewCoordinator()
	log := NewChangeLog()
	coordinator.Observe = func(value Transition) {
		if value.Stage == StageBegin {
			log.Begin(value)
		} else {
			log.Apply(value)
		}
	}
	if err := coordinator.Switch(context.Background(), "primary", model.NewCampaign("g")); err != nil {
		t.Fatal(err)
	}
	if items := log.OutageBySource(); len(items) != 0 {
		t.Fatalf("initial open must not attribute an outage, got %+v", items)
	}
}

// A receipt from a superseded switch is rejected and cannot rewrite the route
// that replaced it; a receipt for the live switch is applied.
func TestStaleReceiptCannotChangeCurrentRoute(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.Current.Prepare("A")
	_ = coordinator.Current.Activate()

	var firstSwitch uuid.UUID
	coordinator.Observe = func(value Transition) {
		if value.Stage == StageBegin {
			firstSwitch = value.ID
		}
	}

	if err := coordinator.Switch(context.Background(), "B", model.NewCampaign("g1")); err != nil {
		t.Fatal(err)
	}
	bRevision := coordinator.Current.Revision

	// Same switch id but wrong revision is fenced out.
	if err := coordinator.ApplyReceipt(Receipt{SwitchID: firstSwitch, Revision: uuid.New(), Source: "B"}); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("expected stale receipt for wrong revision, got %v", err)
	}

	// Live receipt is applied.
	if err := coordinator.ApplyReceipt(Receipt{SwitchID: firstSwitch, Revision: bRevision, Source: "B", Open: true, Flow: 0.5}); err != nil {
		t.Fatalf("live receipt should be accepted, got %v", err)
	}
	if coordinator.Current.Flow != 0.5 {
		t.Fatalf("live receipt did not apply, flow=%v", coordinator.Current.Flow)
	}

	// A later switch supersedes the first token.
	if err := coordinator.Switch(context.Background(), "C", model.NewCampaign("g2")); err != nil {
		t.Fatal(err)
	}
	late := Receipt{SwitchID: firstSwitch, Revision: bRevision, Source: "B", Open: false, Flow: 0}
	if err := coordinator.ApplyReceipt(late); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("late receipt for a superseded switch must be rejected, got %v", err)
	}
	if coordinator.Current.Source != "C" || coordinator.Current.Flow != 1 || !coordinator.Current.Open {
		t.Fatalf("late receipt rewrote the current route: %+v", coordinator.Current)
	}
}
