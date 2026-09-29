package service

import (
	"context"
	"errors"
	"testing"

	"github.com/wyw14/solo-039/internal/feed"
)

func newTestPlant(t *testing.T) *Plant {
	t.Helper()
	return NewPlant(t.TempDir())
}

// A real switch through the plant produces a revision-bound record carrying the
// old-source isolation and new-source activation stages, and the outage is
// attributed to the source that was taken away.
func TestFeedSwitchHistoryBoundToRevision(t *testing.T) {
	plant := newTestPlant(t)
	ctx := context.Background()

	started, err := plant.Start(ctx, "grade-a", "A")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	transitioned, err := plant.Transition(ctx, "grade-b", "B")
	if err != nil {
		t.Fatalf("transition: %v", err)
	}

	records := plant.FeedChanges.Records()
	if len(records) != 2 {
		t.Fatalf("expected start + transition records, got %d", len(records))
	}
	change := records[1]
	if change.Revision != transitioned.Revision {
		t.Fatal("switch is not bound to the transition revision")
	}
	if change.OldSource != "A" || change.NewSource != "B" {
		t.Fatalf("unexpected sources %q -> %q", change.OldSource, change.NewSource)
	}
	if change.OldFlow != 0 || change.OldIsolatedAt.IsZero() {
		t.Fatal("old-source isolation (flow zero) not recorded")
	}
	if change.NewActiveAt.IsZero() || change.Result != feed.ResultComplete {
		t.Fatal("new-source activation not recorded as complete")
	}

	// Revision-scoped lookup returns only that campaign's switch.
	if scoped := plant.FeedChanges.ForRevision(started.Revision); len(scoped) != 1 || scoped[0].NewSource != "A" {
		t.Fatalf("revision lookup returned %+v", scoped)
	}

	outages := plant.FeedChanges.OutageBySource()
	if len(outages) != 1 || outages[0].Source != "A" || outages[0].Switches != 1 {
		t.Fatalf("outage should be charged once to A, got %+v", outages)
	}

	status := plant.Status()
	if _, ok := status["feed_switches"].(map[string]any); !ok {
		t.Fatal("status page must expose the feed switch timeline")
	}
}

// A late receipt for the switch that was just superseded is rejected and leaves
// the current route intact.
func TestLateReceiptRejected(t *testing.T) {
	plant := newTestPlant(t)
	ctx := context.Background()
	if _, err := plant.Start(ctx, "grade-a", "A"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := plant.Transition(ctx, "grade-b", "B"); err != nil {
		t.Fatalf("transition: %v", err)
	}
	staleToken := plant.Feed.CurrentSwitch()
	staleRevision := plant.Feed.Current.Revision

	if _, err := plant.Transition(ctx, "grade-c", "C"); err != nil {
		t.Fatalf("second transition: %v", err)
	}

	err := plant.AcknowledgeFeed(feed.Receipt{SwitchID: staleToken, Revision: staleRevision, Source: "B", Open: false, Flow: 0})
	if !errors.Is(err, feed.ErrStaleReceipt) {
		t.Fatalf("expected stale receipt rejection, got %v", err)
	}
	if plant.FeedChanges.StaleReceipts() != 1 {
		t.Fatal("rejected receipt should be counted")
	}
	if plant.Feed.Current.Source != "C" || plant.Feed.Current.Flow != 1 || !plant.Feed.Current.Open {
		t.Fatalf("late receipt rewrote the current route: %+v", plant.Feed.Current)
	}
}
