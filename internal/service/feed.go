package service

import (
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/feed"
)

// FeedSwitchHistory returns each switch with its stage timestamps and the
// per-source outage analysis. An empty revision returns the full history;
// otherwise only switches ordered under that campaign revision are returned.
func (p *Plant) FeedSwitchHistory(revision string) map[string]any {
	var records []feed.ChangeRecord
	revisionMatched := true
	if revision == "" {
		records = p.FeedChanges.Records()
	} else {
		id, err := uuid.Parse(revision)
		if err != nil {
			records = []feed.ChangeRecord{}
			revisionMatched = false
		} else {
			records = p.FeedChanges.ForRevision(id)
		}
	}
	return map[string]any{
		"switches":       records,
		"source_outages": p.FeedChanges.OutageBySource(),
		"stale_receipts": p.FeedChanges.StaleReceipts(),
		"count":          len(records),
		"revision_valid": revisionMatched,
	}
}

// AcknowledgeFeed applies a late physical acknowledgement. Receipts belonging
// to a superseded switch are counted and rejected without altering the route.
func (p *Plant) AcknowledgeFeed(receipt feed.Receipt) error {
	if err := p.Feed.ApplyReceipt(receipt); err != nil {
		if errors.Is(err, feed.ErrStaleReceipt) {
			p.FeedChanges.RecordStaleReceipt()
		}
		return err
	}
	return nil
}
