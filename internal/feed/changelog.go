package feed

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Switch stages. A switch is two physical steps: isolate the old source, then
// open the new one. Each stage gets its own timestamp so a switch can be traced
// end to end instead of collapsing into a single cumulative counter.
const (
	StageBegin       = "switch-begin"
	StageOldIsolated = "old-isolated"
	StageNewActive   = "new-active"
	StageFailed      = "failed"
)

// Result values for a completed switch record.
const (
	ResultComplete = "complete"
	ResultFailed   = "failed"
)

// ChangeRecord is one feed-source switch bound to the campaign revision that
// ordered it. Outage is the window where neither source is feeding, measured
// from old-source isolation to new-source activation.
type ChangeRecord struct {
	ID         uuid.UUID
	CampaignID uuid.UUID
	Revision   uuid.UUID
	Epoch      uint64
	Fencing    uint64

	OldSource string
	NewSource string

	BeganAt       time.Time
	OldIsolatedAt time.Time
	OldFlow       float64
	NewActiveAt   time.Time
	CompletedAt   time.Time

	Result      string
	FailedStage string
	Error       string
}

// OutageSeconds is the no-feed window between old isolation and new activation.
// A switch that never reached new-active is reported as not measurable.
func (r ChangeRecord) OutageSeconds() (int64, bool) {
	if r.OldIsolatedAt.IsZero() || r.NewActiveAt.IsZero() {
		return 0, false
	}
	return int64(r.NewActiveAt.Sub(r.OldIsolatedAt).Seconds()), true
}

// ChangeLog is the append-only, revision-bound history of feed switches.
type ChangeLog struct {
	mu            sync.Mutex
	records       []ChangeRecord
	staleReceipts int
}

func NewChangeLog() *ChangeLog { return &ChangeLog{records: make([]ChangeRecord, 0, 32)} }

// RecordStaleReceipt counts an acknowledgement that arrived after its switch
// was superseded. It is logged but never applied to the live route.
func (l *ChangeLog) RecordStaleReceipt() {
	l.mu.Lock()
	l.staleReceipts++
	l.mu.Unlock()
}

// StaleReceipts returns how many late receipts have been rejected.
func (l *ChangeLog) StaleReceipts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.staleReceipts
}

func (l *ChangeLog) Begin(t Transition) ChangeRecord {
	record := ChangeRecord{
		ID:         t.ID,
		CampaignID: t.CampaignID,
		Revision:   t.Revision,
		Epoch:      t.Epoch,
		Fencing:    t.Fencing,
		OldSource:  t.OldSource,
		NewSource:  t.NewSource,
		BeganAt:    t.At,
	}
	if record.ID == uuid.Nil {
		record.ID = uuid.New()
	}
	if record.BeganAt.IsZero() {
		record.BeganAt = time.Now()
	}
	l.mu.Lock()
	l.records = append(l.records, record)
	l.mu.Unlock()
	return record
}

// Apply folds one stage observation into the record identified by t.ID.
func (l *ChangeLog) Apply(t Transition) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range l.records {
		if l.records[i].ID != t.ID {
			continue
		}
		l.records[i] = mergeStage(l.records[i], t)
		return
	}
}

func mergeStage(record ChangeRecord, t Transition) ChangeRecord {
	at := t.At
	if at.IsZero() {
		at = time.Now()
	}
	switch t.Stage {
	case StageOldIsolated:
		record.OldIsolatedAt = at
		record.OldFlow = t.OldFlow
	case StageNewActive:
		record.NewActiveAt = at
		record.CompletedAt = at
		record.Result = ResultComplete
	case StageFailed:
		record.CompletedAt = at
		record.Result = ResultFailed
		record.FailedStage = t.FailedStage
		if t.Err != nil {
			record.Error = t.Err.Error()
		}
	}
	return record
}

// Records returns a copy of the full history, oldest first.
func (l *ChangeLog) Records() []ChangeRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ChangeRecord, len(l.records))
	copy(out, l.records)
	return out
}

// ForRevision returns every switch ordered under a campaign revision.
func (l *ChangeLog) ForRevision(revision uuid.UUID) []ChangeRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ChangeRecord, 0)
	for _, record := range l.records {
		if record.Revision == revision {
			out = append(out, record)
		}
	}
	return out
}

// SourceOutage is the aggregated no-feed time attributed to a source. The
// outage belongs to the source that was taken away.
type SourceOutage struct {
	Source         string `json:"source"`
	Switches       int    `json:"switches"`
	OutageSeconds  int64  `json:"outage_seconds"`
	Unfinished     int    `json:"unfinished"`
	LastSwitchedAt int64  `json:"last_switched_at"`
}

// OutageBySource aggregates measured outage windows by the source that was
// isolated. Switches whose new source never came up are counted separately so
// they cannot silently shorten the reported downtime.
func (l *ChangeLog) OutageBySource() []SourceOutage {
	l.mu.Lock()
	defer l.mu.Unlock()
	index := map[string]int{}
	out := make([]SourceOutage, 0)
	for _, record := range l.records {
		// The initial source open has no source being taken away and therefore no
		// outage to attribute; it still appears in the full switch timeline.
		if record.OldSource == "" {
			continue
		}
		source := record.OldSource
		pos, ok := index[source]
		if !ok {
			pos = len(out)
			index[source] = pos
			out = append(out, SourceOutage{Source: source})
		}
		item := &out[pos]
		item.Switches++
		if seconds, measurable := record.OutageSeconds(); measurable {
			item.OutageSeconds += seconds
		} else {
			item.Unfinished++
		}
		if !record.BeganAt.IsZero() {
			item.LastSwitchedAt = record.BeganAt.Unix()
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Source < out[j].Source })
	return out
}

// Count returns the number of switches started.
func (l *ChangeLog) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.records)
}

// Latest returns the most recently started switch.
func (l *ChangeLog) Latest() (ChangeRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.records) == 0 {
		return ChangeRecord{}, false
	}
	return l.records[len(l.records)-1], true
}

// Snapshot is the status-page view: every switch plus per-source outage.
func (l *ChangeLog) Snapshot() map[string]any {
	return map[string]any{
		"switches":       l.Records(),
		"source_outages": l.OutageBySource(),
		"stale_receipts": l.StaleReceipts(),
		"count":          l.Count(),
	}
}

// ErrStaleReceipt means an acknowledgement carries a switch token that no
// longer owns the live route and must not change it.
var ErrStaleReceipt = errors.New("stale feed switch receipt")

// Receipt is a late acknowledgement of a physical feed action. The token binds
// it to the exact switch and route revision that produced it.
type Receipt struct {
	SwitchID uuid.UUID
	Revision uuid.UUID
	Source   string
	Open     bool
	Flow     float64
}
