package feed

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

// Transition is one stage of a feed-source switch. A single switch emits a
// begin, then one event per physical step (old isolation, new activation), so
// observers can tell when the old flow hit zero and when the new source came up.
type Transition struct {
	// SwitchID identifies the two-step switch as a whole; every stage shares it.
	ID uuid.UUID

	Stage string
	Err   error
	// FailedStage names the step that failed, when Stage is StageFailed.
	FailedStage string

	// Campaign the switch was ordered under. Binds the record to the revision.
	CampaignID uuid.UUID
	Revision   uuid.UUID
	Epoch      uint64
	Fencing    uint64

	OldSource string
	OldOpen   bool
	OldFlow   float64
	NewSource string
	NewOpen   bool

	At time.Time
}

type Coordinator struct {
	Current  *Route
	Previous *Route
	Observe  func(Transition)

	// switchID fences late receipts: it only ever matches the in-flight or most
	// recent switch, so an acknowledgement from an older switch can never touch
	// the route that replaced it.
	switchID uuid.UUID
}

func NewCoordinator() *Coordinator { return &Coordinator{Current: &Route{}, Previous: &Route{}} }

func (c *Coordinator) Switch(ctx context.Context, source string, campaign model.Campaign) error {
	old := c.Current
	switchID := uuid.New()
	c.switchID = switchID

	c.emit(Transition{
		ID:         switchID,
		Stage:      StageBegin,
		CampaignID: campaign.ID,
		Revision:   campaign.Revision,
		Epoch:      campaign.Epoch,
		Fencing:    campaign.Fencing,
		OldSource:  old.Source,
		NewSource:  source,
	})

	if old.Source != "" {
		if err := old.Close(ctx); err != nil {
			c.fail(switchID, campaign, "old-isolate", old, source, err)
			return err
		}
		// Reported after Close: open=false and flow=0 prove the old source fell
		// to zero, not merely that a close was requested.
		c.emit(Transition{
			ID:         switchID,
			Stage:      StageOldIsolated,
			CampaignID: campaign.ID,
			Revision:   campaign.Revision,
			Epoch:      campaign.Epoch,
			Fencing:    campaign.Fencing,
			OldSource:  old.Source,
			OldOpen:    old.Open,
			OldFlow:    old.Flow,
			NewSource:  source,
		})
	}

	c.Previous = old
	c.Current = &Route{}
	c.Current.Prepare(source)
	if err := c.Current.Activate(); err != nil {
		c.fail(switchID, campaign, "new-open", old, source, err)
		return err
	}
	c.emit(Transition{
		ID:         switchID,
		Stage:      StageNewActive,
		CampaignID: campaign.ID,
		Revision:   campaign.Revision,
		Epoch:      campaign.Epoch,
		Fencing:    campaign.Fencing,
		OldSource:  old.Source,
		OldOpen:    old.Open,
		OldFlow:    old.Flow,
		NewSource:  c.Current.Source,
		NewOpen:    c.Current.Open,
	})
	return nil
}

func (c *Coordinator) fail(switchID uuid.UUID, campaign model.Campaign, failedStage string, old *Route, source string, err error) {
	c.emit(Transition{
		ID:          switchID,
		Stage:       StageFailed,
		Err:         err,
		FailedStage: failedStage,
		CampaignID:  campaign.ID,
		Revision:    campaign.Revision,
		Epoch:       campaign.Epoch,
		Fencing:     campaign.Fencing,
		OldSource:   old.Source,
		OldOpen:     old.Open,
		OldFlow:     old.Flow,
		NewSource:   source,
	})
}

func (c *Coordinator) emit(value Transition) {
	if c.Observe == nil {
		return
	}
	if value.At.IsZero() {
		value.At = time.Now()
	}
	c.Observe(value)
}

// ApplyReceipt accepts a late acknowledgement of a feed action. It is applied
// only when the receipt belongs to the switch and route revision currently
// installed; a receipt for a since-replaced switch is rejected without touching
// the live route.
func (c *Coordinator) ApplyReceipt(receipt Receipt) error {
	if c.switchID == uuid.Nil || receipt.SwitchID != c.switchID {
		return ErrStaleReceipt
	}
	if c.Current == nil || c.Current.Source == "" || receipt.Revision != c.Current.Revision {
		return ErrStaleReceipt
	}
	if receipt.Source != c.Current.Source {
		return ErrStaleReceipt
	}
	c.Current.Open = receipt.Open
	c.Current.Flow = receipt.Flow
	return nil
}

// CurrentSwitch returns the token fencing the in-flight or most recent switch.
func (c *Coordinator) CurrentSwitch() uuid.UUID { return c.switchID }

func (c *Coordinator) SafeSwitch(ctx context.Context, source string, campaign model.Campaign) error {
	if c.Current.Source != "" && !c.Current.Isolated() {
		return errors.New("old source is not isolated")
	}
	return c.Switch(ctx, source, campaign)
}
