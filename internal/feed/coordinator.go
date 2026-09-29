package feed

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

// Transition 是一次切换阶段进展的对外通知（旧源隔离 / 新源启用 / 失败）。
type Transition struct {
	Stage       string    `json:"stage"`
	RecordID    uuid.UUID `json:"record_id"`
	CampaignID  uuid.UUID `json:"campaign_id"`
	Revision    uuid.UUID `json:"revision"`
	OldSource   string    `json:"old_source,omitempty"`
	OldRevision uuid.UUID `json:"old_revision,omitempty"`
	OldOpen     bool      `json:"old_open"`
	OldFlow     float64   `json:"old_flow"`
	NewSource   string    `json:"new_source,omitempty"`
	NewRevision uuid.UUID `json:"new_revision,omitempty"`
	NewOpen     bool      `json:"new_open"`
	NewFlow     float64   `json:"new_flow"`
	At          time.Time `json:"at"`
	FailedStage string    `json:"failed_stage,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// SwitchReceipt 是现场设备对某次切换的迟到回执，只携带其所属批次修订。
type SwitchReceipt struct {
	Revision uuid.UUID `json:"revision"`
	Source   string    `json:"source,omitempty"`
	Flow     float64   `json:"flow,omitempty"`
}

type Coordinator struct {
	Current  *Route
	Previous *Route
	Log      *SwitchLog
	Now      func() time.Time
	Observe  func(Transition)
}

func NewCoordinator() *Coordinator {
	return &Coordinator{Current: &Route{}, Previous: &Route{}, Log: NewSwitchLog(), Now: time.Now}
}

func (c *Coordinator) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Switch 执行"旧源隔离 → 新源开通"两步切换，并逐阶段记入切换日志。
// 关键约束：只有新源启用成功后才提交 Current/Previous；任一步失败都保留
// 原路线指针，失败位置记录在案，且整条记录绑定发起时的批次修订。
func (c *Coordinator) Switch(ctx context.Context, source string, campaign model.Campaign) error {
	old := c.Current
	record := c.Log.Begin(campaign.ID, campaign.Revision, old, source, c.now())
	base := Transition{RecordID: record.ID, CampaignID: campaign.ID, Revision: campaign.Revision, NewSource: source}

	if old != nil && old.Source != "" {
		if err := old.Close(ctx); err != nil {
			c.Log.Fail(record.ID, StageOldIsolated, err, c.now())
			c.emit(withFailure(base, StageOldIsolated, err), old, nil)
			return err
		}
		c.Log.MarkIsolated(record.ID, c.now())
		c.emit(withStage(base, StageOldIsolated), old, nil)
	}

	next := &Route{}
	next.Prepare(source, campaign.Revision)
	if err := next.Activate(); err != nil {
		c.Log.Fail(record.ID, StageNewActive, err, c.now())
		c.emit(withFailure(base, StageNewActive, err), old, next)
		return err
	}

	// 新源确认启用后才提交路线切换。
	c.Previous = old
	c.Current = next
	c.Log.Complete(record.ID, c.now())
	c.emit(withStage(base, StageNewActive), old, next)
	return nil
}

func withStage(base Transition, stage string) Transition {
	base.Stage = stage
	return base
}

func withFailure(base Transition, stage string, err error) Transition {
	base.Stage = stage
	base.FailedStage = stage
	if err != nil {
		base.Error = err.Error()
	}
	return base
}

func (c *Coordinator) emit(base Transition, old, next *Route) {
	if c.Observe == nil {
		return
	}
	value := base
	value.At = c.now()
	if old != nil {
		value.OldSource = old.Source
		value.OldRevision = old.Revision
		value.OldOpen = old.Open
		value.OldFlow = old.Flow
	}
	if next != nil {
		value.NewSource = next.Source
		value.NewRevision = next.Revision
		value.NewOpen = next.Open
		value.NewFlow = next.Flow
	}
	c.Observe(value)
}

// AcknowledgeReceipt 接收一条迟到的现场切换回执。回执按其批次修订围栏：
// 修订已被新路线取代时返回 ErrStaleReceipt，当前路线不被改写。
func (c *Coordinator) AcknowledgeReceipt(receipt SwitchReceipt) (SwitchRecord, error) {
	if receipt.Revision == uuid.Nil {
		return SwitchRecord{}, errors.New("receipt revision is required")
	}
	currentRevision := uuid.Nil
	if c.Current != nil {
		currentRevision = c.Current.Revision
	}
	return c.Log.Acknowledge(receipt.Revision, currentRevision, c.now())
}

func (c *Coordinator) SafeSwitch(ctx context.Context, source string, campaign model.Campaign) error {
	if c.Current.Source != "" && !c.Current.Isolated() {
		return errors.New("old source is not isolated")
	}
	return c.Switch(ctx, source, campaign)
}

// StageEvent 是持久化事件重放用的切换阶段数据（由状态页事件日志反解得到）。
type StageEvent struct {
	RecordID    uuid.UUID
	CampaignID  uuid.UUID
	Revision    uuid.UUID
	Stage       string
	OldSource   string
	OldRevision uuid.UUID
	OldFlow     float64
	NewSource   string
	NewRevision uuid.UUID
	At          time.Time
	FailedStage string
	Error       string
}

// Replay 用持久化的阶段事件重建切换日志，并恢复 Current/Previous 路线。
// 路线只由成功的新源启用事件推导，迟到/失败事件不会改写最终路线。
func (c *Coordinator) Replay(stages []StageEvent) {
	ordered := append([]StageEvent(nil), stages...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At.Before(ordered[j].At) })

	index := map[uuid.UUID]int{}
	records := make([]*SwitchRecord, 0)
	for _, event := range ordered {
		position, ok := index[event.RecordID]
		if !ok {
			record := &SwitchRecord{ID: event.RecordID, CampaignID: event.CampaignID, Revision: event.Revision, NewSource: event.NewSource, CreatedAt: event.At, UpdatedAt: event.At}
			records = append(records, record)
			position = len(records) - 1
			index[event.RecordID] = position
		}
		record := records[position]
		record.UpdatedAt = event.At
		switch {
		case event.FailedStage != "":
			record.FailedStage = event.FailedStage
			record.Error = event.Error
			if event.OldSource != "" {
				record.OldSource = event.OldSource
				record.OldRevision = event.OldRevision
				record.OldFlowBefore = event.OldFlow
			}
			if event.NewSource != "" {
				record.NewSource = event.NewSource
			}
		case event.Stage == StageOldIsolated:
			record.OldSource = event.OldSource
			record.OldRevision = event.OldRevision
			record.OldFlowBefore = event.OldFlow
			record.OldIsolatedAt = event.At
		case event.Stage == StageNewActive:
			record.NewSource = event.NewSource
			record.NewActiveAt = event.At
		}
	}

	flat := make([]SwitchRecord, 0, len(records))
	for _, record := range records {
		flat = append(flat, *record)
	}
	c.Log = NewSwitchLog()
	c.Log.Restore(flat)
	c.restoreRoutes(records)
}

func (c *Coordinator) restoreRoutes(records []*SwitchRecord) {
	successes := make([]SwitchRecord, 0)
	for _, record := range records {
		if !record.NewActiveAt.IsZero() && record.FailedStage == "" {
			successes = append(successes, *record)
		}
	}
	sort.Slice(successes, func(i, j int) bool { return successes[i].NewActiveAt.Before(successes[j].NewActiveAt) })

	switch len(successes) {
	case 0:
		c.Current = &Route{}
		c.Previous = &Route{}
	default:
		last := successes[len(successes)-1]
		c.Current = &Route{Source: last.NewSource, Revision: last.Revision, Open: true, Flow: 1}
		// 切换前的当前路线即这次成功切换的旧源；与运行时 Previous 语义一致。
		c.Previous = &Route{Source: last.OldSource, Revision: last.OldRevision}
	}

	// 最近一次操作若在新源开通阶段失败，旧源已隔离、新源未起：
	// 当前路线保持为已隔离的旧源（流量零），且绝不指向未启用的新源。
	if len(records) > 0 {
		latest := records[len(records)-1]
		if latest.FailedStage == StageNewActive {
			c.Current = &Route{Source: latest.OldSource, Revision: latest.OldRevision, Open: false, Flow: 0}
		}
	}
}
