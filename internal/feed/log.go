package feed

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// 切换阶段：先隔离旧源，再开通新源。
const (
	StageOldIsolated = "old-isolated"
	StageNewActive   = "new-active"
)

var (
	// ErrUnknownSwitch 回执对应的修订从未发起过切换。
	ErrUnknownSwitch = errors.New("switch receipt references an unknown revision")
	// ErrStaleReceipt 回执属于已被修订取代的旧切换，不能改写当前路线。
	ErrStaleReceipt = errors.New("stale switch receipt: revision already superseded")
)

// SwitchRecord 一次进料切换的两阶段记录，绑定发起时的批次修订。
// 迟到的旧切换回执按 Revision 围栏，只能查补其所属那次切换，不能改写当前路线。
type SwitchRecord struct {
	ID          uuid.UUID `json:"id"`
	CampaignID  uuid.UUID `json:"campaign_id"`
	Revision    uuid.UUID `json:"revision"`
	OldSource   string    `json:"old_source"`
	OldRevision uuid.UUID `json:"old_revision"`
	NewSource   string    `json:"new_source"`

	OldFlowBefore float64   `json:"old_flow_before_close"`
	OldIsolatedAt time.Time `json:"old_isolated_at"`
	NewActiveAt   time.Time `json:"new_active_at"`

	FailedStage string `json:"failed_stage,omitempty"`
	Error       string `json:"error,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Resolved 新源已启用、无失败标记；若本次切换存在旧源，则旧源必须已隔离。
func (r SwitchRecord) Resolved() bool {
	if r.NewActiveAt.IsZero() || r.FailedStage != "" {
		return false
	}
	if r.OldSource != "" && r.OldIsolatedAt.IsZero() {
		return false
	}
	return true
}

// OutageSeconds 旧源流量归零到新源启用之间的停供秒数；未完成时以 at 截算。
func (r SwitchRecord) OutageSeconds(at time.Time) float64 {
	if r.OldIsolatedAt.IsZero() {
		return 0
	}
	end := r.NewActiveAt
	if end.IsZero() || end.Before(r.OldIsolatedAt) {
		end = at
	}
	return end.Sub(r.OldIsolatedAt).Seconds()
}

// SwitchLog 逐次保存切换记录，并支持按修订围栏的迟到回执。
type SwitchLog struct {
	mu      sync.RWMutex
	records []*SwitchRecord
}

func NewSwitchLog() *SwitchLog {
	return &SwitchLog{records: make([]*SwitchRecord, 0, 32)}
}

// Begin 登记一次切换的发起信息（旧源尚未确认隔离）。
func (l *SwitchLog) Begin(campaignID, revision uuid.UUID, old *Route, newSource string, at time.Time) *SwitchRecord {
	record := &SwitchRecord{
		ID:         uuid.New(),
		CampaignID: campaignID,
		Revision:   revision,
		NewSource:  newSource,
		CreatedAt:  at,
		UpdatedAt:  at,
	}
	if old != nil {
		record.OldSource = old.Source
		record.OldRevision = old.Revision
		record.OldFlowBefore = old.Flow
	}
	l.mu.Lock()
	l.records = append(l.records, record)
	l.mu.Unlock()
	return record
}

// MarkIsolated 确认旧源已在 at 时刻隔离（流量降为零）。
func (l *SwitchLog) MarkIsolated(id uuid.UUID, at time.Time) *SwitchRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.records {
		if record.ID == id {
			record.OldIsolatedAt = at
			record.UpdatedAt = at
			return cloneRecord(record)
		}
	}
	return nil
}

// Complete 标记新源启用成功。
func (l *SwitchLog) Complete(id uuid.UUID, at time.Time) *SwitchRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.records {
		if record.ID == id {
			record.NewActiveAt = at
			record.FailedStage = ""
			record.Error = ""
			record.UpdatedAt = at
			return cloneRecord(record)
		}
	}
	return nil
}

// Fail 记录切换在某个阶段失败（旧源隔离失败或新源开通失败）。
func (l *SwitchLog) Fail(id uuid.UUID, stage string, cause error, at time.Time) *SwitchRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.records {
		if record.ID == id {
			record.FailedStage = stage
			if cause != nil {
				record.Error = cause.Error()
			}
			record.UpdatedAt = at
			return cloneRecord(record)
		}
	}
	return nil
}

// Acknowledge 处理一条迟到的切换回执。围栏规则：
// 回执只能命中其 Revision 对应的那次切换；当前路线已属于更新的修订时，
// 回执被拒绝（ErrStaleReceipt），绝不改写当前路线。
func (l *SwitchLog) Acknowledge(revision, currentRevision uuid.UUID, at time.Time) (SwitchRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var target *SwitchRecord
	for i := len(l.records) - 1; i >= 0; i-- {
		if l.records[i].Revision == revision {
			target = l.records[i]
			break
		}
	}
	if target == nil {
		return SwitchRecord{}, ErrUnknownSwitch
	}
	if currentRevision != uuid.Nil && currentRevision != revision {
		return *cloneRecord(target), ErrStaleReceipt
	}
	target.UpdatedAt = at
	return *cloneRecord(target), nil
}

// List 返回切换记录，可按批次修订或来源过滤。source 为空表示不过滤；
// 来源同时匹配旧源与新源，便于按来源分析停供时间。
func (l *SwitchLog) List(revision uuid.UUID, source string) []SwitchRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]SwitchRecord, 0, len(l.records))
	for _, record := range l.records {
		if revision != uuid.Nil && record.Revision != revision {
			continue
		}
		if source != "" && record.OldSource != source && record.NewSource != source {
			continue
		}
		out = append(out, *cloneRecord(record))
	}
	return out
}

// OutageStat 单个来源的停供汇总。
type OutageStat struct {
	Source        string  `json:"source"`
	Switches      int     `json:"switches"`
	Open          int     `json:"open_switches"`
	OutageSeconds float64 `json:"outage_seconds"`
}

// SourceOutage 按旧源汇总停供秒数与未决切换数。
func (l *SwitchLog) SourceOutage(at time.Time) map[string]OutageStat {
	l.mu.RLock()
	defer l.mu.RUnlock()
	stats := map[string]OutageStat{}
	for _, record := range l.records {
		if record.OldSource == "" || record.OldIsolatedAt.IsZero() {
			continue
		}
		stat := stats[record.OldSource]
		stat.Source = record.OldSource
		stat.OutageSeconds += record.OutageSeconds(at)
		stat.Switches++
		if record.NewActiveAt.IsZero() || record.FailedStage != "" {
			stat.Open++
		}
		stats[record.OldSource] = stat
	}
	return stats
}

// LatestRevision 返回最近一次切换所属的批次修订；无记录时为 uuid.Nil。
func (l *SwitchLog) LatestRevision() uuid.UUID {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.records) == 0 {
		return uuid.Nil
	}
	return l.records[len(l.records)-1].Revision
}

// Restore 用持久化事件重放重建日志（启动恢复用），按 ID 去重并按时间排序。
func (l *SwitchLog) Restore(records []SwitchRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := make(map[uuid.UUID]bool, len(l.records))
	for _, record := range l.records {
		seen[record.ID] = true
	}
	for _, record := range records {
		record := record
		if seen[record.ID] {
			continue
		}
		l.records = append(l.records, &record)
		seen[record.ID] = true
	}
	sort.Slice(l.records, func(i, j int) bool { return l.records[i].CreatedAt.Before(l.records[j].CreatedAt) })
}

func cloneRecord(r *SwitchRecord) *SwitchRecord {
	copyValue := *r
	return &copyValue
}
