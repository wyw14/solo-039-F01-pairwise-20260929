package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/feed"
)

type receiptRequest struct {
	Revision string `json:"revision"`
}

// feedSwitches 返回每次进料源切换的可查记录：旧源流量归零时刻、新源启用时刻、
// 失败阶段，以及按来源汇总的停供时间。
// 可选过滤：revision（批次修订）、source（旧源或新源）、failed=true（仅失败）。
func (s *Server) feedSwitches(w http.ResponseWriter, r *http.Request) {
	now := time.Now()

	revision := uuid.Nil
	if raw := r.URL.Query().Get("revision"); raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid revision"})
			return
		}
		revision = parsed
	}
	source := r.URL.Query().Get("source")
	onlyFailed := r.URL.Query().Get("failed") == "true"

	records := s.plant.Feed.Log.List(revision, source)
	items := make([]map[string]any, 0, len(records))
	for _, record := range records {
		if onlyFailed && record.FailedStage == "" {
			continue
		}
		items = append(items, map[string]any{
			"record":         record,
			"resolved":       record.Resolved(),
			"outage_seconds": record.OutageSeconds(now),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":         items,
		"source_outage": s.plant.Feed.Log.SourceOutage(now),
	})
}

// feedReceipt 接收现场迟到的进料切换回执。修订已推进的旧回执返回 409，
// 且不会改变当前路线。
func (s *Server) feedReceipt(w http.ResponseWriter, r *http.Request) {
	var input receiptRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	revision, err := uuid.Parse(input.Revision)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid revision"})
		return
	}

	record, err := s.plant.AcknowledgeFeedReceipt(revision)
	if err != nil {
		switch {
		case errors.Is(err, feed.ErrUnknownSwitch):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		case errors.Is(err, feed.ErrStaleReceipt):
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "stale": true, "record": record})
		default:
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"record": record, "resolved": record.Resolved()})
}
