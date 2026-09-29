package api

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/feed"
)

type feedReceiptRequest struct {
	SwitchID string  `json:"switch_id"`
	Revision string  `json:"revision"`
	Source   string  `json:"source"`
	Open     bool    `json:"open"`
	Flow     float64 `json:"flow"`
}

// feedSwitches exposes the per-switch timeline and per-source outage analysis.
// Optional ?revision= narrows to switches bound to one campaign revision.
func (s *Server) feedSwitches(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.plant.FeedSwitchHistory(r.URL.Query().Get("revision")))
}

// feedReceipt accepts a late physical acknowledgement. A receipt for a
// superseded switch is rejected so it cannot rewrite the current route.
func (s *Server) feedReceipt(w http.ResponseWriter, r *http.Request) {
	var input feedReceiptRequest
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	switchID, err := uuid.Parse(input.SwitchID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "switch_id is invalid"})
		return
	}
	revision, err := uuid.Parse(input.Revision)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "revision is invalid"})
		return
	}
	err = s.plant.AcknowledgeFeed(feed.Receipt{SwitchID: switchID, Revision: revision, Source: input.Source, Open: input.Open, Flow: input.Flow})
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}
