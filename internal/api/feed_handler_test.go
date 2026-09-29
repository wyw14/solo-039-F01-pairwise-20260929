package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/service"
)

func newStartedPlant(t *testing.T) *service.Plant {
	t.Helper()
	plant := service.NewPlant(t.TempDir())
	if _, err := plant.Start(context.Background(), "grade-a", "A"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := plant.Transition(context.Background(), "grade-b", "B"); err != nil {
		t.Fatalf("transition: %v", err)
	}
	return plant
}

func TestFeedSwitchesEndpoint(t *testing.T) {
	plant := newStartedPlant(t)
	server := NewServerWithPlant(plant)
	transitionRevision := plant.Feed.Log.List(uuid.Nil, "")[1].Revision.String()

	request := httptest.NewRequest(http.MethodGet, "/api/feed/switches?source=A&revision="+transitionRevision, nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var payload struct {
		Items        []map[string]any `json:"items"`
		SourceOutage map[string]any   `json:"source_outage"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("revision+source filter must return 1 switch, got %d", len(payload.Items))
	}
	if _, ok := payload.SourceOutage["A"]; !ok {
		t.Fatalf("source_outage must account for A: %s", response.Body.String())
	}
}

func TestFeedReceiptEndpointRejectsStaleRevision(t *testing.T) {
	plant := newStartedPlant(t)
	server := NewServerWithPlant(plant)
	staleRevision := plant.Feed.Log.List(uuid.Nil, "")[0].Revision.String()

	body, _ := json.Marshal(map[string]string{"revision": staleRevision})
	request := httptest.NewRequest(http.MethodPost, "/api/feed/receipts", bytes.NewReader(body))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("expected 409 for stale receipt, got %d: %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["stale"] != true {
		t.Fatalf("expected stale=true, got %s", response.Body.String())
	}
	if plant.Feed.Current.Source != "B" {
		t.Fatalf("current route must remain B, got %q", plant.Feed.Current.Source)
	}
}
