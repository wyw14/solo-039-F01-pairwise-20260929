package feed

import (
	"context"
	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
	"testing"
)

func TestSwitchClosesPreviousRoute(t *testing.T) {
	coordinator := NewCoordinator()
	coordinator.Current.Prepare("A", uuid.New())
	if err := coordinator.Current.Activate(); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Switch(context.Background(), "B", model.NewCampaign("grade")); err != nil {
		t.Fatal(err)
	}
	if !coordinator.Previous.Isolated() {
		t.Fatal("previous route remains active")
	}
	if !coordinator.Current.Open {
		t.Fatal("new route is closed")
	}
}
