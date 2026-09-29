package feed

import (
	"context"
	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

type State struct {
	route    *Route
	revision uuid.UUID
}

func New() *State { return &State{route: &Route{}, revision: uuid.New()} }

func (s *State) Open(ctx context.Context, c model.Campaign) error {
	s.route.Prepare("primary", c.Revision)
	s.revision = c.Revision
	return s.route.Activate()
}

func (s *State) Close(ctx context.Context) error { return s.route.Close(ctx) }

func (s *State) Revision() uuid.UUID { return s.revision }

func (s *State) Active() bool { return s.route.Open && s.route.Flow > 0 }
