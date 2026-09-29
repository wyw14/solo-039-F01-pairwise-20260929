package feed

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

type Route struct {
	Source   string    `json:"source"`
	Revision uuid.UUID `json:"revision"`
	Open     bool      `json:"open"`
	Flow     float64   `json:"flow"`
}

// Prepare 按给定的批次修订开通候选路线；在 Activate 成功前路线保持隔离状态。
func (r *Route) Prepare(source string, revision uuid.UUID) {
	r.Source = source
	r.Revision = revision
	r.Open = false
	r.Flow = 0
}

func (r *Route) Isolated() bool { return !r.Open && r.Flow == 0 }

func (r *Route) Activate() error {
	if r.Source == "" {
		return errors.New("source required")
	}
	r.Open = true
	r.Flow = 1
	return nil
}

func (r *Route) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		r.Open = false
		r.Flow = 0
		return nil
	}
}

func (r *Route) Event(c model.Campaign) model.Event { return model.NewEvent("feed.route", c, r.Source) }
