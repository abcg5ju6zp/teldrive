package health

import (
	"context"
	"fmt"
)

type Pinger interface {
	Ping(context.Context) error
}

type Status struct {
	State   string
	Version string
}

type Service struct {
	version string
	db      Pinger
	checks  []Pinger
}

func NewService(version string, db Pinger, readiness ...Pinger) *Service {
	checks := make([]Pinger, 0, len(readiness))
	for _, check := range readiness {
		if check == nil {
			continue
		}
		checks = append(checks, check)
	}
	return &Service{version: version, db: db, checks: checks}
}

func (s *Service) Live() Status {
	return Status{State: "ok", Version: s.version}
}

func (s *Service) Ready(ctx context.Context) (Status, error) {
	if s.db == nil {
		return Status{State: "degraded", Version: s.version}, fmt.Errorf("database is not configured")
	}
	if err := s.db.Ping(ctx); err != nil {
		return Status{State: "degraded", Version: s.version}, fmt.Errorf("ping database: %w", err)
	}
	for _, check := range s.checks {
		if err := check.Ping(ctx); err != nil {
			return Status{State: "degraded", Version: s.version}, fmt.Errorf("readiness check failed: %w", err)
		}
	}
	return Status{State: "ok", Version: s.version}, nil
}
