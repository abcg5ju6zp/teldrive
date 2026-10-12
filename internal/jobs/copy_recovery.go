package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"
)

const (
	CopyRecoveryKind  = "teldrive_recover_pending_copies"
	CopyRecoveryQueue = CleanupQueue
)

var ErrCopyRecoveryNotConfigured = errors.New("pending copy recovery worker is not configured")

// CopyRecoveryService is implemented by the file operations service. A zero
// staleBefore and limit select service defaults.
type CopyRecoveryService interface {
	RecoverPendingCopies(ctx context.Context, staleBefore time.Time, limit int32) (int, error)
	DeleteExpiredCopyOperations(ctx context.Context) (int64, error)
}

type CopyRecoverySweepArgs struct{}

func (CopyRecoverySweepArgs) Kind() string { return CopyRecoveryKind }

func (CopyRecoverySweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: CopyRecoveryQueue, MaxAttempts: 3, Priority: 2}
}

// PendingCopyRecoveryWorker finishes copy operations whose request was
// cancelled, whose publish result was lost, or whose process restarted,
// continuing each one from its pinned snapshot and Telegram part receipts.
type PendingCopyRecoveryWorker struct {
	river.WorkerDefaults[CopyRecoverySweepArgs]
	service CopyRecoveryService
}

func NewPendingCopyRecoveryWorker(service CopyRecoveryService) *PendingCopyRecoveryWorker {
	return &PendingCopyRecoveryWorker{service: service}
}

func (w *PendingCopyRecoveryWorker) Timeout(*river.Job[CopyRecoverySweepArgs]) time.Duration {
	return 2 * time.Hour
}

func (w *PendingCopyRecoveryWorker) Work(ctx context.Context, _ *river.Job[CopyRecoverySweepArgs]) error {
	if w == nil || w.service == nil {
		return ErrCopyRecoveryNotConfigured
	}
	if _, err := w.service.DeleteExpiredCopyOperations(ctx); err != nil {
		return fmt.Errorf("delete expired copy operations: %w", err)
	}
	for {
		recovered, err := w.service.RecoverPendingCopies(ctx, time.Time{}, 0)
		if err != nil {
			return err
		}
		if recovered == 0 {
			return nil
		}
	}
}
