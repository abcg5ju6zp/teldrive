package fileops

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

var (
	ErrInvalidInput = errors.New("invalid file operation input")
	ErrNotFound     = errors.New("file not found")
	ErrNotTrashed   = errors.New("file must be trashed before permanent deletion")
)

type Service struct {
	pool     *pgxpool.Pool
	queries  *sqlcgen.Queries
	catalog  *catalog.Service
	channels *channels.Service
	storage  telegramstore.Storage
}

type CopyInput struct {
	UserID         int64
	FileID         uuid.UUID
	ParentID       *uuid.UUID
	Name           *string
	ConflictPolicy sqlcgen.NameConflictPolicy
}

type treeNode struct {
	File  sqlcgen.File
	Depth int32
}

func NewService(pool *pgxpool.Pool, catalogService *catalog.Service, channelService *channels.Service, storage telegramstore.Storage) (*Service, error) {
	if pool == nil || catalogService == nil || channelService == nil || storage == nil {
		return nil, ErrInvalidInput
	}
	return &Service{pool: pool, queries: sqlcgen.New(pool), catalog: catalogService, channels: channelService, storage: storage}, nil
}

func optionalText(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func optionalInt64(value pgtype.Int8) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

func optionalInt32(value pgtype.Int4) *int32 {
	if !value.Valid {
		return nil
	}
	return &value.Int32
}

func nextAvailableCopyName(ctx context.Context, queries *sqlcgen.Queries, userID int64, parentID *uuid.UUID, original string) (string, error) {
	names, err := queries.ListActiveNames(ctx, sqlcgen.ListActiveNamesParams{
		UserID: userID, ParentID: dbtypes.OptionalUUID(parentID),
	})
	if err != nil {
		return "", fmt.Errorf("list copy destination names: %w", err)
	}
	used := make(map[string]struct{}, len(names))
	for _, name := range names {
		used[name] = struct{}{}
	}
	base, extension := splitCopyName(original)
	for sequence := 1; sequence <= 10000; sequence++ {
		candidate := base + fmt.Sprintf(" (%d)", sequence) + extension
		if _, exists := used[candidate]; !exists {
			return candidate, nil
		}
	}
	return "", catalog.ErrConflict
}

func splitCopyName(name string) (string, string) {
	index := strings.LastIndex(name, ".")
	if index <= 0 || index == len(name)-1 {
		return name, ""
	}
	return name[:index], name[index:]
}

func copyDestinationLockID(userID int64, parentID *uuid.UUID) int64 {
	input := []byte("teldrive/catalog-destination/")
	var user [8]byte
	binary.BigEndian.PutUint64(user[:], uint64(userID))
	input = append(input, user[:]...)
	if parentID != nil {
		input = append(input, parentID[:]...)
	} else {
		input = append(input, make([]byte, 16)...)
	}
	digest := sha256.Sum256(input)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func (s *Service) CleanTrash(ctx context.Context, userID int64) (int64, error) {
	if userID <= 0 {
		return 0, ErrInvalidInput
	}
	count, err := s.queries.MarkAllTrashedDeletionPending(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("mark trash deletion pending: %w", err)
	}
	return count, nil
}

func (s *Service) QueuePurge(ctx context.Context, userID int64, fileID uuid.UUID) error {
	if userID <= 0 || fileID == uuid.Nil {
		return ErrInvalidInput
	}
	files, err := s.queries.QueueFileSubtreePurge(ctx, sqlcgen.QueueFileSubtreePurgeParams{
		UserID: userID, FileID: dbtypes.UUID(fileID),
	})
	if err != nil {
		return fmt.Errorf("queue file subtree purge: %w", err)
	}
	if len(files) == 0 {
		if _, err := s.queries.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{
			FileID: dbtypes.UUID(fileID), UserID: userID,
		}); errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return fmt.Errorf("load purge root: %w", err)
		}
		return ErrNotTrashed
	}
	ids := make([]uuid.UUID, 0, len(files))
	for _, file := range files {
		id, ok := dbtypes.GoogleUUID(file.ID)
		if !ok {
			return ErrNotFound
		}
		ids = append(ids, id)
	}
	s.catalog.InvalidateFiles(ctx, userID, ids...)
	return nil
}

func (s *Service) Purge(ctx context.Context, userID int64, fileID uuid.UUID) error {
	return s.PurgeMany(ctx, userID, []uuid.UUID{fileID})
}

func (s *Service) PurgeMany(ctx context.Context, userID int64, rootIDs []uuid.UUID) error {
	if userID <= 0 || len(rootIDs) == 0 {
		return ErrInvalidInput
	}
	uniqueRoots := make([]uuid.UUID, 0, len(rootIDs))
	seenRoots := make(map[uuid.UUID]struct{}, len(rootIDs))
	for _, rootID := range rootIDs {
		if rootID == uuid.Nil {
			return ErrInvalidInput
		}
		if _, ok := seenRoots[rootID]; ok {
			continue
		}
		seenRoots[rootID] = struct{}{}
		uniqueRoots = append(uniqueRoots, rootID)
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire purge lock connection: %w", err)
	}
	defer conn.Release()
	lockQueries := sqlcgen.New(conn)
	lockedRoots := make([]uuid.UUID, 0, len(uniqueRoots))
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock_all()")
	}()
	rootByLockID := make(map[int64]uuid.UUID, len(uniqueRoots))
	lockIDs := make([]int64, 0, len(uniqueRoots))
	for _, rootID := range uniqueRoots {
		lockID := purgeAdvisoryLockID(userID, rootID)
		rootByLockID[lockID] = rootID
		lockIDs = append(lockIDs, lockID)
	}
	locks, err := lockQueries.TryAdvisoryLocks(ctx, lockIDs)
	if err != nil {
		return fmt.Errorf("acquire purge advisory locks: %w", err)
	}
	for _, lock := range locks {
		if lock.Locked {
			lockedRoots = append(lockedRoots, rootByLockID[lock.LockID])
		}
	}
	if len(lockedRoots) == 0 {
		return nil
	}

	nodesByID := make(map[uuid.UUID]treeNode)
	rows, err := s.queries.LoadFileSubtrees(ctx, sqlcgen.LoadFileSubtreesParams{
		RootIds: pgUUIDs(lockedRoots), UserID: userID,
	})
	if err != nil {
		return fmt.Errorf("load file subtrees: %w", err)
	}
	foundRoots := make(map[uuid.UUID]sqlcgen.FileStatus, len(lockedRoots))
	rootSet := make(map[uuid.UUID]struct{}, len(lockedRoots))
	for _, rootID := range lockedRoots {
		rootSet[rootID] = struct{}{}
	}
	for _, row := range rows {
		id, ok := dbtypes.GoogleUUID(row.ID)
		if !ok {
			return ErrNotFound
		}
		node := treeNode{File: sqlcgen.File{
			ID: row.ID, UserID: row.UserID, ParentID: row.ParentID, Name: row.Name,
			Kind: row.Kind, MimeType: row.MimeType,
			Size: row.Size, HashAlgorithm: row.HashAlgorithm, HashValue: row.HashValue,
			Encryption: row.Encryption, EncryptionKeyVersion: row.EncryptionKeyVersion,
			Status: row.Status, ModTime: row.ModTime, Generation: row.Generation,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, DeletedAt: row.DeletedAt,
		}, Depth: row.Depth}
		if _, isRoot := rootSet[id]; isRoot {
			foundRoots[id] = row.Status
		}
		if existing, exists := nodesByID[id]; !exists || node.Depth > existing.Depth {
			nodesByID[id] = node
		}
	}
	for _, rootID := range lockedRoots {
		status, ok := foundRoots[rootID]
		if !ok {
			return ErrNotFound
		}
		if status != sqlcgen.FileStatusTrashed && status != sqlcgen.FileStatusDeletionPending {
			return ErrNotTrashed
		}
	}
	nodes := make([]treeNode, 0, len(nodesByID))
	ids := make([]uuid.UUID, 0, len(nodesByID))
	for id, node := range nodesByID {
		nodes = append(nodes, node)
		ids = append(ids, id)
	}
	fileIDs := pgUUIDs(ids)
	if err := s.queries.MarkFileIDsDeletionPending(ctx, sqlcgen.MarkFileIDsDeletionPendingParams{
		UserID: userID, FileIds: fileIDs,
	}); err != nil {
		return fmt.Errorf("mark subtree deletion pending: %w", err)
	}
	s.catalog.InvalidateFiles(ctx, userID, ids...)

	refs, err := s.queries.ListFilePartMessageRefs(ctx, fileIDs)
	if err != nil {
		return fmt.Errorf("list purge parts: %w", err)
	}
	grouped := make(map[int64][]int64)
	for _, ref := range refs {
		grouped[ref.ChannelID] = append(grouped[ref.ChannelID], ref.MessageID)
	}
	channelIDs := slices.Sorted(maps.Keys(grouped))
	for _, channelID := range channelIDs {
		if err := s.storage.DeleteMessages(ctx, userID, channelID, grouped[channelID]); err != nil {
			return fmt.Errorf("delete Telegram purge messages for channel %d: %w", channelID, err)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if err := queries.DeleteFilePartsByFileIDs(ctx, fileIDs); err != nil {
		return fmt.Errorf("delete purge parts: %w", err)
	}
	if err := queries.ClearUploadSessionParentsByFileIDs(ctx, sqlcgen.ClearUploadSessionParentsByFileIDsParams{
		UserID: userID, FileIds: fileIDs,
	}); err != nil {
		return fmt.Errorf("clear purge upload session parents: %w", err)
	}
	byDepth := make(map[int32][]uuid.UUID)
	depths := make([]int32, 0)
	for _, node := range nodes {
		id, _ := dbtypes.GoogleUUID(node.File.ID)
		if _, ok := byDepth[node.Depth]; !ok {
			depths = append(depths, node.Depth)
		}
		byDepth[node.Depth] = append(byDepth[node.Depth], id)
	}
	slices.SortFunc(depths, func(a, b int32) int { return cmp.Compare(b, a) })
	for _, depth := range depths {
		depthIDs := byDepth[depth]
		count, err := queries.DeleteFileCatalogRowsByIDs(ctx, sqlcgen.DeleteFileCatalogRowsByIDsParams{
			FileIds: pgUUIDs(depthIDs), UserID: userID,
		})
		if err != nil {
			return fmt.Errorf("delete purge catalog rows at depth %d: %w", depth, err)
		}
		if count != int64(len(depthIDs)) {
			return ErrNotFound
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit file purge: %w", err)
	}
	s.catalog.InvalidateFiles(ctx, userID, ids...)
	return nil
}

func (s *Service) loadTree(ctx context.Context, userID int64, rootID uuid.UUID) ([]treeNode, error) {
	rows, err := s.queries.LoadFileSubtree(ctx, sqlcgen.LoadFileSubtreeParams{
		RootID: dbtypes.UUID(rootID), UserID: userID,
	})
	if err != nil {
		return nil, fmt.Errorf("load file subtree: %w", err)
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	out := make([]treeNode, 0, len(rows))
	for _, row := range rows {
		out = append(out, treeNode{File: sqlcgen.File{
			ID: row.ID, UserID: row.UserID, ParentID: row.ParentID, Name: row.Name,
			Kind: row.Kind, MimeType: row.MimeType,
			Size: row.Size, HashAlgorithm: row.HashAlgorithm, HashValue: row.HashValue,
			Encryption: row.Encryption, EncryptionKeyVersion: row.EncryptionKeyVersion,
			Status: row.Status, ModTime: row.ModTime, Generation: row.Generation,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, DeletedAt: row.DeletedAt,
		}, Depth: row.Depth})
	}
	return out, nil
}

func pgUUIDs(ids []uuid.UUID) []pgtype.UUID {
	result := make([]pgtype.UUID, len(ids))
	for index, id := range ids {
		result[index] = dbtypes.UUID(id)
	}
	return result
}

func purgeAdvisoryLockID(userID int64, fileID uuid.UUID) int64 {
	var user [8]byte
	binary.BigEndian.PutUint64(user[:], uint64(userID))
	input := append([]byte("teldrive/purge/"), user[:]...)
	input = append(input, fileID[:]...)
	digest := sha256.Sum256(input)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
