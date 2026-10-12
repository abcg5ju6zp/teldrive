package transfer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/contentcrypto"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

var (
	ErrInvalidUpload       = errors.New("invalid upload request")
	ErrBodyTooShort        = errors.New("upload body is shorter than Content-Length")
	ErrBodyTooLong         = errors.New("upload body is longer than Content-Length")
	ErrChecksumMismatch    = errors.New("plaintext part checksum mismatch")
	ErrEncryptionKey       = errors.New("encryption key is unavailable")
	ErrStoredSizeMismatch  = errors.New("stored Telegram part size mismatch")
	ErrUploadNotConfigured = errors.New("upload pipeline is not configured")
)

const partCleanupTimeout = 5 * time.Second
const defaultLeaseRenewInterval = 20 * time.Second

// UploadCatalog is the durable upload-session boundary required by Pipeline.
type UploadCatalog interface {
	Get(context.Context, int64, uuid.UUID) (*sqlcgen.UploadSession, error)
	GetPart(context.Context, int64, uuid.UUID, int32) (*sqlcgen.UploadPart, error)
	ClaimPart(context.Context, uploads.ClaimPartInput) (*uploads.ClaimPartResult, error)
	FindTakeablePart(context.Context, uploads.FindTakeableInput) (*uploads.Adoption, error)
	ClaimPartForAdoption(context.Context, uploads.ClaimAdoptionInput) (*uploads.ClaimPartResult, error)
	RecordPartPublication(context.Context, uploads.RecordPublicationInput) error
	CondemnPartReceipt(context.Context, uploads.CondemnPartReceiptInput) (int64, error)
	DeletePartReceipt(context.Context, uploads.DeletePartReceiptInput) error
	RenewPart(context.Context, uploads.RenewPartInput) error
	StorePart(context.Context, uploads.StorePartInput) (*sqlcgen.UploadPart, error)
	FailPart(context.Context, uploads.FailPartInput) (*sqlcgen.UploadPart, error)
}

// ChannelResolver chooses an owned channel and performs rollover when needed.
type ChannelResolver interface {
	Resolve(context.Context, int64, int64) (int64, error)
}

// KeyProvider resolves versioned server-managed content-encryption keys.
type KeyProvider interface {
	Key(context.Context, int64, int32) (string, error)
}

type Config struct {
	UploadThreads      int
	RandomizePartNames bool
	DisableHashing     bool
	Random             io.Reader
	LeaseRenewInterval time.Duration
}

type Pipeline struct {
	catalog  UploadCatalog
	channels ChannelResolver
	storage  telegramstore.Storage
	keys     KeyProvider
	config   Config
}

func NewPipeline(catalog UploadCatalog, channels ChannelResolver, storage telegramstore.Storage, keys KeyProvider, cfg Config) *Pipeline {
	if cfg.Random == nil {
		cfg.Random = rand.Reader
	}
	if cfg.LeaseRenewInterval <= 0 {
		cfg.LeaseRenewInterval = defaultLeaseRenewInterval
	}
	return &Pipeline{catalog: catalog, channels: channels, storage: storage, keys: keys, config: cfg}
}

type UploadPartRequest struct {
	UserID             int64
	UploadID           uuid.UUID
	PartNo             int32
	RequestedChannelID int64
	PlainSize          int64
	Checksum           *string
	Body               io.Reader
}

type UploadPartResult struct {
	Part     *sqlcgen.UploadPart
	Existing bool
}

func (p *Pipeline) UploadPart(ctx context.Context, request UploadPartRequest) (*UploadPartResult, error) {
	if p.catalog == nil || p.channels == nil || p.storage == nil {
		return nil, ErrUploadNotConfigured
	}
	if request.UserID <= 0 || request.UploadID == uuid.Nil || request.PartNo <= 0 || request.PlainSize <= 0 || request.Body == nil {
		return nil, ErrInvalidUpload
	}
	checksum, err := normalizeOptionalChecksum(request.Checksum)
	if err != nil {
		return nil, err
	}
	request.Checksum = checksum

	session, err := p.catalog.Get(ctx, request.UserID, request.UploadID)
	if err != nil {
		return nil, err
	}
	if existing, err := p.catalog.GetPart(ctx, request.UserID, request.UploadID, request.PartNo); err == nil {
		if existing.State == sqlcgen.UploadPartStateStored {
			if existing.PlainSize != request.PlainSize || !checksumMatches(existing.Checksum.String, existing.Checksum.Valid, request.Checksum) {
				return nil, uploads.ErrPartConflict
			}
			return &UploadPartResult{Part: existing, Existing: true}, nil
		}
	} else if !errors.Is(err, uploads.ErrNotFound) {
		return nil, err
	}
	slog.DebugContext(ctx, "upload part started", "upload_id", request.UploadID, "part_no", request.PartNo, "plain_size", request.PlainSize, "requested_channel_id", request.RequestedChannelID)

	// Recovery first: a published receipt from a dead lease is adopted without
	// resolving a channel or publishing to Telegram again.
	claim, err := p.claimPartForUpload(ctx, request)
	if err != nil {
		return nil, err
	}
	if claim.Existing {
		return &UploadPartResult{Part: claim.Part, Existing: true}, nil
	}

	uploadCtx, cancelUpload := context.WithCancelCause(ctx)
	defer cancelUpload(nil)
	renewErrors := p.renewPartLease(uploadCtx, cancelUpload, request, claim.LeaseToken)

	if claim.Adoption != nil {
		return p.adoptPart(ctx, uploadCtx, session, request, claim, renewErrors)
	}
	return p.publishPart(ctx, uploadCtx, session, request, claim, renewErrors)
}

// claimPartForUpload adopts a takeable receipt when one exists, otherwise
// reserves a fresh upload through the channel resolver.
func (p *Pipeline) claimPartForUpload(ctx context.Context, request UploadPartRequest) (*uploads.ClaimPartResult, error) {
	takeable, err := p.catalog.FindTakeablePart(ctx, uploads.FindTakeableInput{
		UserID:    request.UserID,
		UploadID:  request.UploadID,
		PartNo:    request.PartNo,
		PlainSize: request.PlainSize,
	})
	if err != nil {
		return nil, err
	}
	if takeable != nil {
		claim, adoptErr := p.catalog.ClaimPartForAdoption(ctx, uploads.ClaimAdoptionInput{
			UserID:    request.UserID,
			UploadID:  request.UploadID,
			PartNo:    request.PartNo,
			PlainSize: request.PlainSize,
			Checksum:  request.Checksum,
		})
		if adoptErr == nil {
			slog.DebugContext(ctx, "adopting published upload part receipt", "upload_id", request.UploadID, "part_no", request.PartNo, "channel_id", takeable.ChannelID, "message_id", takeable.MessageID)
			return claim, nil
		}
		if !errors.Is(adoptErr, uploads.ErrAdoptionUnavailable) {
			return nil, adoptErr
		}
		// Lost the adoption race; fall through to a fresh claim.
	}

	channelID, err := p.channels.Resolve(ctx, request.UserID, request.RequestedChannelID)
	if err != nil {
		return nil, err
	}
	return p.catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID:    request.UserID,
		UploadID:  request.UploadID,
		PartNo:    request.PartNo,
		ChannelID: channelID,
		PlainSize: request.PlainSize,
		Checksum:  request.Checksum,
	})
}

// publishPart uploads new bytes to Telegram. The published message is recorded
// on the durable receipt before any validation, so failed commits or dropped
// responses never strand or double-publish the message.
func (p *Pipeline) publishPart(ctx, uploadCtx context.Context, session *sqlcgen.UploadSession, request UploadPartRequest, claim *uploads.ClaimPartResult, renewErrors <-chan error) (*UploadPartResult, error) {
	channelID := claim.Part.ChannelID
	exact := newExactReader(uploadCtx, request.Body, request.PlainSize)
	shouldHash := !p.config.DisableHashing || request.Checksum != nil || session.ExpectedHashAlgorithm.Valid
	var hasher *treehash.BlockHasher
	plainReader := io.Reader(exact)
	if shouldHash {
		hasher = treehash.NewBlockHasher()
		plainReader = io.TeeReader(exact, hasher)
	}
	storedReader := plainReader
	storedSize := request.PlainSize
	var salt *string

	if session.Encryption {
		if p.keys == nil || !session.EncryptionKeyVersion.Valid {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "encryption_key_missing", ErrEncryptionKey)
		}
		key, keyErr := p.keys.Key(ctx, request.UserID, session.EncryptionKeyVersion.Int32)
		if keyErr != nil || key == "" {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "encryption_key_missing", errors.Join(ErrEncryptionKey, keyErr))
		}
		generatedSalt, saltErr := generateSalt(p.config.Random)
		if saltErr != nil {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "salt_generation_failed", saltErr)
		}
		cipher, cipherErr := contentcrypto.NewCipher(key, generatedSalt)
		if cipherErr != nil {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "cipher_initialization_failed", cipherErr)
		}
		encrypted, cipherErr := cipher.EncryptData(plainReader)
		if cipherErr != nil {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "cipher_initialization_failed", cipherErr)
		}
		storedReader = encrypted
		storedSize = contentcrypto.EncryptedSize(request.PlainSize)
		salt = &generatedSalt
	}

	stored, err := p.storage.Upload(uploadCtx, telegramstore.UploadRequest{
		UserID:    request.UserID,
		ChannelID: channelID,
		Name:      p.partName(session.Name, request.PartNo),
		Reader:    storedReader,
		Size:      storedSize,
		Threads:   p.config.UploadThreads,
	})
	slog.DebugContext(ctx, "uploading part to telegram", "upload_id", request.UploadID, "part_no", request.PartNo, "channel_id", channelID, "stored_size", storedSize, "threads", p.config.UploadThreads)
	if err != nil {
		return nil, p.failPart(ctx, request, claim.LeaseToken, "telegram_upload_failed", errors.Join(err, pendingRenewError(renewErrors)))
	}

	// The message exists remotely from this point. Record it on the attempt's
	// own receipt: this survives part-lease loss, request cancellation and
	// ambiguous StorePart commits.
	pubCtx, cancelPub := partCleanupContext(ctx)
	pubErr := p.catalog.RecordPartPublication(pubCtx, uploads.RecordPublicationInput{
		UploadID:   request.UploadID,
		PartNo:     request.PartNo,
		LeaseToken: claim.LeaseToken,
		MessageID:  stored.MessageID,
		StoredSize: stored.Size,
		Salt:       salt,
	})
	cancelPub()
	if pubErr != nil && errors.Is(pubErr, uploads.ErrLeaseLost) {
		// The attempt no longer owns the part or its receipt. Its own message is
		// referenced nowhere; best-effort remove it.
		return nil, errors.Join(uploads.ErrLeaseLost, pendingRenewError(renewErrors), p.deleteUploaded(ctx, request.UserID, stored))
	}
	if renewErr := pendingRenewError(renewErrors); renewErr != nil {
		// Receipt durably recorded under this token; a retry adopts it. Do not
		// delete the message.
		return nil, renewErr
	}
	if stored.ChannelID != channelID || stored.MessageID <= 0 || stored.Size != storedSize {
		return nil, p.rejectPublished(ctx, request, claim.LeaseToken, stored, "telegram_size_mismatch", ErrStoredSizeMismatch)
	}
	if err := exact.Verify(); err != nil {
		return nil, p.rejectPublished(ctx, request, claim.LeaseToken, stored, "body_size_mismatch", err)
	}

	var (
		actualChecksum *string
		blockHashes    []byte
	)
	if shouldHash {
		blockHashes = hasher.Sum()
		if len(blockHashes) == 0 || len(blockHashes)%treehash.DigestSize != 0 {
			return nil, p.rejectPublished(ctx, request, claim.LeaseToken, stored, "hash_generation_failed", ErrInvalidUpload)
		}
		value := treehash.SumToHex(treehash.ComputeTreeHash(blockHashes))
		actualChecksum = &value
		if request.Checksum != nil && !strings.EqualFold(*request.Checksum, value) {
			return nil, p.rejectPublished(ctx, request, claim.LeaseToken, stored, "checksum_mismatch", ErrChecksumMismatch)
		}
	}

	part, err := p.commitPart(ctx, request, claim.LeaseToken, channelID, uploads.StorePartInput{
		UploadID:    request.UploadID,
		PartNo:      request.PartNo,
		LeaseToken:  claim.LeaseToken,
		MessageID:   stored.MessageID,
		StoredSize:  stored.Size,
		Checksum:    valueOrEmpty(actualChecksum),
		Salt:        salt,
		BlockHashes: append([]byte(nil), blockHashes...),
	})
	if err != nil {
		return nil, err
	}
	slog.DebugContext(ctx, "upload part completed", "upload_id", request.UploadID, "part_no", request.PartNo, "channel_id", stored.ChannelID, "message_id", stored.MessageID, "stored_size", stored.Size)
	return &UploadPartResult{Part: part}, nil
}

// adoptPart verifies the re-streamed plaintext body against a published
// receipt and commits the stored part without touching Telegram. The receipt
// owns message identity, stored size and salt.
func (p *Pipeline) adoptPart(ctx, uploadCtx context.Context, session *sqlcgen.UploadSession, request UploadPartRequest, claim *uploads.ClaimPartResult, renewErrors <-chan error) (*UploadPartResult, error) {
	adoption := claim.Adoption
	expectedStoredSize := request.PlainSize
	if session.Encryption {
		expectedStoredSize = contentcrypto.EncryptedSize(request.PlainSize)
	}

	exact := newExactReader(uploadCtx, request.Body, request.PlainSize)
	shouldHash := !p.config.DisableHashing || request.Checksum != nil || session.ExpectedHashAlgorithm.Valid
	reader := io.Reader(exact)
	var hasher *treehash.BlockHasher
	if shouldHash {
		hasher = treehash.NewBlockHasher()
		reader = io.TeeReader(exact, hasher)
	}
	if _, readErr := io.Copy(io.Discard, reader); readErr != nil {
		return nil, p.failPart(ctx, request, claim.LeaseToken, "body_size_mismatch", readErr)
	}
	if err := exact.Verify(); err != nil {
		return nil, p.failPart(ctx, request, claim.LeaseToken, "body_size_mismatch", err)
	}

	var (
		actualChecksum *string
		blockHashes    []byte
	)
	if shouldHash {
		blockHashes = hasher.Sum()
		if len(blockHashes) == 0 || len(blockHashes)%treehash.DigestSize != 0 {
			return nil, p.failPart(ctx, request, claim.LeaseToken, "hash_generation_failed", ErrInvalidUpload)
		}
		value := treehash.SumToHex(treehash.ComputeTreeHash(blockHashes))
		actualChecksum = &value
		if request.Checksum != nil && !strings.EqualFold(*request.Checksum, value) {
			// The re-sent body disagrees with the stored message; keep the
			// receipt takeable for a retry with the correct body.
			return nil, p.failPart(ctx, request, claim.LeaseToken, "checksum_mismatch", ErrChecksumMismatch)
		}
	}
	if renewErr := pendingRenewError(renewErrors); renewErr != nil {
		return nil, renewErr
	}
	if adoption.StoredSize != expectedStoredSize {
		return nil, p.rejectPublished(ctx, request, claim.LeaseToken, telegramstore.StoredPart{ChannelID: adoption.ChannelID, MessageID: adoption.MessageID, Size: adoption.StoredSize}, "telegram_size_mismatch", ErrStoredSizeMismatch)
	}

	part, err := p.commitPart(ctx, request, claim.LeaseToken, adoption.ChannelID, uploads.StorePartInput{
		UploadID:    request.UploadID,
		PartNo:      request.PartNo,
		LeaseToken:  claim.LeaseToken,
		MessageID:   adoption.MessageID,
		StoredSize:  adoption.StoredSize,
		Checksum:    valueOrEmpty(actualChecksum),
		Salt:        nil,
		BlockHashes: append([]byte(nil), blockHashes...),
	})
	if err != nil {
		return nil, err
	}
	slog.DebugContext(ctx, "upload part adopted from published receipt", "upload_id", request.UploadID, "part_no", request.PartNo, "channel_id", adoption.ChannelID, "message_id", adoption.MessageID, "stored_size", adoption.StoredSize)
	return &UploadPartResult{Part: part}, nil
}

// commitPart stores the part and reconciles ambiguous failures. When the
// database outcome is uncertain it re-reads the part: a part already stored
// with the same message is the original commit result and is returned as
// success. Nothing is deleted on failure; published receipts remain takeable
// or are swept by the cleanup worker.
func (p *Pipeline) commitPart(ctx context.Context, request UploadPartRequest, leaseToken uuid.UUID, channelID int64, in uploads.StorePartInput) (*sqlcgen.UploadPart, error) {
	storeCtx, cancelStore := partCleanupContext(ctx)
	defer cancelStore()
	part, err := p.catalog.StorePart(storeCtx, in)
	if err == nil {
		return part, nil
	}
	if errors.Is(err, uploads.ErrLeaseLost) {
		return nil, err
	}
	retryCtx, cancelRetry := partCleanupContext(ctx)
	defer cancelRetry()
	if part, retryErr := p.catalog.StorePart(retryCtx, in); retryErr == nil {
		return part, nil
	}
	committed, getErr := p.catalog.GetPart(retryCtx, request.UserID, request.UploadID, request.PartNo)
	if getErr == nil && committed.State == sqlcgen.UploadPartStateStored &&
		committed.ChannelID == channelID && committed.MessageID.Valid &&
		committed.MessageID.Int64 == in.MessageID {
		return committed, nil
	}
	return nil, err
}

// rejectPublished condemns this attempt's published receipt, removes the
// remote message and marks the part failed. A lost lease (zero condemned rows)
// means the message belongs to another owner and is left untouched.
func (p *Pipeline) rejectPublished(ctx context.Context, request UploadPartRequest, leaseToken uuid.UUID, published telegramstore.StoredPart, code string, cause error) error {
	cleanupCtx, cancel := partCleanupContext(ctx)
	defer cancel()
	var cleanup error
	condemned, condemnErr := p.catalog.CondemnPartReceipt(cleanupCtx, uploads.CondemnPartReceiptInput{
		UploadID:   request.UploadID,
		PartNo:     request.PartNo,
		LeaseToken: leaseToken,
	})
	if condemnErr != nil {
		cleanup = errors.Join(cleanup, fmt.Errorf("condemn uploaded part: %w", condemnErr))
	}
	if condemnErr == nil && condemned > 0 {
		if err := p.storage.DeleteMessages(cleanupCtx, request.UserID, published.ChannelID, []int64{published.MessageID}); err != nil {
			cleanup = errors.Join(cleanup, fmt.Errorf("compensate Telegram upload: %w", err))
		} else if err := p.catalog.DeletePartReceipt(cleanupCtx, uploads.DeletePartReceiptInput{
			UploadID:   request.UploadID,
			PartNo:     request.PartNo,
			LeaseToken: leaseToken,
			MessageID:  published.MessageID,
		}); err != nil {
			cleanup = errors.Join(cleanup, err)
		}
	}
	return p.failPart(ctx, request, leaseToken, code, errors.Join(cause, cleanup))
}

func (p *Pipeline) failPart(ctx context.Context, request UploadPartRequest, leaseToken uuid.UUID, code string, cause error) error {
	cleanupCtx, cancel := partCleanupContext(ctx)
	defer cancel()
	_, failErr := p.catalog.FailPart(cleanupCtx, uploads.FailPartInput{
		UploadID:   request.UploadID,
		PartNo:     request.PartNo,
		LeaseToken: leaseToken,
		ErrorCode:  code,
	})
	if failErr != nil && !errors.Is(failErr, uploads.ErrLeaseLost) {
		return errors.Join(cause, failErr)
	}
	return cause
}

func (p *Pipeline) renewPartLease(ctx context.Context, cancel context.CancelCauseFunc, request UploadPartRequest, leaseToken uuid.UUID) <-chan error {
	errorsCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(p.config.LeaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := partCleanupContext(ctx)
				err := p.catalog.RenewPart(renewCtx, uploads.RenewPartInput{
					UploadID: request.UploadID, PartNo: request.PartNo, LeaseToken: leaseToken,
				})
				stop()
				if err != nil {
					errorsCh <- err
					cancel(err)
					return
				}
			}
		}
	}()
	return errorsCh
}

func pendingRenewError(errorsCh <-chan error) error {
	select {
	case err := <-errorsCh:
		return err
	default:
		return nil
	}
}

func partCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), partCleanupTimeout)
}

func (p *Pipeline) deleteUploaded(ctx context.Context, userID int64, part telegramstore.StoredPart) error {
	if part.ChannelID == 0 || part.MessageID <= 0 {
		return nil
	}
	if err := p.storage.DeleteMessages(ctx, userID, part.ChannelID, []int64{part.MessageID}); err != nil {
		return fmt.Errorf("compensate Telegram upload: %w", err)
	}
	return nil
}

func (p *Pipeline) partName(fileName string, partNo int32) string {
	if !p.config.RandomizePartNames {
		return fmt.Sprintf("%s.%03d", fileName, partNo)
	}
	digest := sha256.Sum256([]byte(uuid.NewString()))
	return hex.EncodeToString(digest[:])
}
func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func generateSalt(random io.Reader) (string, error) {
	if random == nil {
		return "", errors.New("random source is required")
	}
	seed := make([]byte, 32)
	if _, err := io.ReadFull(random, seed); err != nil {
		return "", fmt.Errorf("read encryption salt entropy: %w", err)
	}
	digest := sha256.Sum256(seed)
	return base64.URLEncoding.EncodeToString(digest[:]), nil
}

func normalizeOptionalChecksum(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	normalized := strings.ToLower(strings.TrimSpace(*value))
	digest, err := hex.DecodeString(normalized)
	if err != nil || len(digest) != treehash.DigestSize {
		return nil, ErrInvalidUpload
	}
	return &normalized, nil
}

func checksumMatches(stored string, valid bool, expected *string) bool {
	if expected == nil {
		return true
	}
	return valid && strings.EqualFold(stored, *expected)
}

type exactReader struct {
	ctx       context.Context
	source    io.Reader
	remaining int64
}

func newExactReader(ctx context.Context, source io.Reader, size int64) *exactReader {
	return &exactReader{ctx: ctx, source: source, remaining: size}
}

func (r *exactReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.source.Read(p)
	r.remaining -= int64(n)
	if errors.Is(err, io.EOF) && r.remaining > 0 {
		return n, ErrBodyTooShort
	}
	if n == 0 && err == nil {
		return 0, io.ErrNoProgress
	}
	return n, err
}

func (r *exactReader) Verify() error {
	if r.remaining != 0 {
		return ErrBodyTooShort
	}
	var probe [1]byte
	n, err := r.source.Read(probe[:])
	if n > 0 {
		return ErrBodyTooLong
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// StaticKeyProvider is suitable for configuration-backed key rotation. New
// uploads reference a version while old files remain decryptable.
type StaticKeyProvider map[int32]string

func (p StaticKeyProvider) Key(_ context.Context, _ int64, version int32) (string, error) {
	key, ok := p[version]
	if !ok || key == "" {
		return "", ErrEncryptionKey
	}
	return key, nil
}
