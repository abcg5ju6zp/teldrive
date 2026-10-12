package localtelegram

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

type Server struct {
	root          string
	statePath     string
	uploadsDir    string
	documentsDir  string
	pendingDir    string
	quarantineDir string

	mu     sync.Mutex
	state  persistedState
	report RecoveryReport
}

func Open(root string) (*Server, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("local Telegram root is required")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve local Telegram root: %w", err)
	}
	server := &Server{
		root:          root,
		statePath:     filepath.Join(root, stateFileName),
		uploadsDir:    filepath.Join(root, uploadsDirName),
		documentsDir:  filepath.Join(root, documentsDirName),
		pendingDir:    filepath.Join(root, pendingDirName),
		quarantineDir: filepath.Join(root, quarantineDirName),
	}
	for _, dir := range []string{server.root, server.uploadsDir, server.documentsDir, server.pendingDir, server.quarantineDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create local Telegram directory %q: %w", dir, err)
		}
	}
	if err := server.recover(); err != nil {
		return nil, err
	}
	return server, nil
}

func (s *Server) Root() string {
	if s == nil {
		return ""
	}
	return s.root
}

func (s *Server) Client() *tg.Client {
	if s == nil {
		return nil
	}
	return tg.NewClient(s)
}

func (s *Server) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if s == nil || input == nil || output == nil {
		return errors.New("local Telegram invoker is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	response, err := s.handle(ctx, input)
	if err != nil {
		return err
	}
	buffer := new(bin.Buffer)
	if err := response.Encode(buffer); err != nil {
		return fmt.Errorf("encode local Telegram response: %w", err)
	}
	if err := output.Decode(buffer); err != nil {
		return fmt.Errorf("decode local Telegram response: %w", err)
	}
	return nil
}

func (s *Server) handle(ctx context.Context, input bin.Encoder) (bin.Encoder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch request := input.(type) {
	case *tg.ChannelsGetChannelsRequest:
		return s.getChannels(request), nil
	case *tg.MessagesGetDialogsRequest:
		return s.getDialogs(request), nil
	case *tg.MessagesGetHistoryRequest:
		return s.getHistory(request), nil
	case *tg.UsersGetUsersRequest:
		return s.getUsers(request), nil
	case *tg.UploadSaveFilePartRequest:
		return s.saveUploadPart(ctx, request.FileID, request.FilePart, request.Bytes)
	case *tg.UploadSaveBigFilePartRequest:
		return s.saveUploadPart(ctx, request.FileID, request.FilePart, request.Bytes)
	case *tg.MessagesSendMediaRequest:
		return s.sendMedia(ctx, request)
	case *tg.ChannelsGetMessagesRequest:
		return s.getMessages(request), nil
	case *tg.UploadGetFileRequest:
		return s.getFile(ctx, request)
	case *tg.ChannelsDeleteMessagesRequest:
		return s.deleteMessages(request)
	case *tg.ChannelsCreateChannelRequest:
		return s.createChannel(request)
	case *tg.ChannelsDeleteChannelRequest:
		return s.deleteChannel(request)
	default:
		return nil, fmt.Errorf("local Telegram RPC %T is not implemented", input)
	}
}

func (s *Server) getChannels(request *tg.ChannelsGetChannelsRequest) *tg.MessagesChats {
	chats := make([]tg.ChatClass, 0, len(request.ID))
	for _, input := range request.ID {
		channelID, ok := inputChannelID(input)
		if !ok {
			continue
		}
		if record, exists := s.state.Channels[channelKey(channelID)]; exists {
			chats = append(chats, telegramChannel(record))
		}
	}
	return &tg.MessagesChats{Chats: chats}
}

func (s *Server) getDialogs(request *tg.MessagesGetDialogsRequest) *tg.MessagesDialogs {
	channels := make([]channelRecord, 0, len(s.state.Channels))
	for _, channel := range s.state.Channels {
		channels = append(channels, channel)
	}
	slices.SortFunc(channels, func(a, b channelRecord) int { return cmp.Compare(a.ID, b.ID) })
	limit := request.Limit
	if limit <= 0 || limit > len(channels) {
		limit = len(channels)
	}
	dialogs := make([]tg.DialogClass, 0, limit)
	chats := make([]tg.ChatClass, 0, limit)
	for _, channel := range channels[:limit] {
		dialogs = append(dialogs, &tg.Dialog{
			Peer:           &tg.PeerChannel{ChannelID: channel.ID},
			NotifySettings: tg.PeerNotifySettings{},
		})
		chats = append(chats, telegramChannel(channel))
	}
	return &tg.MessagesDialogs{Dialogs: dialogs, Chats: chats}
}

func (s *Server) getUsers(*tg.UsersGetUsersRequest) *tg.UserClassVector {
	return &tg.UserClassVector{Elems: []tg.UserClass{&tg.User{
		Self: true, ID: 1, AccessHash: 1_000_001,
		FirstName: "Local", LastName: "Telegram", Username: "localtelegram",
	}}}
}

func (s *Server) saveUploadPart(ctx context.Context, fileID int64, part int, payload []byte) (bin.Encoder, error) {
	if fileID == 0 || part < 0 {
		return nil, errors.New("invalid local Telegram upload part")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(s.uploadsDir, fmt.Sprintf("%d", fileID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create local Telegram upload: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%08d.part", part))
	if err := writeAtomic(path, payload, 0o600); err != nil {
		return nil, fmt.Errorf("write local Telegram upload part: %w", err)
	}
	return &tg.BoolTrue{}, nil
}

func (s *Server) sendMedia(ctx context.Context, request *tg.MessagesSendMediaRequest) (bin.Encoder, error) {
	channelID, ok := inputPeerChannelID(request.Peer)
	if !ok {
		return nil, errors.New("local Telegram destination is not a channel")
	}
	channel, exists := s.state.Channels[channelKey(channelID)]
	if !exists {
		return nil, fmt.Errorf("local Telegram channel %d does not exist", channelID)
	}

	messageID := s.state.NextMessageID
	s.state.NextMessageID++
	createdAt := int(time.Now().Unix())

	var document documentRecord
	switch media := request.Media.(type) {
	case *tg.InputMediaUploadedDocument:
		created, err := s.finalizeUpload(ctx, media, channelID, messageID, createdAt)
		if err != nil {
			s.state.NextMessageID = messageID
			return nil, err
		}
		document = created
	case *tg.InputMediaDocument:
		input, ok := media.ID.(*tg.InputDocument)
		if !ok {
			s.state.NextMessageID = messageID
			return nil, fmt.Errorf("local Telegram document reference %T is not supported", media.ID)
		}
		stored, exists := s.state.Documents[documentKey(input.ID)]
		if !exists || stored.AccessHash != input.AccessHash {
			s.state.NextMessageID = messageID
			return nil, fmt.Errorf("local Telegram document %d does not exist", input.ID)
		}
		document = stored
		if err := s.commitMessage(channelID, messageID, document.ID, createdAt); err != nil {
			s.state.NextMessageID = messageID
			return nil, err
		}
	default:
		s.state.NextMessageID = messageID
		return nil, fmt.Errorf("local Telegram media %T is not supported", request.Media)
	}

	message := messageRecord{
		ChannelID:  channelID,
		ID:         messageID,
		DocumentID: document.ID,
		CreatedAt:  createdAt,
	}
	telegramMessage := s.telegramMessage(message)
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: telegramMessage, Pts: messageID, PtsCount: 1}},
		Chats:   []tg.ChatClass{telegramChannel(channel)},
		Date:    message.CreatedAt,
		Seq:     messageID,
	}, nil
}

// finalizeUpload turns uploaded parts into a committed document message.
//
// The on-disk ordering is crash-safe:
//  1. parts are assembled and hashed into pending/<id>.blob,
//  2. a pending/<id>.json intent pins document/message IDs and metadata,
//  3. the blob is published as documents/<id>.bin,
//  4. state.json atomically commits document + message indexes.
//
// A restart at any point converges: intents are replayed, unpublished content
// is quarantined, and confirmed messages are never republished.
func (s *Server) finalizeUpload(ctx context.Context, media *tg.InputMediaUploadedDocument, channelID int64, messageID int, createdAt int) (documentRecord, error) {
	fileID, parts, name, ok := inputFileDetails(media.File)
	if !ok || parts <= 0 {
		return documentRecord{}, fmt.Errorf("local Telegram input file %T is invalid", media.File)
	}
	uploadDir := filepath.Join(s.uploadsDir, fmt.Sprintf("%d", fileID))
	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		return documentRecord{}, fmt.Errorf("read local Telegram upload: %w", err)
	}
	if len(entries) != parts {
		return documentRecord{}, fmt.Errorf("local Telegram upload %d has %d parts, want %d", fileID, len(entries), parts)
	}

	documentID := s.state.NextDocumentID
	s.state.NextDocumentID++
	stagedPath := filepath.Join(s.pendingDir, stagingBlobName(documentID))
	intentPath := filepath.Join(s.pendingDir, fmt.Sprintf("%d.json", documentID))
	installedPath := filepath.Join(s.documentsDir, fmt.Sprintf("%d.bin", documentID))
	committed := false
	defer func() {
		if !committed {
			// Live failure before the commit point: roll back durable
			// artifacts and in-memory index changes so the running view
			// matches the last committed view.
			delete(s.state.Messages, messageKey(channelID, messageID))
			delete(s.state.Documents, documentKey(documentID))
			s.state.NextDocumentID = documentID
			_ = os.Remove(intentPath)
			_ = os.Remove(stagedPath)
			_ = os.Remove(installedPath)
			_ = syncDir(s.pendingDir)
			_ = syncDir(s.documentsDir)
			_ = os.RemoveAll(uploadDir)
		}
	}()

	digest, size, err := s.assembleUpload(ctx, fileID, parts, stagedPath)
	if err != nil {
		return documentRecord{}, err
	}

	mimeType := strings.TrimSpace(media.MimeType)
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	if attributeName := filenameFromAttributes(media.Attributes); attributeName != "" {
		name = attributeName
	}
	record := documentRecord{
		ID:            documentID,
		AccessHash:    documentID + 1_000_000,
		FileReference: []byte(digest),
		MimeType:      mimeType,
		Size:          size,
		DCID:          1,
		FileName:      name,
		CreatedAt:     createdAt,
	}
	commit := pendingCommit{
		Version:      stateVersion,
		ChannelID:    channelID,
		MessageID:    messageID,
		UploadFileID: fileID,
		UploadParts:  parts,
		Document:     record,
		CreatedAt:    createdAt,
	}
	payload, err := json.MarshalIndent(commit, "", "  ")
	if err != nil {
		return documentRecord{}, fmt.Errorf("encode local Telegram publish intent: %w", err)
	}
	// Intent must not appear before the staged blob it describes is durable.
	if err := writeAtomicFile(intentPath, append(payload, '\n'), 0o600); err != nil {
		return documentRecord{}, fmt.Errorf("write local Telegram publish intent: %w", err)
	}
	if err := replaceFile(stagedPath, installedPath); err != nil {
		return documentRecord{}, fmt.Errorf("publish local Telegram document: %w", err)
	}

	s.state.Documents[documentKey(documentID)] = record
	s.state.Messages[messageKey(channelID, messageID)] = messageRecord{
		ChannelID:  channelID,
		ID:         messageID,
		DocumentID: documentID,
		CreatedAt:  createdAt,
	}
	if err := saveState(s.statePath, s.state); err != nil {
		return documentRecord{}, err
	}
	if err := s.removePublishArtifacts(commit); err != nil {
		return documentRecord{}, err
	}
	committed = true
	return record, nil
}

// commitMessage appends a message for an already-committed document (used by
// document copies) and persists the index atomically.
func (s *Server) commitMessage(channelID int64, messageID int, documentID int64, createdAt int) error {
	s.state.Messages[messageKey(channelID, messageID)] = messageRecord{
		ChannelID:  channelID,
		ID:         messageID,
		DocumentID: documentID,
		CreatedAt:  createdAt,
	}
	if err := saveState(s.statePath, s.state); err != nil {
		delete(s.state.Messages, messageKey(channelID, messageID))
		return err
	}
	return nil
}

// assembleUpload concatenates numbered upload parts into stagedPath,
// returning the hex SHA-256 digest and total size.
func (s *Server) assembleUpload(ctx context.Context, fileID int64, parts int, stagedPath string) (string, int64, error) {
	temp, err := os.CreateTemp(s.pendingDir, stagedTempPrefix+"*.tmp")
	if err != nil {
		return "", 0, fmt.Errorf("create local Telegram staged document: %w", err)
	}
	tempName := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempName)
		}
	}()

	uploadDir := filepath.Join(s.uploadsDir, fmt.Sprintf("%d", fileID))
	hash := sha256.New()
	writer := io.MultiWriter(temp, hash)
	var size int64
	for part := range parts {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		partFile, err := os.Open(filepath.Join(uploadDir, fmt.Sprintf("%08d.part", part)))
		if err != nil {
			return "", 0, fmt.Errorf("open local Telegram upload part %d: %w", part, err)
		}
		written, copyErr := io.Copy(writer, partFile)
		closeErr := partFile.Close()
		if copyErr != nil {
			return "", 0, fmt.Errorf("copy local Telegram upload part %d: %w", part, copyErr)
		}
		if closeErr != nil {
			return "", 0, fmt.Errorf("close local Telegram upload part %d: %w", part, closeErr)
		}
		size += written
	}
	if err := temp.Sync(); err != nil {
		return "", 0, fmt.Errorf("sync local Telegram staged document: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", 0, fmt.Errorf("close local Telegram staged document: %w", err)
	}
	if err := os.Rename(tempName, stagedPath); err != nil {
		return "", 0, fmt.Errorf("stage local Telegram document: %w", err)
	}
	if err := syncDir(s.pendingDir); err != nil {
		return "", 0, err
	}
	cleanup = false
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

// assembleFromParts rebuilds stagedPath from the upload parts captured by a
// publish intent during startup recovery.
func (s *Server) assembleFromParts(commit pendingCommit, stagedPath string) error {
	_, _, err := s.assembleUpload(context.Background(), commit.UploadFileID, commit.UploadParts, stagedPath)
	if err != nil {
		return err
	}
	digest, size, err := hashFile(stagedPath)
	if err != nil {
		return err
	}
	if size != commit.Document.Size || digest != documentHash(commit.Document) {
		_ = os.Remove(stagedPath)
		return fmt.Errorf("rebuilt document %d does not match intent", commit.Document.ID)
	}
	return nil
}

func (s *Server) getMessages(request *tg.ChannelsGetMessagesRequest) *tg.MessagesMessages {
	channelID, ok := inputChannelID(request.Channel)
	if !ok {
		return &tg.MessagesMessages{}
	}
	messages := make([]tg.MessageClass, 0, len(request.ID))
	for _, input := range request.ID {
		messageID, ok := inputMessageID(input)
		if !ok {
			continue
		}
		if record, exists := s.state.Messages[messageKey(channelID, messageID)]; exists {
			messages = append(messages, s.telegramMessage(record))
		}
	}
	chats := make([]tg.ChatClass, 0, 1)
	if channel, exists := s.state.Channels[channelKey(channelID)]; exists {
		chats = append(chats, telegramChannel(channel))
	}
	return &tg.MessagesMessages{Messages: messages, Chats: chats}
}

func (s *Server) getFile(ctx context.Context, request *tg.UploadGetFileRequest) (bin.Encoder, error) {
	location, ok := request.Location.(*tg.InputDocumentFileLocation)
	if !ok {
		return nil, fmt.Errorf("local Telegram file location %T is not supported", request.Location)
	}
	document, exists := s.state.Documents[documentKey(location.ID)]
	if !exists || document.AccessHash != location.AccessHash {
		return nil, fmt.Errorf("local Telegram document %d does not exist", location.ID)
	}
	if request.Offset < 0 || request.Limit < 0 || request.Offset > document.Size {
		return nil, errors.New("invalid local Telegram file range")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(s.documentsDir, fmt.Sprintf("%d.bin", document.ID)))
	if err != nil {
		return nil, fmt.Errorf("open local Telegram document: %w", err)
	}
	defer file.Close()
	limit := int64(request.Limit)
	if request.Offset+limit > document.Size {
		limit = document.Size - request.Offset
	}
	payload := make([]byte, limit)
	if limit > 0 {
		if _, err := file.ReadAt(payload, request.Offset); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read local Telegram document: %w", err)
		}
	}
	return &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Mtime: document.CreatedAt, Bytes: payload}, nil
}

func (s *Server) deleteMessages(request *tg.ChannelsDeleteMessagesRequest) (bin.Encoder, error) {
	channelID, ok := inputChannelID(request.Channel)
	if !ok {
		return nil, errors.New("invalid local Telegram channel")
	}
	for _, messageID := range request.ID {
		delete(s.state.Messages, messageKey(channelID, messageID))
	}
	// Commit the index before unlinking blobs: a crash in between leaves
	// unreferenced files, which startup quarantines, never dangling index
	// entries pointing at missing files.
	blobPaths := s.detachUnreferencedDocuments()
	if err := saveState(s.statePath, s.state); err != nil {
		return nil, err
	}
	if err := removeDocumentBlobs(s.documentsDir, blobPaths); err != nil {
		return nil, err
	}
	return &tg.MessagesAffectedMessages{Pts: s.state.NextMessageID, PtsCount: len(request.ID)}, nil
}

func (s *Server) createChannel(request *tg.ChannelsCreateChannelRequest) (bin.Encoder, error) {
	title := strings.TrimSpace(request.Title)
	if title == "" {
		return nil, errors.New("local Telegram channel title is required")
	}
	channelID := s.state.NextChannelID
	s.state.NextChannelID++
	record := channelRecord{
		ID:         channelID,
		AccessHash: channelID + 1_000_000,
		Title:      title,
		CreatedAt:  int(time.Now().Unix()),
	}
	s.state.Channels[channelKey(channelID)] = record
	if err := saveState(s.statePath, s.state); err != nil {
		return nil, err
	}
	return &tg.Updates{Chats: []tg.ChatClass{telegramChannel(record)}, Date: record.CreatedAt, Seq: s.state.NextMessageID}, nil
}

func (s *Server) deleteChannel(request *tg.ChannelsDeleteChannelRequest) (bin.Encoder, error) {
	channelID, ok := inputChannelID(request.Channel)
	if !ok {
		return nil, errors.New("invalid local Telegram channel")
	}
	delete(s.state.Channels, channelKey(channelID))
	for key, message := range s.state.Messages {
		if message.ChannelID == channelID {
			delete(s.state.Messages, key)
		}
	}
	blobPaths := s.detachUnreferencedDocuments()
	if err := saveState(s.statePath, s.state); err != nil {
		return nil, err
	}
	if err := removeDocumentBlobs(s.documentsDir, blobPaths); err != nil {
		return nil, err
	}
	return &tg.Updates{Date: int(time.Now().Unix()), Seq: s.state.NextMessageID}, nil
}

// detachUnreferencedDocuments removes documents that no longer have a message
// reference from the in-memory index, returning the blob paths that may be
// unlinked after the index commit lands.
func (s *Server) detachUnreferencedDocuments() []string {
	referenced := make(map[int64]struct{}, len(s.state.Messages))
	for _, message := range s.state.Messages {
		referenced[message.DocumentID] = struct{}{}
	}
	paths := make([]string, 0)
	for key, document := range s.state.Documents {
		if _, ok := referenced[document.ID]; ok {
			continue
		}
		paths = append(paths, filepath.Join(s.documentsDir, fmt.Sprintf("%d.bin", document.ID)))
		delete(s.state.Documents, key)
	}
	return paths
}

// removeDocumentBlobs unlinks already-detached document blobs after their
// index commit. Missing files are expected from retried deletes.
func removeDocumentBlobs(documentsDir string, paths []string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove local Telegram document: %w", err)
		}
	}
	if len(paths) > 0 {
		if err := syncDir(documentsDir); err != nil {
			return err
		}
	}
	return nil
}

// getHistory serves messages.getHistory for storage maintenance traversal.
// Results are ordered by message ID descending, strictly older than
// OffsetID, matching the pagination contract used by the orphan cleanup.
func (s *Server) getHistory(request *tg.MessagesGetHistoryRequest) *tg.MessagesChannelMessages {
	channelID, ok := inputPeerChannelID(request.Peer)
	response := &tg.MessagesChannelMessages{}
	if !ok {
		return response
	}
	channel, exists := s.state.Channels[channelKey(channelID)]
	if !exists {
		return response
	}
	ids := make([]int, 0)
	for _, message := range s.state.Messages {
		if message.ChannelID != channelID {
			continue
		}
		if request.OffsetID > 0 && message.ID >= request.OffsetID {
			continue
		}
		ids = append(ids, message.ID)
	}
	slices.SortFunc(ids, func(a, b int) int { return cmp.Compare(b, a) })
	limit := request.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	response.Count = len(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	messages := make([]tg.MessageClass, 0, len(ids))
	for _, id := range ids {
		messages = append(messages, s.telegramMessage(s.state.Messages[messageKey(channelID, id)]))
	}
	response.Messages = messages
	response.Chats = []tg.ChatClass{telegramChannel(channel)}
	return response
}

func (s *Server) telegramMessage(record messageRecord) *tg.Message {
	document := s.state.Documents[documentKey(record.DocumentID)]
	return &tg.Message{
		ID:     record.ID,
		Out:    true,
		Post:   true,
		PeerID: &tg.PeerChannel{ChannelID: record.ChannelID},
		Date:   record.CreatedAt,
		Media:  &tg.MessageMediaDocument{Document: telegramDocument(document)},
	}
}

// Ping implements the readiness contract: the committed index must parse at
// the supported version and every managed directory must be present.
func (s *Server) Ping(ctx context.Context) error {
	if s == nil {
		return errors.New("local Telegram server is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		return fmt.Errorf("read local Telegram state: %w", err)
	}
	state, err := decodeState(data)
	if err != nil {
		return fmt.Errorf("decode local Telegram state: %w", err)
	}
	if err := validateVersion(state); err != nil {
		return err
	}
	for _, dir := range []string{s.uploadsDir, s.documentsDir, s.pendingDir, s.quarantineDir} {
		info, err := os.Stat(dir)
		if err != nil {
			return fmt.Errorf("local Telegram directory %q is unavailable: %w", filepath.Base(dir), err)
		}
		if !info.IsDir() {
			return fmt.Errorf("local Telegram path %q is not a directory", filepath.Base(dir))
		}
	}
	return nil
}

// RecoveryReport summarizes the last startup reconciliation.
func (s *Server) RecoveryReport() RecoveryReport {
	if s == nil {
		return RecoveryReport{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	report := s.report
	report.Quarantined = append([]QuarantineEntry(nil), s.report.Quarantined...)
	report.DanglingReferences = append([]DanglingReference(nil), s.report.DanglingReferences...)
	return report
}

func telegramChannel(record channelRecord) *tg.Channel {
	return &tg.Channel{
		Creator:    true,
		Broadcast:  true,
		ID:         record.ID,
		AccessHash: record.AccessHash,
		Title:      record.Title,
		Photo:      &tg.ChatPhotoEmpty{},
		Date:       record.CreatedAt,
	}
}

func telegramDocument(record documentRecord) *tg.Document {
	attributes := make([]tg.DocumentAttributeClass, 0, 1)
	if record.FileName != "" {
		attributes = append(attributes, &tg.DocumentAttributeFilename{FileName: record.FileName})
	}
	return &tg.Document{
		ID:            record.ID,
		AccessHash:    record.AccessHash,
		FileReference: append([]byte(nil), record.FileReference...),
		Date:          record.CreatedAt,
		MimeType:      record.MimeType,
		Size:          record.Size,
		DCID:          record.DCID,
		Attributes:    attributes,
	}
}

func writeAtomic(path string, payload []byte, mode os.FileMode) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".part-*.tmp")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempName)
		}
	}()
	if err := temp.Chmod(mode); err != nil {
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func inputChannelID(input tg.InputChannelClass) (int64, bool) {
	channel, ok := input.(*tg.InputChannel)
	if !ok || channel.ChannelID == 0 {
		return 0, false
	}
	return channel.ChannelID, true
}

func inputPeerChannelID(input tg.InputPeerClass) (int64, bool) {
	channel, ok := input.(*tg.InputPeerChannel)
	if !ok || channel.ChannelID == 0 {
		return 0, false
	}
	return channel.ChannelID, true
}

func inputMessageID(input tg.InputMessageClass) (int, bool) {
	message, ok := input.(*tg.InputMessageID)
	if !ok || message.ID <= 0 {
		return 0, false
	}
	return message.ID, true
}

func inputFileDetails(input tg.InputFileClass) (id int64, parts int, name string, ok bool) {
	switch file := input.(type) {
	case *tg.InputFile:
		return file.ID, file.Parts, file.Name, file.ID != 0 && file.Parts > 0
	case *tg.InputFileBig:
		return file.ID, file.Parts, file.Name, file.ID != 0 && file.Parts > 0
	default:
		return 0, 0, "", false
	}
}

func filenameFromAttributes(attributes []tg.DocumentAttributeClass) string {
	for _, attribute := range attributes {
		if filename, ok := attribute.(*tg.DocumentAttributeFilename); ok {
			return strings.TrimSpace(filename.FileName)
		}
	}
	return ""
}

type Runner struct {
	client *tg.Client
}

func NewRunner(server *Server) (Runner, error) {
	if server == nil {
		return Runner{}, errors.New("local Telegram server is required")
	}
	return Runner{client: server.Client()}, nil
}

func (r Runner) Run(ctx context.Context, userID int64, _ telegramstore.Operation, fn func(context.Context, *tg.Client) error) error {
	if r.client == nil || userID <= 0 || fn == nil {
		return telegramstore.ErrInvalidRequest
	}
	return fn(ctx, r.client)
}

var _ tg.Invoker = (*Server)(nil)
var _ telegramstore.Runner = Runner{}
