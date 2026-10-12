package app

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/gotd/log/logslog"
	"github.com/gotd/td/telegram"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tgdrive/teldrive/v2/internal/authn"
	"github.com/tgdrive/teldrive/v2/internal/botgateway"
	"github.com/tgdrive/teldrive/v2/internal/bots"
	"github.com/tgdrive/teldrive/v2/internal/cache"
	"github.com/tgdrive/teldrive/v2/internal/config"
	"github.com/tgdrive/teldrive/v2/internal/health"
	"github.com/tgdrive/teldrive/v2/internal/localtelegram"
	"github.com/tgdrive/teldrive/v2/internal/logingateway"
	"github.com/tgdrive/teldrive/v2/internal/secureblob"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

type telegramComponents struct {
	login           authn.TelegramLogin
	verifier        bots.Verifier
	account         telegramstore.Account
	storage         telegramstore.Storage
	downloadClients *telegramstore.DownloadClientPool
	readiness       health.Pinger
}

func buildLegacyBotVerifier(cfg config.Config, logger *slog.Logger) (bots.Verifier, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Telegram.Backend)) {
	case "filesystem":
		return localBotVerifier{}, nil
	case "remote":
		factory, err := newRemoteTelegramFactory(cfg, logger)
		if err != nil {
			return nil, err
		}
		verifier, err := botgateway.NewGotdVerifier(factory)
		if err != nil {
			return nil, fmt.Errorf("create bot verifier: %w", err)
		}
		return verifier, nil
	default:
		return nil, fmt.Errorf("unsupported Telegram backend %q", cfg.Telegram.Backend)
	}
}

func newRemoteTelegramFactory(cfg config.Config, logger *slog.Logger) (*telegramstore.Factory, error) {
	gotdLogger := logslog.New(logger)
	if !cfg.Telegram.ClientLogging {
		gotdLogger = nil
	}
	factory, err := telegramstore.NewFactory(telegramstore.FactoryConfig{
		AppID: cfg.Telegram.AppID, AppHash: cfg.Telegram.AppHash,
		Device: telegram.DeviceConfig{
			DeviceModel: cfg.Telegram.DeviceModel, SystemVersion: cfg.Telegram.SystemVersion,
			AppVersion: cfg.Telegram.AppVersion, LangCode: cfg.Telegram.LanguageCode,
			SystemLangCode: cfg.Telegram.SystemLanguageCode, LangPack: cfg.Telegram.LanguagePack,
		},
		DialTimeout: cfg.Telegram.DialTimeout, ReconnectTimeout: cfg.Telegram.ReconnectTimeout,
		MaxRetries: cfg.Telegram.MaxRetries, RateLimit: cfg.Telegram.RateLimit,
		RateInterval: cfg.Telegram.RateInterval, RateBurst: cfg.Telegram.RateBurst,
		Proxy: cfg.Telegram.Proxy, MTProxyAddress: cfg.Telegram.MTProxy.Address,
		MTProxySecret: cfg.Telegram.MTProxy.Secret,
		Logger:        gotdLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("create Telegram client factory: %w", err)
	}
	return factory, nil
}

func buildTelegramComponents(cfg config.Config, pool *pgxpool.Pool, cipher *secureblob.Cipher, logger *slog.Logger, injected telegramstore.Storage, globalCache cache.Cacher) (telegramComponents, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Telegram.Backend)) {
	case "filesystem":
		root, err := expandHomePath(cfg.Telegram.LocalRoot)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("resolve local Telegram root: %w", err)
		}
		server, err := localtelegram.Open(root)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("open local Telegram emulator: %w", err)
		}
		logRecoveryReport(logger, root, server.RecoveryReport())
		runner, err := localtelegram.NewRunner(server)
		if err != nil {
			return telegramComponents{}, err
		}
		account, err := telegramstore.NewGotdAccount(runner)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("create local Telegram account gateway: %w", err)
		}
		storage := injected
		if storage == nil {
			storage = telegramstore.NewGotdStorage(runner, globalCache)
		}
		return telegramComponents{
			login: localTelegramLogin{}, verifier: localBotVerifier{}, account: account, storage: storage,
			readiness: server,
		}, nil

	case "remote":
		factory, err := newRemoteTelegramFactory(cfg, logger)
		if err != nil {
			return telegramComponents{}, err
		}
		login, err := logingateway.New(factory)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("create Telegram login gateway: %w", err)
		}
		verifier, err := botgateway.NewGotdVerifier(factory)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("create bot verifier: %w", err)
		}
		provider, err := telegramstore.NewDatabaseClientProvider(pool, cipher, factory)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("create Telegram session provider: %w", err)
		}
		userRunner := telegramstore.ClientRunner{Provider: provider, Factory: factory}
		account, err := telegramstore.NewGotdAccount(userRunner)
		if err != nil {
			return telegramComponents{}, fmt.Errorf("create Telegram account gateway: %w", err)
		}
		storage := injected
		var downloadClients *telegramstore.DownloadClientPool
		if storage == nil {
			channelBotProvider, err := botgateway.NewChannelBotProvider(pool)
			if err != nil {
				return telegramComponents{}, fmt.Errorf("create channel bot provider: %w", err)
			}
			uploadRunner, err := botgateway.NewUploadAwareRunner(pool, cipher, factory, userRunner, cfg.Telegram.DownloadBots, cfg.Telegram.BotRotationBackend)
			if err != nil {
				return telegramComponents{}, fmt.Errorf("create upload-aware Telegram runner: %w", err)
			}
			options := []telegramstore.GotdStorageOption{
				telegramstore.WithBotProvider(channelBotProvider),
				telegramstore.WithDownloadReadBuffers(cfg.Telegram.DownloadReadBuffers),
				telegramstore.WithDownloadReadParallel(cfg.Telegram.DownloadReadParallel),
			}
			if cfg.Telegram.DownloadClientPool {
				downloadClients, err = telegramstore.NewDownloadClientPool(uploadRunner, telegramstore.DownloadClientPoolConfig{
					Clients:      max(1, cfg.Telegram.DownloadBots),
					ReadBuffers:  cfg.Telegram.DownloadReadBuffers,
					ReadParallel: cfg.Telegram.DownloadReadParallel,
				}, globalCache)
				if err != nil {
					return telegramComponents{}, fmt.Errorf("create Telegram download client pool: %w", err)
				}
				options = append(options, telegramstore.WithDownloadClientPool(downloadClients))
			}
			storage = telegramstore.NewGotdStorage(uploadRunner, globalCache, options...)
		}
		return telegramComponents{
			login: login, verifier: verifier, account: account, storage: storage, downloadClients: downloadClients,
		}, nil

	default:
		return telegramComponents{}, fmt.Errorf("unsupported Telegram backend %q", cfg.Telegram.Backend)
	}
}

// logRecoveryReport surfaces startup reconciliation of the local Telegram
// root: completed publishes are debug noise, but anything quarantined or
// dropped means an operator may need to inspect the root.
func logRecoveryReport(logger *slog.Logger, root string, report localtelegram.RecoveryReport) {
	if logger == nil {
		logger = slog.Default()
	}
	args := []any{
		"root", root,
		"replayed_commits", report.ReplayedCommits,
		"discarded_uploads", report.DiscardedUploads,
		"quarantined", len(report.Quarantined),
		"dangling_references", len(report.DanglingReferences),
	}
	if report.UpgradedFrom > 0 {
		args = append(args, "upgraded_from_version", report.UpgradedFrom)
	}
	if report.RestoredFromBackup {
		args = append(args, "restored_from_backup", true)
	}
	switch {
	case len(report.Quarantined) > 0 || len(report.DanglingReferences) > 0 || report.RestoredFromBackup:
		logger.Warn("local Telegram root recovered with observable inconsistencies", args...)
	case report.ReplayedCommits > 0 || report.DiscardedUploads > 0 || report.UpgradedFrom > 0:
		logger.Info("local Telegram root recovered interrupted operations", args...)
	default:
		logger.Debug("local Telegram root is consistent", args...)
	}
}
