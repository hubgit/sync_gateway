//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/pkg/errors"

	// Register profiling handlers (see Go docs)
	_ "net/http/pprof"
)

var (
	DefaultInterface              = ":4984"
	DefaultAdminInterface         = "127.0.0.1:4985" // Only accessible on localhost!
	DefaultServer                 = "walrus:"
	DefaultPool                   = "default"
	DefaultMinimumTLSVersionConst = tls.VersionTLS10

	// The value of defaultLogFilePath is populated by --defaultLogFilePath in ParseCommandLine()
	defaultLogFilePath string

	// ErrUnknownField is marked as the cause of the error when trying to decode a JSON config with unknown fields
	ErrUnknownField = errors.New("unrecognized config value")
)

var config *ServerConfig

const (
	DefaultMaxCouchbaseConnections         = 16
	DefaultMaxCouchbaseOverflowConnections = 0

	// Default value of ServerConfig.MaxIncomingConnections
	DefaultMaxIncomingConnections = 0

	// Default value of ServerConfig.MaxFileDescriptors
	DefaultMaxFileDescriptors uint64 = 5000

	// Default number of index replicas
	DefaultNumIndexReplicas = uint(1)
)

type SyncGatewayRunMode uint8

const (
	SyncGatewayRunModeNormal SyncGatewayRunMode = iota
	SyncGatewayRunModeAccel
)

// JSON object that defines the server configuration.
type ServerConfig struct {
	TLSMinVersion              *string                  `json:"tls_minimum_version,omitempty"`     // Set TLS Version
	Interface                  *string                  `json:",omitempty"`                        // Interface to bind REST API to, default ":4984"
	SSLCert                    *string                  `json:",omitempty"`                        // Path to SSL cert file, or nil
	SSLKey                     *string                  `json:",omitempty"`                        // Path to SSL private key file, or nil
	ServerReadTimeout          *int                     `json:",omitempty"`                        // maximum duration.Second before timing out read of the HTTP(S) request
	ServerWriteTimeout         *int                     `json:",omitempty"`                        // maximum duration.Second before timing out write of the HTTP(S) response
	AdminInterface             *string                  `json:",omitempty"`                        // Interface to bind admin API to, default "localhost:4985"
	AdminUI                    *string                  `json:",omitempty"`                        // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface           *string                  `json:",omitempty"`                        // Interface to bind Go profile API to (no default)
	ConfigServer               *string                  `json:",omitempty"`                        // URL of config server (for dynamic db discovery)
	Facebook                   *FacebookConfig          `json:",omitempty"`                        // Configuration for Facebook validation
	Google                     *GoogleConfig            `json:",omitempty"`                        // Configuration for Google validation
	CORS                       *CORSConfig              `json:",omitempty"`                        // Configuration for allowing CORS
	DeprecatedLog              []string                 `json:"log,omitempty"`                     // Log keywords to enable
	DeprecatedLogFilePath      *string                  `json:"logFilePath,omitempty"`             // Path to log file, if missing write to stderr
	Logging                    *base.LoggingConfig      `json:",omitempty"`                        // Configuration for logging with optional log file rotation
	Pretty                     bool                     `json:",omitempty"`                        // Pretty-print JSON responses?
	DeploymentID               *string                  `json:",omitempty"`                        // Optional customer/deployment ID for stats reporting
	StatsReportInterval        *float64                 `json:",omitempty"`                        // Optional stats report interval (0 to disable)
	MaxCouchbaseConnections    *int                     `json:",omitempty"`                        // Max # of sockets to open to a Couchbase Server node
	MaxCouchbaseOverflow       *int                     `json:",omitempty"`                        // Max # of overflow sockets to open
	CouchbaseKeepaliveInterval *int                     `json:",omitempty"`                        // TCP keep-alive interval between SG and Couchbase server
	SlowQueryWarningThreshold  *int                     `json:",omitempty"`                        // Log warnings if N1QL queries take this many ms
	MaxIncomingConnections     *int                     `json:",omitempty"`                        // Max # of incoming HTTP connections to accept
	MaxFileDescriptors         *uint64                  `json:",omitempty"`                        // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses          *bool                    `json:",omitempty"`                        // If false, disables compression of HTTP responses
	Databases                  DbConfigMap              `json:",omitempty"`                        // Pre-configured databases, mapped by name
	Replications               []*ReplicationConfig     `json:",omitempty"`                        // sg-replicate replication definitions
	MaxHeartbeat               uint64                   `json:",omitempty"`                        // Max heartbeat value for _changes request (seconds)
	ClusterConfig              *ClusterConfig           `json:"cluster_config,omitempty"`          // Bucket and other config related to CBGT
	SkipRunmodeValidation      bool                     `json:"skip_runmode_validation,omitempty"` // If this is true, skips any config validation regarding accel vs normal mode
	Unsupported                *UnsupportedServerConfig `json:"unsupported,omitempty"`             // Config for unsupported features
	RunMode                    SyncGatewayRunMode       `json:"runmode,omitempty"`                 // Whether this is an SG reader or an SG Accelerator
	ReplicatorCompression      *int                     `json:"replicator_compression,omitempty"`  // BLIP data compression level (0-9)
	BcryptCost                 int                      `json:"bcrypt_cost,omitempty"`             // bcrypt cost to use for password hashes - Default: bcrypt.DefaultCost
}

// Bucket configuration elements - used by db, shadow, index
type BucketConfig struct {
	Server     *string `json:"server,omitempty"`      // Couchbase server URL
	Pool       *string `json:"pool,omitempty"`        // Couchbase pool name, default "default"
	Bucket     *string `json:"bucket,omitempty"`      // Bucket name
	Username   string  `json:"username,omitempty"`    // Username for authenticating to server
	Password   string  `json:"password,omitempty"`    // Password for authenticating to server
	CertPath   string  `json:"certpath,omitempty"`    // Cert path (public key) for X.509 bucket auth
	KeyPath    string  `json:"keypath,omitempty"`     // Key path (private key) for X.509 bucket auth
	CACertPath string  `json:"cacertpath,omitempty"`  // Root CA cert path for X.509 bucket auth
	KvTLSPort  int     `json:"kv_tls_port,omitempty"` // Memcached TLS port, if not default (11207)
}

func (bc *BucketConfig) MakeBucketSpec() base.BucketSpec {

	server := "http://localhost:8091"
	pool := "default"
	bucketName := ""
	tlsPort := 11207

	if bc.Server != nil {
		server = *bc.Server
	}
	if bc.Pool != nil {
		pool = *bc.Pool
	}
	if bc.Bucket != nil {
		bucketName = *bc.Bucket
	}

	if bc.KvTLSPort != 0 {
		tlsPort = bc.KvTLSPort
	}

	return base.BucketSpec{
		Server:     server,
		PoolName:   pool,
		BucketName: bucketName,
		Keypath:    bc.KeyPath,
		Certpath:   bc.CertPath,
		CACertPath: bc.CACertPath,
		KvTLSPort:  tlsPort,
		Auth:       bc,
	}
}

// Implementation of AuthHandler interface for BucketConfig
func (bucketConfig *BucketConfig) GetCredentials() (username string, password string, bucketname string) {
	return base.TransformBucketCredentials(bucketConfig.Username, bucketConfig.Password, *bucketConfig.Bucket)
}

type ClusterConfig struct {
	BucketConfig
	DataDir                  string  `json:"data_dir,omitempty"`
	HeartbeatIntervalSeconds *uint16 `json:"heartbeat_interval_seconds,omitempty"`
}

func (c ClusterConfig) CBGTEnabled() bool {
	// if we have a non-empty server field, then assume CBGT is enabled.
	return c.Server != nil && *c.Server != ""
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	BucketConfig
	Name                      string                         `json:"name,omitempty"`                         // Database name in REST API (stored as key in JSON)
	Sync                      *string                        `json:"sync,omitempty"`                         // Sync function defines which users can see which data
	Users                     map[string]*db.PrincipalConfig `json:"users,omitempty"`                        // Initial user accounts
	Roles                     map[string]*db.PrincipalConfig `json:"roles,omitempty"`                        // Initial roles
	RevsLimit                 *uint32                        `json:"revs_limit,omitempty"`                   // Max depth a document's revision tree can grow to
	AutoImport                interface{}                    `json:"import_docs,omitempty"`                  // Whether to automatically import Couchbase Server docs into SG.  Xattrs must be enabled.  true or "continuous" both enable this.
	ImportFilter              *string                        `json:"import_filter,omitempty"`                // Filter function (import)
	ImportBackupOldRev        bool                           `json:"import_backup_old_rev"`                  // Whether import should attempt to create a temporary backup of the previous revision body, when available.
	Shadow                    *ShadowConfig                  `json:"shadow,omitempty"`                       // This is where the ShadowConfig used to be.  If found, it should throw an error
	EventHandlers             interface{}                    `json:"event_handlers,omitempty"`               // Event handlers (webhook)
	FeedType                  string                         `json:"feed_type,omitempty"`                    // Feed type - "DCP" or "TAP"; defaults based on Couchbase server version
	AllowEmptyPassword        bool                           `json:"allow_empty_password,omitempty"`         // Allow empty passwords?  Defaults to false
	CacheConfig               *CacheConfig                   `json:"cache,omitempty"`                        // Cache settings
	ChannelIndex              *ChannelIndexConfig            `json:"channel_index,omitempty"`                // Channel index settings
	DeprecatedRevCacheSize    *uint32                        `json:"rev_cache_size,omitempty"`               // Maximum number of revisions to store in the revision cache (deprecated, CBG-356)
	StartOffline              bool                           `json:"offline,omitempty"`                      // start the DB in the offline state, defaults to false
	Unsupported               db.UnsupportedOptions          `json:"unsupported,omitempty"`                  // Config for unsupported features
	Deprecated                DeprecatedOptions              `json:"deprecated,omitempty"`                   // Config for Deprecated features
	OIDCConfig                *auth.OIDCOptions              `json:"oidc,omitempty"`                         // Config properties for OpenID Connect authentication
	OldRevExpirySeconds       *uint32                        `json:"old_rev_expiry_seconds,omitempty"`       // The number of seconds before old revs are removed from CBS bucket
	ViewQueryTimeoutSecs      *uint32                        `json:"view_query_timeout_secs,omitempty"`      // The view query timeout in seconds
	LocalDocExpirySecs        *uint32                        `json:"local_doc_expiry_secs,omitempty"`        // The _local doc expiry time in seconds
	EnableXattrs              *bool                          `json:"enable_shared_bucket_access,omitempty"`  // Whether to use extended attributes to store _sync metadata
	SessionCookieName         string                         `json:"session_cookie_name"`                    // Custom per-database session cookie name
	AllowConflicts            *bool                          `json:"allow_conflicts,omitempty"`              // False forbids creating conflicts
	NumIndexReplicas          *uint                          `json:"num_index_replicas"`                     // Number of GSI index replicas used for core indexes
	UseViews                  bool                           `json:"use_views"`                              // Force use of views instead of GSI
	SendWWWAuthenticateHeader *bool                          `json:"send_www_authenticate_header,omitempty"` // If false, disables setting of 'WWW-Authenticate' header in 401 responses
	BucketOpTimeoutMs         *uint32                        `json:"bucket_op_timeout_ms,omitempty"`         // How long bucket ops should block returning "operation timed out". If nil, uses GoCB default.  GoCB buckets only.
	DeltaSync                 *DeltaSyncConfig               `json:"delta_sync,omitempty"`                   // Config for delta sync
	CompactIntervalDays       *float32                       `json:"compact_interval_days,omitempty"`        //Interval in days between compaction is automatically ran - 0 means don't run
}

type DeltaSyncConfig struct {
	Enabled          *bool   `json:"enabled,omitempty"`             // Whether delta sync is enabled (requires EE)
	RevMaxAgeSeconds *uint32 `json:"rev_max_age_seconds,omitempty"` // The number of seconds deltas for old revs are available for
}

type DeprecatedOptions struct {
	Shadow *ShadowConfig `json:"shadow,omitempty"` // External bucket to shadow
}

type DbConfigMap map[string]*DbConfig

type ReplConfigMap map[string]*ReplicationConfig

type FacebookConfig struct {
	Register bool // If true, server will register new user accounts
}

type GoogleConfig struct {
	Register    bool     // If true, server will register new user accounts
	AppClientID []string `json:"app_client_id"` // list of enabled client ids
}

type CORSConfig struct {
	Origin      []string // List of allowed origins, use ["*"] to allow access from everywhere
	LoginOrigin []string // List of allowed login origins
	Headers     []string // List of allowed headers
	MaxAge      int      // Maximum age of the CORS Options request
}

type ShadowConfig struct {
	BucketConfig
	Doc_id_regex *string `json:"doc_id_regex,omitempty"` // Optional regex that doc IDs must match
	FeedType     string  `json:"feed_type,omitempty"`    // Feed type - "DCP" or "TAP"; defaults to TAP
}

type EventHandlerConfig struct {
	MaxEventProc    uint           `json:"max_processes,omitempty"`    // Max concurrent event handling goroutines
	WaitForProcess  string         `json:"wait_for_process,omitempty"` // Max wait time when event queue is full (ms)
	DocumentChanged []*EventConfig `json:"document_changed,omitempty"` // Document Commit
	DBStateChanged  []*EventConfig `json:"db_state_changed,omitempty"` // DB state change
}

type EventConfig struct {
	HandlerType string  `json:"handler"`           // Handler type
	Url         string  `json:"url,omitempty"`     // Url (webhook)
	Filter      string  `json:"filter,omitempty"`  // Filter function (webhook)
	Timeout     *uint64 `json:"timeout,omitempty"` // Timeout (webhook)
}

type CacheConfig struct {
	RevCacheConfig     *RevCacheConfig     `json:"rev_cache"`     // Revision Cache Config Settings
	ChannelCacheConfig *ChannelCacheConfig `json:"channel_cache"` // Channel Cache Config Settings
	DeprecatedCacheConfig
}

// ***************************************************************
//	Kept around for CBG-356 backwards compatability
// ***************************************************************
type DeprecatedCacheConfig struct {
	DeprecatedCachePendingSeqMaxWait *uint32 `json:"max_wait_pending,omitempty"`         // Max wait for pending sequence before skipping
	DeprecatedCachePendingSeqMaxNum  *int    `json:"max_num_pending,omitempty"`          // Max number of pending sequences before skipping
	DeprecatedCacheSkippedSeqMaxWait *uint32 `json:"max_wait_skipped,omitempty"`         // Max wait for skipped sequence before abandoning
	DeprecatedEnableStarChannel      *bool   `json:"enable_star_channel,omitempty"`      // Enable star channel
	DeprecatedChannelCacheMaxLength  *int    `json:"channel_cache_max_length,omitempty"` // Maximum number of entries maintained in cache per channel
	DeprecatedChannelCacheMinLength  *int    `json:"channel_cache_min_length,omitempty"` // Minimum number of entries maintained in cache per channel
	DeprecatedChannelCacheAge        *int    `json:"channel_cache_expiry,omitempty"`     // Time (seconds) to keep entries in cache beyond the minimum retained
}

type RevCacheConfig struct {
	Size       *uint32 `json:"size,omitempty"`        // Maximum number of revisions to store in the revision cache
	ShardCount *uint16 `json:"shard_count,omitempty"` // Number of shards the rev cache should be split into
}

type ChannelCacheConfig struct {
	MaxNumber            *int    `json:"max_number,omitempty"`                 // Maximum number of channel caches which will exist at any one point
	HighWatermarkPercent *int    `json:"compact_high_watermark_pct,omitempty"` // High watermark for channel cache eviction (percent)
	LowWatermarkPercent  *int    `json:"compact_low_watermark_pct,omitempty"`  // High watermark for channel cache eviction (percent)
	MaxWaitPending       *uint32 `json:"max_wait_pending,omitempty"`           // Max wait for pending sequence before skipping
	MaxNumPending        *int    `json:"max_num_pending,omitempty"`            // Max number of pending sequences before skipping
	MaxWaitSkipped       *uint32 `json:"max_wait_skipped,omitempty"`           // Max wait for skipped sequence before abandoning
	EnableStarChannel    *bool   `json:"enable_star_channel,omitempty"`        // Enable star channel
	MaxLength            *int    `json:"max_length,omitempty"`                 // Maximum number of entries maintained in cache per channel
	MinLength            *int    `json:"min_length,omitempty"`                 // Minimum number of entries maintained in cache per channel
	ExpirySeconds        *int    `json:"expiry_seconds,omitempty"`             // Time (seconds) to keep entries in cache beyond the minimum retained
}

type ChannelIndexConfig struct {
	BucketConfig
	IndexWriter               bool                `json:"writer,omitempty"`       // Whether SG node is a channel index writer
	NumShards                 uint16              `json:"num_shards,omitempty"`   // Number of partitions in the channel index
	SequenceHashConfig        *SequenceHashConfig `json:"seq_hashing,omitempty"`  // Sequence hash configuration
	TombstoneCompactFrequency *int                `json:"tombstone_compact_freq"` // How often sg-accel attempts to compact purged tombstones
}

type SequenceHashConfig struct {
	BucketConfig         // Bucket used for Sequence hashing
	Expiry       *uint32 `json:"expiry,omitempty"`         // Expiry set for hash values on latest use
	Frequency    *int    `json:"hash_frequency,omitempty"` // Frequency of sequence hashing in changes feeds
}

type UnsupportedServerConfig struct {
	Http2Config           *Http2Config `json:"http2,omitempty"`               // Config settings for HTTP2
	StatsLogFrequencySecs int          `json:"stats_log_freq_secs,omitempty"` // How often should stats be written to stats logs
}

type Http2Config struct {
	Enabled *bool `json:"enabled,omitempty"` // Whether HTTP2 support is enabled
}

//TODO: Add support for TLS 1.3 when we switch to Go 1.13

func GetTLSVersionFromString(stringV *string) uint16 {
	if stringV != nil {
		switch *stringV {
		case "tlsv1":
			return tls.VersionTLS10
		case "tlsv1.1":
			return tls.VersionTLS11
		case "tlsv1.2":
			return tls.VersionTLS12
		}
	}
	return uint16(DefaultMinimumTLSVersionConst)
}

func (dbConfig *DbConfig) setup(name string) error {

	dbConfig.Name = name
	if dbConfig.Bucket == nil {
		dbConfig.Bucket = &dbConfig.Name
	}
	if dbConfig.Server == nil {
		dbConfig.Server = &DefaultServer
	}
	if dbConfig.Pool == nil {
		dbConfig.Pool = &DefaultPool
	}

	url, err := url.Parse(*dbConfig.Server)
	if err == nil && url.User != nil {
		// Remove credentials from URL and put them into the DbConfig.Username and .Password:
		if dbConfig.Username == "" {
			dbConfig.Username = url.User.Username()
		}
		if dbConfig.Password == "" {
			if password, exists := url.User.Password(); exists {
				dbConfig.Password = password
			}
		}
		url.User = nil
		urlStr := url.String()
		dbConfig.Server = &urlStr
	}

	if dbConfig.Shadow != nil {
		return fmt.Errorf("Bucket shadowing configuration has been moved to the 'deprecated' section of the config.  Please update your config and retry")
	}

	if dbConfig.Deprecated.Shadow != nil {
		url, err = url.Parse(*dbConfig.Deprecated.Shadow.Server)
		if err == nil && url.User != nil {
			// Remove credentials from shadow URL and put them into the dbConfig.Deprecated.Shadow.Username and .Password:
			if dbConfig.Deprecated.Shadow.Username == "" {
				dbConfig.Deprecated.Shadow.Username = url.User.Username()
			}
			if dbConfig.Deprecated.Shadow.Password == "" {
				if password, exists := url.User.Password(); exists {
					dbConfig.Deprecated.Shadow.Password = password
				}
			}
			url.User = nil
			urlStr := url.String()
			dbConfig.Deprecated.Shadow.Server = &urlStr
		}
	}

	if dbConfig.ChannelIndex != nil {
		url, err = url.Parse(*dbConfig.ChannelIndex.Server)
		if err == nil && url.User != nil {
			// Remove credentials from shadow URL and put them into the DbConfig.ChannelIndex.Username and .Password:
			if dbConfig.ChannelIndex.Username == "" {
				dbConfig.ChannelIndex.Username = url.User.Username()
			}
			if dbConfig.ChannelIndex.Password == "" {
				if password, exists := url.User.Password(); exists {
					dbConfig.ChannelIndex.Password = password
				}
			}
			url.User = nil
			urlStr := url.String()
			dbConfig.ChannelIndex.Server = &urlStr
		}
	}

	return err
}

func (dbConfig *DbConfig) AutoImportEnabled() (bool, error) {
	if dbConfig.AutoImport == nil {
		return base.DefaultAutoImport, nil
	}

	if b, ok := dbConfig.AutoImport.(bool); ok {
		return b, nil
	}

	str, ok := dbConfig.AutoImport.(string)
	if ok && str == "continuous" {
		base.Warnf(base.KeyAll, `Using deprecated config value for "import_docs": "continuous". Use "import_docs": true instead.`)
		return true, nil
	}

	return false, fmt.Errorf("Unrecognized value for import_docs: %#v. Valid values are true and false.", dbConfig.AutoImport)
}

func (dbConfig DbConfig) validate() []error {

	errorMessages := make([]error, 0)

	// if there is a ChannelIndex being used, then the only valid feed type is DCPSHARD
	if dbConfig.ChannelIndex != nil {
		if strings.ToLower(dbConfig.FeedType) != strings.ToLower(base.DcpShardFeedType) {
			msg := "ChannelIndex declared in config, but the FeedType is %v " +
				"rather than expected value of DCPSHARD"
			errorMessages = append(errorMessages, fmt.Errorf(msg, dbConfig.FeedType))
		}
	}

	if dbConfig.CompactIntervalDays != nil {
		if *dbConfig.CompactIntervalDays < db.CompactIntervalMinDays && *dbConfig.CompactIntervalDays != 0 {
			errorMessages = append(errorMessages, fmt.Errorf("compact_interval_days cannot be lower than %g", db.CompactIntervalMinDays))
		}
		if *dbConfig.CompactIntervalDays > db.CompactIntervalMinDays && *dbConfig.CompactIntervalDays != 0 {
			errorMessages = append(errorMessages, fmt.Errorf("compact_interval_days cannot be higher than %g", db.CompactIntervalMaxDays))
		}
	}

	if dbConfig.CacheConfig != nil {

		if dbConfig.CacheConfig.ChannelCacheConfig != nil {
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.max_num_pending is 1"))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.max_wait_pending is 1"))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.max_wait_skipped is 1"))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxLength != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxLength < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.max_length is 1"))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MinLength != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MinLength < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.min_length is 1"))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds != nil && *dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.expiry_seconds is 1"))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber < 1 {
				errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.channel_cache.max_number is 1"))
			}

			// Compact watermark validation
			hwm := db.DefaultCompactHighWatermarkPercent
			lwm := db.DefaultCompactLowWatermarkPercent
			if dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent != nil {
				if *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent < 1 || *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent > 100 {
					errorMessages = append(errorMessages, fmt.Errorf("valid range for cache.channel_cache.compact_high_watermark_pct is 0-100"))
				}
				hwm = *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent != nil {
				if *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent < 1 || *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent > 100 {
					errorMessages = append(errorMessages, fmt.Errorf("valid range for cache.channel_cache.compact_low_watermark_pct is 0-100"))
				}
				lwm = *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent
			}
			if lwm >= hwm {
				errorMessages = append(errorMessages, fmt.Errorf("cache.channel_cache.compact_high_watermark_pct (%v) must be greater than cache.channel_cache.compact_low_watermark_pct (%v)", hwm, lwm))
			}

		}

		if dbConfig.CacheConfig.RevCacheConfig != nil {
			if dbConfig.CacheConfig.RevCacheConfig.ShardCount != nil {
				if *dbConfig.CacheConfig.RevCacheConfig.ShardCount < 1 {
					errorMessages = append(errorMessages, fmt.Errorf("minimum value for cache.rev_cache.shard_count is 1"))

				}
			}
		}
	}

	// if the feed type is DCPSHARD, then there must be a ChannelIndex
	if strings.ToLower(dbConfig.FeedType) == strings.ToLower(base.DcpShardFeedType) {
		if dbConfig.ChannelIndex == nil {
			msg := "FeedType is DCPSHARD, but no ChannelIndex declared in config"
			errorMessages = append(errorMessages, fmt.Errorf(msg))
		}
	}

	// Error if Delta Sync is explicitly enabled in CE
	if dbConfig.DeltaSync != nil && dbConfig.DeltaSync.Enabled != nil {
		if *dbConfig.DeltaSync.Enabled && !base.IsEnterpriseEdition() {
			errorMessages = append(errorMessages, fmt.Errorf("Delta sync is not supported in CE"))

		}
	}

	return errorMessages

}

func (dbConfig *DbConfig) validateSgDbConfig() []error {

	errorMessages := make([]error, 0)

	if err := dbConfig.validate(); err != nil {
		errorMessages = append(errorMessages, err...)
	}

	if dbConfig.ChannelIndex != nil && dbConfig.ChannelIndex.IndexWriter == true {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration for Sync Gw.  Must not be configured as an IndexWriter"))
	}

	// Don't allow Distributed Index and Bucket Shadowing to co-exist
	if err := dbConfig.verifyNoDistributedIndexAndBucketShadowing(); err != nil {
		errorMessages = append(errorMessages, err)
	}

	autoImportEnabled, err := dbConfig.AutoImportEnabled()
	if err != nil {
		errorMessages = append(errorMessages, err)

	}

	if dbConfig.FeedType == base.TapFeedType && autoImportEnabled == true {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration for Sync Gw. TAP feed type can not be used with auto-import"))
	}

	return errorMessages

}

// Checks for deprecated cache config options and if they are set it will return a warning. If the old one is set and
// the new one is not set it will set the new to the old value. If they are both set it will still give the warning but
// will choose the new value.
func (dbConfig *DbConfig) deprecatedConfigCacheFallback() (warnings []string) {

	warningMsgFmt := "Using deprecated config option: %q. Use %q instead."

	if dbConfig.CacheConfig == nil {
		dbConfig.CacheConfig = &CacheConfig{}
	}

	if dbConfig.CacheConfig.RevCacheConfig == nil {
		dbConfig.CacheConfig.RevCacheConfig = &RevCacheConfig{}
	}

	if dbConfig.CacheConfig.ChannelCacheConfig == nil {
		dbConfig.CacheConfig.ChannelCacheConfig = &ChannelCacheConfig{}
	}

	if dbConfig.DeprecatedRevCacheSize != nil {
		if dbConfig.CacheConfig.RevCacheConfig.Size == nil {
			dbConfig.CacheConfig.RevCacheConfig.Size = dbConfig.DeprecatedRevCacheSize
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "rev_cache_size", "cache.rev_cache.size"))
	}

	if dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxWait != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending = dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxWait
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "max_wait_pending", "cache.channel_cache.max_wait_pending"))
	}

	if dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxNum != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending = dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxNum
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "max_num_pending", "cache.channel_cache.max_num_pending"))
	}

	if dbConfig.CacheConfig.DeprecatedCacheSkippedSeqMaxWait != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped = dbConfig.CacheConfig.DeprecatedCacheSkippedSeqMaxWait
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "max_wait_skipped", "cache.channel_cache.max_wait_skipped"))
	}

	if dbConfig.CacheConfig.DeprecatedEnableStarChannel != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel = dbConfig.CacheConfig.DeprecatedEnableStarChannel
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "enable_star_channel", "cache.channel_cache.enable_star_channel"))
	}

	if dbConfig.CacheConfig.DeprecatedChannelCacheMaxLength != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxLength == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxLength = dbConfig.CacheConfig.DeprecatedChannelCacheMaxLength
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "channel_cache_max_length", "cache.channel_cache.max_length"))
	}

	if dbConfig.CacheConfig.DeprecatedChannelCacheMinLength != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MinLength == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MinLength = dbConfig.CacheConfig.DeprecatedChannelCacheMinLength
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "channel_cache_min_length", "cache.channel_cache.min_length"))
	}

	if dbConfig.CacheConfig.DeprecatedChannelCacheAge != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds = dbConfig.CacheConfig.DeprecatedChannelCacheAge
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "channel_cache_expiry", "cache.channel_cache.expiry"))
	}

	return warnings

}

func (dbConfig *DbConfig) validateSgAccelDbConfig() []error {

	errorMessages := make([]error, 0)

	if err := dbConfig.validate(); err != nil {
		errorMessages = append(errorMessages, err...)
	}

	if dbConfig.ChannelIndex == nil {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration for Sync Gw Accel.  Must have a ChannelIndex defined"))
	}

	if dbConfig.ChannelIndex.IndexWriter == false {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration for Sync Gw Accel.  Must be configured as an IndexWriter"))
	}

	if strings.ToLower(dbConfig.FeedType) != strings.ToLower(base.DcpShardFeedType) {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration for Sync Gw Accel.  Must be configured for DCPSHARD feedtype"))
	}

	// Don't allow Distributed Index and Bucket Shadowing to co-exist
	if err := dbConfig.verifyNoDistributedIndexAndBucketShadowing(); err != nil {
		errorMessages = append(errorMessages, err)
	}

	return errorMessages

}

func (dbConfig *DbConfig) verifyNoDistributedIndexAndBucketShadowing() error {
	// Don't allow Distributed Index and Bucket Shadowing to co-exist
	if dbConfig.ChannelIndex != nil && dbConfig.Deprecated.Shadow != nil {
		return fmt.Errorf("Using Sync Gateway Accel with Bucket Shadowing is not supported")
	}
	return nil
}

func (dbConfig *DbConfig) modifyConfig() {
	if dbConfig.ChannelIndex != nil {
		// if there is NO feed type, set to DCPSHARD, since that's the only
		// valid config when a Channel Index is specified
		if dbConfig.FeedType == "" {
			dbConfig.FeedType = base.DcpShardFeedType
		}
	}
}

// Implementation of AuthHandler interface for DbConfig
func (dbConfig *DbConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(dbConfig.Username, dbConfig.Password, *dbConfig.Bucket)
}

func (dbConfig *DbConfig) ConflictsAllowed() *bool {
	if dbConfig.AllowConflicts != nil {
		return dbConfig.AllowConflicts
	}
	return base.BoolPtr(base.DefaultAllowConflicts)
}

func (dbConfig *DbConfig) UseXattrs() bool {
	if dbConfig.EnableXattrs != nil {
		return *dbConfig.EnableXattrs
	}
	return base.DefaultUseXattrs
}

// Create a deepcopy of this DbConfig, or panic.
// This will only copy all of the _exported_ fields of the DbConfig.
func (dbConfig *DbConfig) DeepCopy() (dbConfigCopy *DbConfig, err error) {

	dbConfigDeepCopy := &DbConfig{}
	err = base.DeepCopyInefficient(&dbConfigDeepCopy, dbConfig)
	if err != nil {
		return nil, err
	}
	return dbConfigDeepCopy, nil

}

// Implementation of AuthHandler interface for ClusterConfig
func (clusterConfig *ClusterConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(clusterConfig.Username, clusterConfig.Password, *clusterConfig.Bucket)
}

// LoadServerConfig loads a ServerConfig from either a JSON file or from a URL
func LoadServerConfig(runMode SyncGatewayRunMode, path string) (config *ServerConfig, err error) {
	var dataReadCloser io.ReadCloser

	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		resp, err := http.Get(path)
		if err != nil {
			return nil, err
		} else if resp.StatusCode >= 300 {
			return nil, base.HTTPErrorf(resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		dataReadCloser = resp.Body
	} else {
		dataReadCloser, err = os.Open(path)
		if err != nil {
			return nil, err
		}
	}

	defer dataReadCloser.Close()
	return readServerConfig(runMode, dataReadCloser)
}

// readServerConfig returns a validated ServerConfig from an io.Reader
func readServerConfig(runMode SyncGatewayRunMode, r io.Reader) (config *ServerConfig, err error) {
	err = decodeAndSanitiseConfig(r, &config)
	if config != nil {
		config.RunMode = runMode
	}
	return config, err
}

// decodeAndSanitiseConfig will sanitise a ServerConfig or dbConfig from an io.Reader and unmarshal it into the given config parameter.
func decodeAndSanitiseConfig(r io.Reader, config interface{}) (err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	b = base.ConvertBackQuotedStrings(b)

	d := json.NewDecoder(bytes.NewBuffer(b))
	d.DisallowUnknownFields()
	err = d.Decode(config)
	if err != nil && strings.HasPrefix(err.Error(), "json: unknown field") {
		// Special handling for unknown field errors
		// json.Decode continues to decode the full data into the struct
		// so it's safe to use even after this error
		return errors.WithMessage(ErrUnknownField, err.Error())
	}
	return err
}

func (config *ServerConfig) setupAndValidateDatabases() []error {
	if config == nil {
		return nil
	}

	for name, dbConfig := range config.Databases {

		if err := dbConfig.setup(name); err != nil {
			return []error{err}
		}

		if err := config.validateDbConfig(dbConfig); err != nil && len(err) > 0 {
			return err
		}
	}
	return nil
}

// setupAndValidateLogging sets up and validates logging,
// and returns a slice of defferred logs to execute later.
func (config *ServerConfig) SetupAndValidateLogging() (warnings []base.DeferredLogFn, err error) {

	if config.Logging == nil {
		config.Logging = &base.LoggingConfig{}
	}

	// populate values from deprecated logging config options if not set
	warnings = config.deprecatedConfigLoggingFallback()

	base.SetRedaction(config.Logging.RedactionLevel)

	warningsInit, err := config.Logging.Init(defaultLogFilePath)
	warnings = append(warnings, warningsInit...)
	if err != nil {
		return warnings, err
	}

	if config.Logging.DeprecatedDefaultLog == nil {
		config.Logging.DeprecatedDefaultLog = &base.LogAppenderConfig{}
	}

	return warnings, nil
}

// deprecatedConfigLoggingFallback will parse the ServerConfig and try to
// use older logging config options for backwards compatibility.
// It will return a slice of deferred warnings to log at a later time.
func (config *ServerConfig) deprecatedConfigLoggingFallback() (warnings []base.DeferredLogFn) {

	warningMsgFmt := "Using deprecated config option: %q. Use %q instead."

	if config.Logging.DeprecatedDefaultLog != nil {
		// Fall back to the old logging.["default"].LogFilePath option
		if config.Logging.LogFilePath == "" && config.Logging.DeprecatedDefaultLog.LogFilePath != nil {
			warnings = append(warnings, func() {
				base.Warnf(base.KeyAll, warningMsgFmt, `logging.["default"].LogFilePath`, "logging.log_file_path")
			})
			// Set the new LogFilePath to be the directory containing the old logfile, instead of the full path.
			config.Logging.LogFilePath = filepath.Dir(*config.Logging.DeprecatedDefaultLog.LogFilePath)
		}

		// Fall back to the old logging.["default"].LogKeys option
		if len(config.Logging.Console.LogKeys) == 0 && len(config.Logging.DeprecatedDefaultLog.LogKeys) > 0 {
			warnings = append(warnings, func() {
				base.Warnf(base.KeyAll, warningMsgFmt, `logging.["default"].LogKeys`, "logging.console.log_keys")
			})
			config.Logging.Console.LogKeys = config.Logging.DeprecatedDefaultLog.LogKeys
		}

		// Fall back to the old logging.["default"].LogLevel option
		if config.Logging.Console.LogLevel == nil && config.Logging.DeprecatedDefaultLog.LogLevel != 0 {
			warnings = append(warnings, func() {
				base.Warnf(base.KeyAll, warningMsgFmt, `logging.["default"].LogLevel`, "logging.console.log_level")
			})
			config.Logging.Console.LogLevel = base.ToLogLevel(config.Logging.DeprecatedDefaultLog.LogLevel)
		}
	}

	// Fall back to the old LogFilePath option
	if config.Logging.LogFilePath == "" && config.DeprecatedLogFilePath != nil {
		warnings = append(warnings, func() {
			base.Warnf(base.KeyAll, warningMsgFmt, "logFilePath", "logging.log_file_path")
		})
		config.Logging.LogFilePath = *config.DeprecatedLogFilePath
	}

	// Fall back to the old Log option
	if config.Logging.Console.LogKeys == nil && len(config.DeprecatedLog) > 0 {
		warnings = append(warnings, func() {
			base.Warnf(base.KeyAll, warningMsgFmt, "log", "logging.console.log_keys")
		})
		config.Logging.Console.LogKeys = config.DeprecatedLog
	}

	return warnings
}

func (config *ServerConfig) validateDbConfig(dbConfig *DbConfig) []error {

	dbConfig.modifyConfig()

	switch config.RunMode {
	case SyncGatewayRunModeNormal:
		if !config.SkipRunmodeValidation && config.HasAnyIndexWriterConfiguredDatabases() {
			base.Panicf(base.KeyAll, "SG is running in normal mode but there are databases configured as index writers")
		}
		return dbConfig.validateSgDbConfig()
	case SyncGatewayRunModeAccel:
		if !config.SkipRunmodeValidation && config.HasAnyIndexReaderConfiguredDatabases() {
			base.Panicf(base.KeyAll, "SG is running in sg-accelerator mode but there are databases configured as index readers")
		}
		return dbConfig.validateSgAccelDbConfig()
	}

	return []error{fmt.Errorf("Unexpected RunMode: %v", config.RunMode)}

}

func (self *ServerConfig) MergeWith(other *ServerConfig) error {
	if self.Interface == nil {
		self.Interface = other.Interface
	}
	if self.AdminInterface == nil {
		self.AdminInterface = other.AdminInterface
	}
	if self.ProfileInterface == nil {
		self.ProfileInterface = other.ProfileInterface
	}
	if self.ConfigServer == nil {
		self.ConfigServer = other.ConfigServer
	}
	if self.DeploymentID == nil {
		self.DeploymentID = other.DeploymentID
	}
	if self.Facebook == nil {
		self.Facebook = other.Facebook
	}
	if self.CORS == nil {
		self.CORS = other.CORS
	}
	for _, flag := range other.DeprecatedLog {
		self.DeprecatedLog = append(self.DeprecatedLog, flag)
	}
	if other.Pretty {
		self.Pretty = true
	}
	for name, db := range other.Databases {
		if self.Databases[name] != nil {
			return base.RedactErrorf("Database %q already specified earlier", base.UD(name))
		}
		self.Databases[name] = db
	}
	return nil
}

// Reads the command line flags and the optional config file.
func ParseCommandLine(runMode SyncGatewayRunMode) (err error) {
	addr := flag.String("interface", DefaultInterface, "Address to bind to")
	authAddr := flag.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profAddr := flag.String("profileInterface", "", "Address to bind profile interface to")
	configServer := flag.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flag.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flag.String("url", DefaultServer, "Address of Couchbase server")
	poolName := flag.String("pool", DefaultPool, "Name of pool")
	bucketName := flag.String("bucket", "sync_gateway", "Name of bucket")
	dbName := flag.String("dbname", "", "Name of Couchbase Server database (defaults to name of bucket)")
	pretty := flag.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := flag.Bool("verbose", false, "Log more info about requests")
	logKeys := flag.String("log", "", "Log keys, comma separated")
	logFilePath := flag.String("logFilePath", "", "Path to log files")
	skipRunModeValidation := flag.Bool("skipRunModeValidation", false, "Skip config validation for runmode (accel vs normal sg)")
	certpath := flag.String("certpath", "", "Client certificate path")
	cacertpath := flag.String("cacertpath", "", "Root CA certificate path")
	keypath := flag.String("keypath", "", "Client certificate key path")

	// used by service scripts as a way to specify a per-distro defaultLogFilePath
	defaultLogFilePathFlag := flag.String("defaultLogFilePath", "", "Path to log files, if not overridden by --logFilePath, or the config")

	flag.Parse()

	if flag.NArg() > 0 {
		// Read the configuration file(s), if any:
		for i := 0; i < flag.NArg(); i++ {
			filename := flag.Arg(i)

			newConfig, newConfigErr := LoadServerConfig(runMode, filename)
			if errors.Cause(newConfigErr) == ErrUnknownField {
				// Delay returning this error so we can continue with other setup
				err = errors.WithMessage(newConfigErr, fmt.Sprintf("Error reading config file %s", base.UD(filename)))
			} else if newConfigErr != nil {
				return errors.WithMessage(newConfigErr, fmt.Sprintf("Error reading config file %s", base.UD(filename)))
			}

			if config == nil {
				config = newConfig
			} else {
				if err := config.MergeWith(newConfig); err != nil {
					return errors.WithMessage(err, fmt.Sprintf("Error reading config file %s", base.UD(filename)))
				}
			}
		}

		// Override the config file with global settings from command line flags:
		if *addr != DefaultInterface {
			config.Interface = addr
		}
		if *authAddr != DefaultAdminInterface {
			config.AdminInterface = authAddr
		}
		if *profAddr != "" {
			config.ProfileInterface = profAddr
		}
		if *configServer != "" {
			config.ConfigServer = configServer
		}
		if *deploymentID != "" {
			config.DeploymentID = deploymentID
		}
		if *pretty {
			config.Pretty = *pretty
		}

		// If the interfaces were not specified in either the config file or
		// on the command line, set them to the default values
		if config.Interface == nil {
			config.Interface = &DefaultInterface
		}
		if config.AdminInterface == nil {
			config.AdminInterface = &DefaultAdminInterface
		}

		if *logFilePath != "" {
			config.Logging.LogFilePath = *logFilePath
		}

		if *logKeys != "" {
			config.Logging.Console.LogKeys = strings.Split(*logKeys, ",")
		}

		if *skipRunModeValidation == true {
			config.SkipRunmodeValidation = *skipRunModeValidation
		}

		if defaultLogFilePathFlag != nil {
			defaultLogFilePath = *defaultLogFilePathFlag
		}

		// Log HTTP Responses if verbose is enabled.
		if verbose != nil && *verbose {
			config.Logging.Console.LogKeys = append(config.Logging.Console.LogKeys, "HTTP+")
		}

	} else {
		// If no config file is given, create a default config, filled in from command line flags:
		if *dbName == "" {
			*dbName = *bucketName
		}

		// At this point the addr is either:
		//   - A value provided by the user, in which case we want to leave it as is
		//   - The default value (":4984"), which is actually _not_ the default value we
		//     want for this case, since we are enabling insecure mode.  We want "localhost:4984" instead.
		// See #708 for more details
		if *addr == DefaultInterface {
			*addr = "localhost:4984"
		}

		config = &ServerConfig{
			Interface:        addr,
			AdminInterface:   authAddr,
			ProfileInterface: profAddr,
			Pretty:           *pretty,
			Databases: map[string]*DbConfig{
				*dbName: {
					Name: *dbName,
					BucketConfig: BucketConfig{
						Server:     couchbaseURL,
						Bucket:     bucketName,
						Pool:       poolName,
						CertPath:   *certpath,
						CACertPath: *cacertpath,
						KeyPath:    *keypath,
					},
					Users: map[string]*db.PrincipalConfig{
						base.GuestUsername: {
							Disabled:         false,
							ExplicitChannels: base.SetFromArray([]string{"*"}),
						},
					},
				},
			},
		}
	}

	return err
}

func SetMaxFileDescriptors(maxP *uint64) {
	maxFDs := DefaultMaxFileDescriptors
	if maxP != nil {
		maxFDs = *maxP
	}
	_, err := base.SetMaxFileDescriptors(maxFDs)
	if err != nil {
		base.Warnf(base.KeyAll, "Error setting MaxFileDescriptors to %d: %v", maxFDs, err)
	}
}

func (config *ServerConfig) Serve(addr string, handler http.Handler) {
	maxConns := DefaultMaxIncomingConnections
	if config.MaxIncomingConnections != nil {
		maxConns = *config.MaxIncomingConnections
	}

	http2Enabled := false
	if config.Unsupported != nil && config.Unsupported.Http2Config != nil {
		http2Enabled = *config.Unsupported.Http2Config.Enabled
	}

	tlsMinVersion := GetTLSVersionFromString(config.TLSMinVersion)

	err := base.ListenAndServeHTTP(
		addr,
		maxConns,
		config.SSLCert,
		config.SSLKey,
		handler,
		config.ServerReadTimeout,
		config.ServerWriteTimeout,
		http2Enabled,
		tlsMinVersion,
	)
	if err != nil {
		base.Fatalf(base.KeyAll, "Failed to start HTTP server on %s: %v", base.UD(addr), err)
	}
}

func (config *ServerConfig) HasAnyIndexReaderConfiguredDatabases() bool {
	numIndexReaders := config.NumIndexReaders()
	return numIndexReaders > 0
}

func (config *ServerConfig) HasAnyIndexWriterConfiguredDatabases() bool {
	numIndexWriters := config.NumIndexWriters()
	return numIndexWriters > 0
}

func (config *ServerConfig) NumIndexReaders() int {
	n := 0
	for _, dbConfig := range config.Databases {
		if dbConfig.ChannelIndex == nil || dbConfig.ChannelIndex.IndexWriter == false {
			n += 1
		}
	}
	return n
}

func (config *ServerConfig) NumIndexWriters() int {
	n := 0
	for _, dbConfig := range config.Databases {
		if dbConfig.ChannelIndex != nil && dbConfig.ChannelIndex.IndexWriter == true {
			n += 1
		}
	}
	return n
}

// Starts and runs the server given its configuration. (This function never returns.)
//
// Note: Changes in here probably need to be made in the corresponding sg-accel ServerMain!
func RunServer(config *ServerConfig) {
	PrettyPrint = config.Pretty

	base.Infof(base.KeyAll, "Logging: Console level: %v", base.ConsoleLogLevel())
	base.Infof(base.KeyAll, "Logging: Console keys: %v", base.ConsoleLogKey().EnabledLogKeys())
	base.Infof(base.KeyAll, "Logging: Redaction level: %s", config.Logging.RedactionLevel)

	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Infof(base.KeyAll, "Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}

	SetMaxFileDescriptors(config.MaxFileDescriptors)

	// Set global bcrypt cost if configured
	if config.BcryptCost > 0 {
		if err := auth.SetBcryptCost(config.BcryptCost); err != nil {
			base.Fatalf(base.KeyAll, "Configuration error: %v", err)
		}
	}

	sc := NewServerContext(config)
	for _, dbConfig := range config.Databases {
		if _, err := sc.AddDatabaseFromConfig(dbConfig); err != nil {
			base.Fatalf(base.KeyAll, "Error opening database %s: %+v", base.MD(dbConfig.Name), err)
		}
	}

	if config.ProfileInterface != nil {
		//runtime.MemProfileRate = 10 * 1024
		base.Infof(base.KeyAll, "Starting profile server on %s", base.UD(*config.ProfileInterface))
		go func() {
			http.ListenAndServe(*config.ProfileInterface, nil)
		}()
	}

	go sc.PostStartup()

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting admin server on %s", *config.AdminInterface)
	go config.Serve(*config.AdminInterface, CreateAdminHandler(sc))

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting server on %s ...", *config.Interface)
	config.Serve(*config.Interface, CreatePublicHandler(sc))
}

func HandleSighup() {
	for logger, err := range base.RotateLogfiles() {
		if err != nil {
			base.Warnf(base.KeyAll, "Error rotating %v: %v", logger, err)
		}
	}
}

func GetConfig() *ServerConfig {
	return config
}

func RegisterSignalHandler() {
	signalchannel := make(chan os.Signal, 1)
	signal.Notify(signalchannel, syscall.SIGHUP, os.Interrupt, os.Kill)

	go func() {
		for sig := range signalchannel {
			base.Infof(base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGHUP:
				HandleSighup()
			case os.Interrupt, os.Kill:
				// Ensure log buffers are flushed before exiting.
				base.FlushLogBuffers()
				os.Exit(130) // 130 == exit code 128 + 2 (interrupt)
			}
		}
	}()
}

func PanicHandler() (panicHandler func()) {
	return func() {
		// Recover from any panics to allow for graceful shutdown.
		if r := recover(); r != nil {
			base.Fatalf(base.KeyAll, "Handling panic: %v\n%v", r, string(debug.Stack()))
		}
	}

}

// Main entry point for a simple server; you can have your main() function just call this.
// It parses command-line flags, reads the optional configuration file, then starts the server.
//
// Note: Changes in here probably need to be made in the corresponding sg-accel ServerMain!
func ServerMain(runMode SyncGatewayRunMode) {
	RegisterSignalHandler()
	defer PanicHandler()()

	var unknownFieldsErr error
	err := ParseCommandLine(runMode)
	if errors.Cause(err) == ErrUnknownField {
		unknownFieldsErr = err
	} else if err != nil {
		base.Fatalf(base.KeyAll, err.Error())
	}

	// Logging config will now have been loaded from command line
	// or from a sync_gateway config file so we can validate the
	// configuration and setup logging now
	warnings, err := config.SetupAndValidateLogging()
	if err != nil {
		// If we didn't set up logging correctly, we *probably* can't log via normal means...
		// as a best-effort, last-ditch attempt, we'll log to stderr as well.
		log.Printf("[ERR] Error setting up logging: %v", err)
		base.Fatalf(base.KeyAll, "Error setting up logging: %v", err)
	}

	// This is the earliest opportunity to log a startup indicator
	// that will be persisted in all log files.
	base.LogSyncGatewayVersion()

	// If we got an unknownFields error when reading the config
	// log and exit now we've tried setting up the logging.
	if unknownFieldsErr != nil {
		base.Fatalf(base.KeyAll, unknownFieldsErr.Error())
	}

	// Execute any deferred warnings from setup.
	for _, logFn := range warnings {
		logFn()
	}

	// Validation
	if errorMsgs := config.setupAndValidateDatabases(); errorMsgs != nil && len(errorMsgs) > 0 {
		for _, err := range errorMsgs {
			base.Errorf(base.KeyAll, "Error during config validation: %v", err)
		}
		base.Fatalf(base.KeyAll, "Error(s) during config validation")
	}

	RunServer(config)
}
