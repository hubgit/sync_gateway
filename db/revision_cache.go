package db

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// RevisionCache is an interface that can be used to fetch a DocumentRevision from a Doc ID and Rev ID pair.
// Implementations of this interface can be found under LRURevisionCache, ShardedLRURevisionCache, and BypassRevisionCache.
type RevisionCache interface {
	Get(docID, revID string) (DocumentRevision, error)
	GetActive(docID string, context *DatabaseContext) (docRev DocumentRevision, err error)
	UpdateDelta(docID, revID string, toDelta *RevisionDelta)
	Put(docID string, docRev DocumentRevision)
}

// Make sure that all SG implementations satisfy the interface.
var _ RevisionCache = &LRURevisionCache{}
var _ RevisionCache = &ShardedLRURevisionCache{}
var _ RevisionCache = &BypassRevisionCache{}

// Revision information as returned by the rev cache
type DocumentRevision struct {
	RevID       string
	Body        Body
	History     Revisions
	Channels    base.Set
	Expiry      *time.Time
	Attachments AttachmentsMeta
	Delta       *RevisionDelta
}

type IDAndRev struct {
	DocID string
	RevID string
}

// Callback function signature for loading something from the rev cache
type RevisionCacheLoaderFunc func(id IDAndRev) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error)

type RevisionDelta struct {
	ToRevID           string   // Target revID for the delta
	DeltaBytes        []byte   // The actual delta
	AttachmentDigests []string // Digests for all attachments present on ToRevID
	RevisionHistory   []string // Revision history from parent of ToRevID to source revID, in descending order
}

func newRevCacheDelta(deltaBytes []byte, fromRevID string, toRevision DocumentRevision) *RevisionDelta {
	return &RevisionDelta{
		ToRevID:           toRevision.RevID,
		DeltaBytes:        deltaBytes,
		AttachmentDigests: AttachmentDigests(toRevision.Attachments), // Flatten the AttachmentsMeta into a list of digests
		RevisionHistory:   toRevision.History.parseAncestorRevisions(fromRevID),
	}
}
