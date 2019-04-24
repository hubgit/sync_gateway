package db

import (
	"expvar"

	sgbucket "github.com/couchbase/sg-bucket"
)

// Number of recently-accessed doc revisions to cache in RAM
var KDefaultRevisionCacheCapacity uint32 = 5000

// Number of shards the ShardedLRURevisionCache has
var KDefaultNumLRUCacheShards uint16 = 8

// ShardedLRURevisionCache is a sharded version of the LRURevisionCache intended to
// reduce cache contention under high read and write concurrency levels across different DocIDs.
type ShardedLRURevisionCache struct {
	caches    []*LRURevisionCache
	numShards uint16
}

// NewShardedLRURevisionCache creates a sharded version of the LRURevisionCache with the given capacity and an optional loader function.
func NewShardedLRURevisionCache(capacity uint32, loaderFunc RevisionCacheLoaderFunc, statsCache *expvar.Map) *ShardedLRURevisionCache {

	numShards := KDefaultNumLRUCacheShards
	if capacity == 0 {
		capacity = KDefaultRevisionCacheCapacity
	}

	caches := make([]*LRURevisionCache, numShards)
	perCacheCapacity := uint32(capacity/uint32(numShards)) + 1
	for i := 0; i < int(numShards); i++ {
		caches[i] = NewLRURevisionCache(perCacheCapacity, loaderFunc, statsCache)
	}

	return &ShardedLRURevisionCache{
		caches:    caches,
		numShards: numShards,
	}
}

// getShardIndex is a helper function to get the shard number for a given DocID
func (sc *ShardedLRURevisionCache) getShardIndex(docID string) uint32 {
	return sgbucket.VBHash(docID, sc.numShards)
}

// getShard is a helper function to get a reference to the shard for a given DocID
func (sc *ShardedLRURevisionCache) getShard(docID string) *LRURevisionCache {
	return sc.caches[sc.getShardIndex(docID)]
}

// Get implements the method for RevisionCache.Get
func (sc *ShardedLRURevisionCache) Get(docID, revID string) (DocumentRevision, error) {
	return sc.getShard(docID).Get(docID, revID)
}

// GetActive implements the method for RevisionCache.GetActive
func (sc *ShardedLRURevisionCache) GetActive(docID string, context *DatabaseContext) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).GetActive(docID, context)
}

// Put implements the method for RevisionCache.Put
func (sc *ShardedLRURevisionCache) Put(docID string, docRev DocumentRevision) {
	sc.getShard(docID).Put(docID, docRev)
}

// UpdateDelta implements the method for RevisionCache.UpdateDelta
func (sc *ShardedLRURevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	sc.getShard(docID).UpdateDelta(docID, revID, toDelta)
}

/**
func (sc *ShardedLRURevisionCache) GetCached(docID, revID string) (DocumentRevision, error) {
	return sc.getShard(docID).GetCached(docID, revID)
}

func (sc *ShardedLRURevisionCache) GetWithCopy(docID, revID string, copyType BodyCopyType) (DocumentRevision, error) {
	return sc.getShard(docID).GetWithCopy(docID, revID, copyType)
}
**/
