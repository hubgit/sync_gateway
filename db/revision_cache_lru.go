package db

import (
	"container/list"
	"errors"
	"expvar"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	cache       map[IDAndRev]*list.Element // Fast lookup of list element by doc/rev ID
	lruList     *list.List                 // List ordered by most recent access (Front is newest)
	capacity    uint32                     // Max number of revisions to cache
	loaderFunc  RevisionCacheLoaderFunc    // Function which does actual loading of something from rev cache
	lock        sync.Mutex                 // For thread-safety
	cacheHits   *expvar.Int
	cacheMisses *expvar.Int
}

// NewLRURevisionCache creates a revision cache using an LRU algorithm for eviction with the given capacity and an optional loader function.
func NewLRURevisionCache(capacity uint32, loaderFunc RevisionCacheLoaderFunc, statsCache *expvar.Map) *LRURevisionCache {
	if capacity == 0 {
		capacity = KDefaultRevisionCacheCapacity
	}

	return &LRURevisionCache{
		cache:       map[IDAndRev]*list.Element{},
		lruList:     list.New(),
		capacity:    capacity,
		loaderFunc:  loaderFunc,
		cacheHits:   statsCache.Get(base.StatKeyRevisionCacheHits).(*expvar.Int),
		cacheMisses: statsCache.Get(base.StatKeyRevisionCacheMisses).(*expvar.Int),
	}
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	key         IDAndRev        // doc/rev IDs
	body        Body            // Revision body (a pristine shallow copy)
	history     Revisions       // Rev history encoded like a "_revisions" property
	channels    base.Set        // Set of channels that have access
	expiry      *time.Time      // Document expiry
	attachments AttachmentsMeta // Document _attachments property
	delta       *RevisionDelta  // Available delta *from* this revision
	err         error           // Error from loaderFunc if it failed
	lock        sync.RWMutex    // Synchronizes access to this struct
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get.
func (rc *LRURevisionCache) Get(docid, revid string) (DocumentRevision, error) {
	return rc.getFromCache(docid, revid, BodyShallowCopy, rc.loaderFunc != nil)
}

// Attempts to retrieve the active revision for a document from the cache.  Requires retrieval
// of the document from the bucket to guarantee the current active revision, but does minimal unmarshalling
// of the retrieved document to get the current rev from _sync metadata.  If active rev is already in the
// rev cache, will use it.  Otherwise will add to the rev cache using the raw document obtained in the
// initial retrieval.
func (rc *LRURevisionCache) GetActive(docid string, context *DatabaseContext) (docRev DocumentRevision, err error) {

	// Look up active rev for doc
	bucketDoc, getErr := context.GetDocument(docid, DocUnmarshalSync)
	if getErr != nil {
		return DocumentRevision{}, getErr
	}
	if bucketDoc == nil {
		return DocumentRevision{}, nil
	}

	// Retrieve from or add to rev cache
	value := rc.getValue(docid, bucketDoc.CurrentRev, true)
	docRev, statEvent, err := value.loadForDoc(bucketDoc, context, BodyShallowCopy)
	rc.statsRecorderFunc(statEvent)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return docRev, err
}

// Adds a revision to the cache.
func (rc *LRURevisionCache) Put(docid string, docRev DocumentRevision) {
	if docRev.History == nil {
		panic("Missing history for RevisionCache.Put")
	}
	value := rc.getValue(docid, docRev.RevID, true)
	value.store(docRev)
}

// Attempt to update the delta on a revision cache entry.  If the entry is no longer resident in the cache,
// fails silently
func (rc *LRURevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	value := rc.getValue(docID, revID, false)
	if value != nil {
		value.updateDelta(toDelta)
	}
}

/*
// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) GetCached(docid, revid string) (DocumentRevision, error) {
	return rc.getFromCache(docid, revid, BodyShallowCopy, false)
}

// Returns the body of the revision based on the specified body copy policy (deep/shallow/none)
func (rc *LRURevisionCache) GetWithCopy(docid, revid string, copyType BodyCopyType) (DocumentRevision, error) {
	return rc.getFromCache(docid, revid, copyType, rc.loaderFunc != nil)
}
*/

func (rc *LRURevisionCache) getFromCache(docid, revid string, copyType BodyCopyType, loadOnCacheMiss bool) (DocumentRevision, error) {
	value := rc.getValue(docid, revid, loadOnCacheMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}
	docRev, statEvent, err := value.load(rc.loaderFunc, copyType)
	rc.statsRecorderFunc(statEvent)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return docRev, err
}
func (rc *LRURevisionCache) statsRecorderFunc(cacheHit bool) {

	if cacheHit {
		rc.cacheHits.Add(1)
	} else {
		rc.cacheMisses.Add(1)
	}
}

func (rc *LRURevisionCache) getValue(docid, revid string, create bool) (value *revCacheValue) {
	if docid == "" || revid == "" {
		panic("RevisionCache: invalid empty doc/rev id")
	}
	key := IDAndRev{DocID: docid, RevID: revid}
	rc.lock.Lock()
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{key: key}
		rc.cache[key] = rc.lruList.PushFront(value)
		for len(rc.cache) > int(rc.capacity) {
			rc.purgeOldest_()
		}
	}
	rc.lock.Unlock()
	return
}

func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	if element := rc.cache[value.key]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.key)
	}
	rc.lock.Unlock()
}

func (rc *LRURevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	delete(rc.cache, value.key)
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(loaderFunc RevisionCacheLoaderFunc, copyType BodyCopyType) (docRev DocumentRevision, cacheHit bool, err error) {

	// Attempt to read cached value
	value.lock.RLock()
	if value.body != nil || value.err != nil {
		docRev, err = value._asDocumentRevision(copyType)
		value.lock.RUnlock()
		return docRev, true, err
	}
	value.lock.RUnlock()

	// Cached value not found - attempt to load if loaderFunc provided
	if loaderFunc == nil {
		return DocumentRevision{}, false, errors.New("No loader function defined for revision cache")
	}
	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.body != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		value.body, value.history, value.channels, value.attachments, value.expiry, value.err = loaderFunc(value.key)
	}

	docRev, err = value._asDocumentRevision(copyType)
	value.lock.Unlock()
	return docRev, cacheHit, err
}

// _asDocumentRevision copies the rev cache value into a DocumentRevision.  Requires callers hold at least the read lock on value.lock
func (value *revCacheValue) _asDocumentRevision(copyType BodyCopyType) (DocumentRevision, error) {
	return DocumentRevision{
		RevID:       value.key.RevID,
		Body:        value.body.Copy(copyType), // Never let the caller mutate the stored body
		History:     value.history,
		Channels:    value.channels,
		Expiry:      value.expiry,
		Attachments: value.attachments.ShallowCopy(), // Avoid caller mutating the stored attachments
		Delta:       value.delta,
	}, value.err
}

// Retrieves the body etc. out of a revCacheValue.  If they aren't already present, loads into the cache value using
// the provided document.
func (value *revCacheValue) loadForDoc(doc *document, context *DatabaseContext, copyType BodyCopyType) (docRev DocumentRevision, cacheHit bool, err error) {

	value.lock.RLock()
	if value.body != nil || value.err != nil {
		docRev, err = value._asDocumentRevision(copyType)
		value.lock.RUnlock()
		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.body != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		value.body, value.history, value.channels, value.attachments, value.expiry, value.err = context.revCacheLoaderForDocument(doc, value.key.RevID)
	}

	docRev, err = value._asDocumentRevision(copyType)
	value.lock.Unlock()
	return docRev, cacheHit, err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(docRev DocumentRevision) {
	value.lock.Lock()
	if value.body == nil {
		value.body = docRev.Body.ShallowCopy() // Don't store a body the caller might later mutate
		value.body[BodyId] = value.key.DocID   // Rev cache includes id and rev in the body.  Ensure they are set in case callers aren't passing
		value.body[BodyRev] = value.key.RevID
		value.history = docRev.History
		value.channels = docRev.Channels
		value.expiry = docRev.Expiry
		value.attachments = docRev.Attachments.ShallowCopy() // Don't store attachments the caller might later mutate
		value.err = nil
	}
	value.lock.Unlock()
}

func (value *revCacheValue) updateDelta(toDelta *RevisionDelta) {
	value.lock.Lock()
	value.delta = toDelta
	value.lock.Unlock()
}
