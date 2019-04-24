package db

// BypassRevisionCache implements the revision cache interface but cannot store anything in memory.
type BypassRevisionCache struct{}

func (BypassRevisionCache) Get(docID, revID string) (DocumentRevision, error) {
	panic("implement me")
}

func (BypassRevisionCache) GetActive(docID string, context *DatabaseContext) (docRev DocumentRevision, err error) {
	panic("implement me")
}

// BypassRevisionCache.UpdateDelta is a no-op.
func (BypassRevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	return
}

// BypassRevisionCache.Put is a no-op.
func (BypassRevisionCache) Put(docID string, docRev DocumentRevision) {
	return
}
