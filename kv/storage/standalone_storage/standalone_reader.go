package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type StorageReader struct {
	tx        *badger.Txn
	ctx       *kvrpcpb.Context
	iterators []engine_util.DBIterator
}

func NewStorageReader(tx *badger.Txn, ctx *kvrpcpb.Context) storage.StorageReader {
	return &StorageReader{
		tx:  tx,
		ctx: ctx,
	}
}

// When the key doesn't exist, return nil for the value
func (s *StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var val, err = engine_util.GetCFFromTxn(s.tx, cf, key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (s *StorageReader) IterCF(cf string) engine_util.DBIterator {
	var iterator = engine_util.NewCFIterator(cf, s.tx)
	s.iterators = append(s.iterators, iterator)
	return iterator
}

func (s *StorageReader) Close() {
	for _, iter := range s.iterators {
		iter.Close()
	}
	s.tx.Discard()
}
