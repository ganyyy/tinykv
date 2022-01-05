package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	option   badger.Options
	badgerDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	var storage StandAloneStorage
	storage.option = badger.DefaultOptions
	storage.option.Dir = conf.DBPath
	storage.option.ValueDir = conf.DBPath
	return &storage
}

func (s *StandAloneStorage) Start() (err error) {
	s.badgerDB, err = badger.Open(s.option)
	return
}

func (s *StandAloneStorage) Stop() error {
	return s.badgerDB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	var txn = s.badgerDB.NewTransaction(false)
	return NewStorageReader(txn, ctx), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.badgerDB.Update(func(txn *badger.Txn) error {
		var err error
		for _, b := range batch {
			switch t := b.Data.(type) {
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(t.Cf, t.Key))
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(t.Cf, t.Key), t.Value)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}
