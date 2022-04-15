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
	innerDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	return &StandAloneStorage{innerDB: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		// TODO: Need to Implement a batch put/delete CF
		case storage.Put:
			engine_util.PutCF(s.innerDB, data.Cf, data.Key, data.Value)
		case storage.Delete:
			engine_util.DeleteCF(s.innerDB, data.Cf, data.Key)
		}
	}
	return nil
}

// Implement Reader Interface for *StandAloneStorage
func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	// When the key doesn't exist, return nil for the value
	value, err := engine_util.GetCF(s.innerDB, cf, key)
	if err != nil {
		if err.Error() == badger.ErrKeyNotFound.Error() {
			return nil, nil
		}
	}
	return value, nil
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	txn := s.innerDB.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (s *StandAloneStorage) Close() {

}
