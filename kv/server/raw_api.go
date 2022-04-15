package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	storage, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	value, err := storage.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawGetResponse)
	resp.Value = value
	if resp.Value == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	putModify := storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}
	modify := storage.Modify{Data: putModify}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawPutResponse)
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	deleteModify := storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}
	modify := storage.Modify{Data: deleteModify}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawDeleteResponse)
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	it := reader.IterCF(req.Cf)
	defer it.Close()
	i := 0
	resp := new(kvrpcpb.RawScanResponse)
	for it.Seek(req.StartKey); i < int(req.Limit) && it.Valid(); it.Next() {
		item := it.Item()
		kvPair := new(kvrpcpb.KvPair)
		kvPair.Key = item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kvPair.Value = value
		resp.Kvs = append(resp.Kvs, kvPair)
		i++
	}

	return resp, nil
}
