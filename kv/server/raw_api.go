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
	var reader, err = server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	var rsp kvrpcpb.RawGetResponse
	rsp.Value = value
	rsp.NotFound = value == nil
	return &rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	var err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var rsp kvrpcpb.RawPutResponse
	return &rsp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	var err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var rsp kvrpcpb.RawDeleteResponse
	return &rsp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	var reader, err = server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	var iter = reader.IterCF(req.GetCf())

	iter.Seek(req.GetStartKey())
	var rsp kvrpcpb.RawScanResponse
	for idx := 0; idx < int(req.GetLimit()) && iter.Valid(); iter.Next() {
		var item = iter.Item()
		var value, _ = item.ValueCopy(nil)
		rsp.Kvs = append(rsp.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		idx++
	}

	return &rsp, nil
}
