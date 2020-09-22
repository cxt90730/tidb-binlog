package sync

import (
	"context"
	"fmt"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
	"github.com/journeymidnight/yig/meta/types"
	"math"
	"strconv"
)

const TableTempObjectPartsPrefix = "op"  // for convert

type TableEvent interface {
	Insert(ref interface{}) error
	Update(oldRef, ref interface{}) error
	Delete(ref interface{}) error
}

type BucketsEvent struct {
	tikvCli *tikvclient.TiKVClient
}

func (e BucketsEvent) Insert(ref interface{}) error {
	b := ref.(*types.Bucket)
	return e.tikvCli.PutBucket(*b)
}

func (e BucketsEvent) Update(oldRef, ref interface{}) error {
	return e.Insert(ref)
}

func (e BucketsEvent) Delete(ref interface{}) error {
	b := ref.(*types.Bucket)
	// NOTE: Delete bucket will delete users at the same time.
	// so delete user will not be handled.
	return e.tikvCli.DeleteBucket(*b)
}

type ObjectsEvent struct{
	tikvCli *tikvclient.TiKVClient
}

func (e ObjectsEvent) Insert(ref interface{}) error {
	o := ref.(*types.Object)
	if o.Type == types.ObjectTypeMultipart {
		v := math.MaxUint64 - o.CreateTime
		partStartKey := GenTempObjectPartKey(o.BucketName, o.Name, strconv.FormatUint(v, 10), 0)
		partEndKey := GenTempObjectPartKey(o.BucketName, o.Name, strconv.FormatUint(v, 10), 10000)
		kvs, err := e.tikvCli.TxScan(partStartKey, partEndKey, 10000, nil)
		if err != nil {
			return err
		}
		var parts = make(map[int]*types.Part)
		for _, kv := range kvs {
			var part types.Part
			err = helper.MsgPackUnMarshal(kv.V, &part)
			if err != nil {
				return err
			}
			parts[part.PartNumber] = &part
		}
		o.Parts = parts
	}
	return e.tikvCli.PutObject(o, nil, false)
}

func (e ObjectsEvent) Update(oldRef, ref interface{}) error {
	o := ref.(*types.Object)
	oo := oldRef.(*types.Object)
	// only update meta data
	if o.BucketName == oo.BucketName && o.Name == oo.Name && o.CreateTime == oo.CreateTime {
		return e.Insert(ref)
	} else if o.BucketName == oo.BucketName && o.Name != oo.Name && o.CreateTime == oo.CreateTime{
		// update name
		oldKey := tikvclient.GenObjectKey(oo.BucketName, oo.Name, types.NullVersion)
		newKey := tikvclient.GenObjectKey(o.BucketName, o.Name, types.NullVersion)
		tx, err := e.tikvCli.TxnCli.Begin(context.TODO())
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = tx.Commit(context.Background())
			}
			if err != nil {
				tx.Rollback()
			}
		}()
		if o.Type == types.ObjectTypeMultipart {
			v := math.MaxUint64 - o.CreateTime
			partStartKey := GenTempObjectPartKey(o.BucketName, o.Name, strconv.FormatUint(v, 10), 0)
			partEndKey := GenTempObjectPartKey(o.BucketName, o.Name, strconv.FormatUint(v, 10), 10000)
			kvs, err := e.tikvCli.TxScan(partStartKey, partEndKey, 10000, nil)
			if err != nil {
				return err
			}
			var parts = make(map[int]*types.Part)
			for _, kv := range kvs {
				var part types.Part
				err = helper.MsgPackUnMarshal(kv.V, &part)
				if err != nil {
					return err
				}
				parts[part.PartNumber] = &part
			}
			o.Parts = parts
		}
		v, err := helper.MsgPackMarshal(*o)
		if err != nil {
			return err
		}
		err = tx.Set(newKey, v)
		if err != nil {
			return err
		}
		err = tx.Delete(oldKey)
		if err != nil {
			return err
		}
	} else {
		// upload object. In this time, the latest object parts has been uploaded.
		return e.Insert(ref)
	}
	return nil
}

func (e ObjectsEvent) Delete(ref interface{}) error {
	o := ref.(*types.Object)
	return e.tikvCli.DeleteObject(o, nil)
}

// version = maxInt64 - object.CreateTime
func GenTempObjectPartKey(bucketName, objectName, version string, partNum int) []byte {
	return tikvclient.GenKey(TableTempObjectPartsPrefix, bucketName, objectName, version, fmt.Sprintf("%05d", partNum))
}

type ObjectPartEvent struct{tikvCli *tikvclient.TiKVClient}

func (e ObjectPartEvent) Insert(ref interface{}) error {
	p := ref.(*types.Part)
	version := strconv.FormatUint(p.Version, 10)
	key := GenTempObjectPartKey(p.BucketName, p.ObjectName, version, p.PartNumber)
	return e.tikvCli.TxPut(key, *p)
}

// NOTE: update part only occurs when update object name
func (e ObjectPartEvent) Update(oldRef, ref interface{}) error {
	return e.Insert(ref)
}

// NOTE: In tikv, delete object part occurs when delete object or update object
func (e ObjectPartEvent) Delete(ref interface{}) error {
	p := ref.(*types.Part)
	version := strconv.FormatUint(p.Version, 10)
	key := GenTempObjectPartKey(p.BucketName, p.ObjectName, version, p.PartNumber)
	return e.tikvCli.TxDelete(key)
}

type MultipartEvent struct{
	tikvCli *tikvclient.TiKVClient
}

func (e MultipartEvent) Insert(ref interface{}) error {
	m := ref.(*types.Multipart)
	return e.tikvCli.CreateMultipart(*m)
}

// NOTE: update multipart shouldn't happen
func (e MultipartEvent) Update(oldRef, ref interface{}) error {
	return nil
}

func (e MultipartEvent) Delete(ref interface{}) error {
	m := ref.(*types.Multipart)
	return e.tikvCli.DeleteMultipart(m, nil)
}

type PartsEvent struct{
	tikvCli *tikvclient.TiKVClient
}

func (e PartsEvent) Insert(ref interface{}) error {
	p := ref.(*types.Part)
	uploadId := types.GetMultipartUploadIdByDbTime(p.Version)
	partKey := tikvclient.GenObjectPartKey(p.BucketName, p.ObjectName, uploadId, p.PartNumber)
	return e.tikvCli.TxPut(partKey, *p)
}

func (e PartsEvent) Update(oldRef, ref interface{}) error {
	return e.Insert(ref)
}

func (e PartsEvent) Delete(ref interface{}) error {
	p := ref.(*types.Part)
	uploadId := types.GetMultipartUploadIdByDbTime(p.Version)
	partKey := tikvclient.GenObjectPartKey(p.BucketName, p.ObjectName, uploadId, p.PartNumber)
	return e.tikvCli.TxDelete(partKey)
}

type UsersEvent struct{
	tikvCli *tikvclient.TiKVClient
}

func (e UsersEvent) Insert(ref interface{}) error {
	u := ref.(*types.Users)
	userKey := tikvclient.GenUserBucketKey(u.OwnerId, u.BucketName)
	return e.tikvCli.TxPut(userKey, *u)
}

func (e UsersEvent) Update(oldRef, ref interface{}) error {
	return e.Insert(ref)
}

func (e UsersEvent) Delete(ref interface{}) error {
	// NOTE: Delete bucket will delete users at the same time.
	// so delete user will not be handled.
	return nil
}
