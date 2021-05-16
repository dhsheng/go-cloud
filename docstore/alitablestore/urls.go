package alitablestore

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"gocloud.dev/docstore"
)

const (
	Scheme = "tablestore"
)

func init() {
	docstore.DefaultURLMux().RegisterCollection(Scheme, new(defaultOpener))
}

type defaultOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *defaultOpener) OpenCollectionURL(ctx context.Context, u *url.URL) (*docstore.Collection, error) {
	o.init.Do(func() {
		o.opener = &URLOpener{
			c: nil,
		}
		o.err = nil
	})

	if o.err != nil {
		return nil, fmt.Errorf("open collection %s: %v", u, o.err)
	}
	return o.opener.OpenCollectionURL(ctx, u)
}


type indexSchema struct {
	Name   string
	Fields []*tablestore.FieldSchema
}

func resolveIndex(cli *tablestore.TableStoreClient, tableName, indexName string) (*indexSchema, error) {
	req := tablestore.DescribeSearchIndexRequest{
		TableName: tableName,
		IndexName: indexName,
	}
	resp, err := cli.DescribeSearchIndex(&req)
	if err != nil {
		return nil, err
	}
	return &indexSchema{
		Name:   indexName,
		Fields: resp.Schema.FieldSchemas,
	}, nil
}

func resolveIndexes(cli *tablestore.TableStoreClient, tableName string) ([]indexSchema, error) {
	req := tablestore.ListSearchIndexRequest{
		TableName: tableName,
	}
	reply, err := cli.ListSearchIndex(&req)
	if err != nil {
		return nil, err
	}

	indexes := make([]indexSchema, len(reply.IndexInfo))
	for _, idx := range reply.IndexInfo {
		i, err := resolveIndex(cli, tableName, idx.IndexName)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, *i)
	}
	return indexes, nil
}

type URLOpener struct {
	c *tablestore.TableStoreClient
	indexes []indexSchema
}

func (o *URLOpener) ensureCollection(c *tablestore.TableStoreClient, collection string) error {
	describeTableReq := new(tablestore.DescribeTableRequest)
	describeTableReq.TableName = collection
	_, err := c.DescribeTable(describeTableReq)
	return err
}

func (o *URLOpener) OpenCollectionURL(ctx context.Context, u *url.URL) (*docstore.Collection, error) {
	query := u.Query()
	instance := query.Get("instance")
	accessKeyId := query.Get("access_key")
	accessSecret := query.Get("access_secret")
	collection := query.Get("collection")

	pk := query.Get("partition_key")
	sk := query.Get("sort_key")

	o.c = tablestore.NewClient(
		fmt.Sprintf("https://%s", u.Hostname()), instance, accessKeyId, accessSecret)

	indexes, err := resolveIndexes(o.c, collection)
	if err != nil {
		return nil, err
	}
	o.indexes = indexes
	coll, err := newCollection(o.c, collection, pk, sk)
	if err != nil {
		return nil, err
	}
	return docstore.NewCollection(coll), nil
}
