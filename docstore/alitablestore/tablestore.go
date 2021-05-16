package alitablestore

import (
	"context"
	"log"
	"reflect"
	"strings"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/pkg/errors"
	"gocloud.dev/docstore/driver"
	"gocloud.dev/gcerrors"
)


func (t table) Key(doc driver.Document) (interface{}, error) {
	pkey, err := doc.GetField(t.partitionKey)
	if err != nil || pkey == nil || driver.IsEmptyValue(reflect.ValueOf(pkey)) {
		return nil, errors.New("no partition key provided")
	}
	keys := [2]interface{}{pkey}
	if t.sortKey != "" {
		keys[1], _ = doc.GetField(t.sortKey)
	}
	return keys, nil
}

func (t table) RevisionField() string {
	return ""
}

func (t table) runGets(ctx context.Context, actions []*driver.Action, errs []error, opts *driver.RunActionsOptions) {
	for _, groups := range driver.GroupByFieldPath(actions) {
		for _, group := range groups {
			log.Println(group.Key, group.FieldPaths, group.Doc, "----")
		}
	}
}

func (t table) runWrites(ctx context.Context, actions []*driver.Action, errs []error, opts *driver.RunActionsOptions) {
	for _, action := range actions {

		v, err := action.Doc.GetField(t.partitionKey)
		if err != nil {
			continue
		}

		pk := new(tablestore.PrimaryKey)
		pk.AddPrimaryKeyColumn(t.partitionKey, v)

		if t.sortKey != "" {
			v, _ := action.Doc.GetField(t.sortKey)
			pk.AddPrimaryKeyColumn(t.sortKey, v)
		}

		fields := action.Doc.FieldNames()
		var cols []tablestore.AttributeColumn

		for _, field := range fields {
			if field == t.partitionKey {
				continue
			}
			v, err := action.Doc.GetField(field)
			if err != nil {
				continue
			}
			if field == t.sortKey {
				continue
			}

			cols = append(cols, tablestore.AttributeColumn{
				ColumnName: field,
				Value: v,
			})
		}

		change := tablestore.PutRowChange{
			TableName:  t.name,
			PrimaryKey: pk,
			ReturnType: tablestore.ReturnType_RT_NONE,
			Columns: cols[:],
		}
		change.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		req := &tablestore.PutRowRequest{
			PutRowChange: &change,
		}

		resp, err := t.c.PutRow(req)
		log.Println(resp, err)
	}

}

func (t table) RunActions(ctx context.Context, actions []*driver.Action, opts *driver.RunActionsOptions) driver.ActionListError {
	errs := make([]error, len(actions))

	_, gets, writes, _ := driver.GroupActions(actions)

	ch := make(chan struct{})
	go func() { defer close(ch); t.runWrites(ctx, writes, errs, opts) }()
	<-ch
	t.runGets(ctx, gets, errs, opts)

	return nil
}


type queryRunner struct {
	collection *table
	searchQuery *tablestore.SearchRequest
	rangeQuery *tablestore.GetRangeRequest
	batchQuery *tablestore.BatchGetRowRequest
	rowQuery *tablestore.GetRowRequest
}

func (q *queryRunner) runSearchQuery(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, nil
}
func (q *queryRunner) runRangeQuery(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, nil
}
func (q *queryRunner) runBatchQuery(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, nil
}

func (q *queryRunner) runGetRowQuery(ctx context.Context) ([]map[string]interface{}, error) {
	res, err := q.collection.c.GetRow(q.rowQuery)
	if err != nil {
		return nil, errors.Wrapf(err, "runGetRowQuery failed")
	}
	m := make(map[string]interface{})

	for _, pk := range res.PrimaryKey.PrimaryKeys {
		m[pk.ColumnName] = pk.Value
	}
	for _, col := range res.Columns {
		m[col.ColumnName] = col.Value
	}

	return []map[string]interface{}{m}, nil
}


func (q *queryRunner) run(ctx context.Context) ([]map[string]interface{}, error) {
	if q.searchQuery != nil {
		return q.runSearchQuery(ctx)
	}
	if q.rangeQuery != nil {
		return q.runRangeQuery(ctx)
	}
	if q.batchQuery != nil {
		return q.runBatchQuery(ctx)
	}
	if q.rowQuery != nil {
		return q.runGetRowQuery(ctx)
	}
	return nil, errors.New("error")
}


func (t *table) isPrimaryKeyQuery(q *driver.Query) bool {
	flag := 0
	for _, f := range q.Filters {
		p := strings.Join(f.FieldPath, ".")
		if p == t.partitionKey {
			flag += 1
			continue
		}
		if p == t.sortKey {
			flag += 1
			continue
		}
	}
	return flag == 2
}

func createRowQueryRunner(t *table, q *driver.Query) *queryRunner {

	req := new(tablestore.GetRowRequest)
	cri := new(tablestore.SingleRowQueryCriteria)
	pkey := new(tablestore.PrimaryKey)

	for _, f := range q.Filters {
		fp := strings.Join(f.FieldPath, ".")
		if fp == t.partitionKey {
			pkey.AddPrimaryKeyColumn(t.partitionKey, f.Value)
			continue
		}
		if fp == t.sortKey {
			pkey.AddPrimaryKeyColumn(t.sortKey, f.Value)
			continue
		}
	}

	cri.PrimaryKey = pkey
	for _, fp := range q.FieldPaths {
		c := strings.Join(fp, ".")
		cri.ColumnsToGet = append(cri.ColumnsToGet, c)
	}

	req.SingleRowQueryCriteria = cri
	req.SingleRowQueryCriteria.TableName = t.name
	req.SingleRowQueryCriteria.MaxVersion = 1
	return &queryRunner{
		collection: t,
		rowQuery: req,
	}
}


func createQueryRunner(t *table, q *driver.Query) *queryRunner {
	if t.isPrimaryKeyQuery(q) {
		return createRowQueryRunner(t, q)
	}
	return nil
}


func (t *table) planQuery(q *driver.Query) *queryRunner {
	return createQueryRunner(t, q)
}


func (t table) RunGetQuery(ctx context.Context, q *driver.Query) (driver.DocumentIterator, error) {
	query := t.planQuery(q)
	rows, err := query.run(ctx)
	if err != nil {
		return nil, err
	}
	return &documentIterator{
		rows: rows,
	}, nil

}

func (t table) QueryPlan(query *driver.Query) (string, error) {
	panic("implement me")
}

func (t table) RevisionToBytes(i interface{}) ([]byte, error) {
	panic("implement me")
}

func (t table) BytesToRevision(bytes []byte) (interface{}, error) {
	panic("implement me")
}

func (t table) As(i interface{}) bool {
	panic("implement me")
}

func (t table) ErrorAs(err error, i interface{}) bool {
	panic("implement me")
}

func (t table) ErrorCode(err error) gcerrors.ErrorCode {
	return gcerrors.OK
}

func (t table) Close() error {
	return nil
}

type table struct {
	c            *tablestore.TableStoreClient
	name         string
	partitionKey string
	sortKey      string
	option       *tablestore.TableOption
	meta         *tablestore.TableMeta
	indexes      []*tablestore.IndexMeta
	stream       *tablestore.StreamDetails
}


func newCollection(c *tablestore.TableStoreClient, tableName, partitionKey, sortKey string) (*table, error) {

	req := new(tablestore.DescribeTableRequest)
	req.TableName = tableName

	res, err := c.DescribeTable(req)
	if err != nil {
		return nil, err
	}

	return &table{
		c:            c,
		name:         tableName,
		partitionKey: partitionKey,
		sortKey:      sortKey,
		option:       res.TableOption,
		meta:         res.TableMeta,
		indexes:      res.IndexMetas,
		stream:       res.StreamDetails,
	}, nil
}
