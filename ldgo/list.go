package ldgo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v210"
)

// Construct params for filter results by relationship
type RelFilterParam struct {
	RelType  string
	EdgeName string
	RelUids  string
}

// Construct params for filter results by exactly fields
type ExaFilterParam struct {
	Predicate string
	DFunc     string
	Value     string
}

type DgraphQuery struct {
	DType             string
	DPredicates       []string
	ExaFilterParams   []*ExaFilterParam
	RelFilterParams   []*RelFilterParam
	FuzzyMatchFields  []string
	FuzzyMatchKeyword string
	OrderOptions      []string
	DefaultOrder      string
	Order             string
	PageSize          int64
	Current           int64
}

func GetDgraphData(ctx context.Context, txn *dgo.Txn, dType, uid string, fields []string) ([]byte, error) {
	f := strings.Join(fields, " ")
	q := fmt.Sprintf(`
	{
		data(func: type(%s))
		@filter(uid(%s))
		{%s}
	}
	`, dType, uid, f)

	fmt.Println(q)
	resp, err := txn.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}
	return resp.Json, err
}

func (d *DgraphQuery) getFuzzyFilter() string {
	c := len(d.FuzzyMatchFields)
	if c == 0 {
		return ""
	}

	filters := make([]string, c)
	for i, f := range d.FuzzyMatchFields {
		filters[i] = fmt.Sprintf(`regexp(%s, /%s/)`, f, d.FuzzyMatchKeyword)
	}
	filter := strings.Join(filters, " or ")

	return filter
}

func (d *DgraphQuery) getPreFilter() string {
	filter := ""

	fuzzyFilter := d.getFuzzyFilter()
	filter = filter + fuzzyFilter

	// TODO: add key-value filter

	if filter == "" {
		return ""
	}

	return filter
}

func (d *DgraphQuery) getRelFilter() (vars, relFilters string) {
	if len(d.RelFilterParams) == 0 {
		return "", ""
	}

	filters := make([]string, len(d.RelFilterParams))

	for i, v := range d.RelFilterParams {
		vars += fmt.Sprintf(`
		var(func:type(%s))
    @filter(uid(%s))
    {uid%v as %s}
		`, v.RelType, v.RelUids, i, v.EdgeName)
		filters[i] = fmt.Sprintf(`uid(uid%v)`, i)
	}

	relFilters = strings.Join(filters, " or ")
	return vars, relFilters
}

func (d *DgraphQuery) getOrder() (string, error) {
	if d.Order == "" {
		return d.DefaultOrder, nil
	}

	orderType := "orderasc"
	field := d.Order
	if strings.HasPrefix(d.Order, "-") {
		orderType = "orderdesc"
		field = d.Order[1:]
	}

	if !StrInStrArray(field, d.OrderOptions) {
		return "", errors.New(fmt.Sprintf("排序字段%s不存在！", field))
	}

	return orderType + ": " + field, nil
}

func (d *DgraphQuery) getPagination() string {

	first := int64(20)
	offset := int64(0)
	if d.PageSize < 0 {
		first = 1000
	} else {
		first = d.PageSize
		offset = (d.Current - 1) * int64(first)
	}

	pageParams := fmt.Sprintf(`first:%v, offset:%v`, first, offset)

	return pageParams
}

func (d *DgraphQuery) getQuery() (string, error) {

	predicates := strings.Join(d.DPredicates, " ")

	fuzzyFilter := d.getFuzzyFilter()

	relVars, relFilters := d.getRelFilter()

	filter := fmt.Sprintf(`@filter((%s))`, fuzzyFilter)
	if relFilters != "" {
		filter = fmt.Sprintf(`@filter((%s)and(%s))`, fuzzyFilter, relFilters)
	}

	order, err := d.getOrder()
	if err != nil {
		return "", err
	}

	pagination := d.getPagination()

	q := fmt.Sprintf(`
	{
		%s
		Total(func:type(%s))
		%s
		{count(uid)}

		Data(func: type(%s), %s, %s)
		%s
		{%s}
	}
	`, relVars, d.DType, filter, d.DType, order, pagination, filter, predicates)

	return q, nil
}

func (d *DgraphQuery) Query(ctx context.Context, txn *dgo.Txn) ([]byte, error) {
	q, err := d.getQuery()
	if err != nil {
		return nil, err
	}

	fmt.Println(q)
	resp, err := txn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return resp.Json, err
}
