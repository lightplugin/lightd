package ldgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
)

type DField struct {
	Key   string
	Value string
}
type DgModel struct {
	Uid     string
	Uniques []DField
	Fields  []DField
	Edges   []DField
	DType   string
}
type DgraphUid struct {
	Uid string `json:"uid"`
}
type UniCheckRoot struct {
	Me     []DgraphUid `json:"me"`
	Others []DgraphUid `json:"other"`
}
type ExistCheckRoot struct {
	Exist []DgraphUid `json:"exist"`
}

func NewDgModel(i interface{}) (dm *DgModel, err error) {
	value := reflect.ValueOf(i)
	tp := value.Type()

	dFields := []DField{}
	uniFields := []DField{}
	edgeFields := []DField{}

	for i := 0; i < value.NumField(); i++ {

		tagStr := tp.Field(i).Tag.Get("dfield")
		var tags []string
		if tagStr != "" {
			tags = strings.Split(tagStr, ",")
		}

		field := value.Field(i)
		jsonStr := tp.Field(i).Tag.Get("json")
		jsonField := strings.Split(jsonStr, ",")[0]

		dfield, unique, edge, err := analysisField(jsonField, tp.Field(i).Name, field.Interface(), tags)
		if err != nil {
			return nil, err
		}
		if dfield != nil {
			dFields = append(dFields, *dfield)
		}
		if unique != nil {
			uniFields = append(uniFields, *dfield)
		}
		if edge != nil {
			edgeFields = append(edgeFields, *dfield)
		}
	}

	dm = &DgModel{
		DType:   tp.Name(),
		Uniques: uniFields,
		Fields:  dFields,
		Edges:   edgeFields,
	}

	return
}

func (d *DgModel) CreateData(ctx context.Context, txn *dgo.Txn, commitNow bool) error {
	q := ""
	if len(d.Uniques) > 0 {
		filter, err := d.getUniFilter()
		if err != nil {
			return err
		}

		f := fmt.Sprintf(`@filter(%s)`, filter)
		q = fmt.Sprintf(`
		{
			data(func: type(%s))
			%s
			{v as uid}
		}
		`, d.DType, f)
	}

	mu := api.Mutation{
		SetJson: d.genSetJson(),
	}
	if q != "" {
		mu.Cond = `@if(eq(len(v), 0))`
	}

	fmt.Println("创建查询", q)

	req := &api.Request{
		CommitNow: commitNow,
		Query:     q,
		Mutations: []*api.Mutation{
			&mu,
		},
	}
	r, err := txn.Do(ctx, req)
	if err != nil {
		return err
	}

	if r.Uids == nil {
		return errors.New("已存在！")
	}

	return nil
}

func (d *DgModel) UpdateDgraphData(ctx context.Context, txn *dgo.Txn, commitNow bool) error {
	var tuid string
	for _, v := range d.Fields {
		if v.Key == "uid" {
			tuid = v.Value
		}
	}
	fmt.Println(tuid)
	me := fmt.Sprintf(`
		me(func: type(%s))
		@filter(uid(%s))
		{myId as uid}
	`, d.DType, tuid)
	cond := `@if(eq(len(myId), 1))`

	other := ""
	if len(d.Uniques) > 0 {
		filter, err := d.getUniFilter()
		if err != nil {
			return err
		}

		other = fmt.Sprintf(`
		{
			other(func: type(%s))
			@filter((%s) and  not uid(%s))
			{otherId as uid}
		}
		`, d.DType, filter, tuid)
		cond = `@if(eq(len(myId), 1) and eq(len(otherId), 0))`
	}

	q := fmt.Sprintf(`
	{
		%s
		%s
	}
	`, me, other)
	fmt.Printf("query string:%s\n", q)

	de := &api.Mutation{
		Cond: cond,
	}
	delEdges := make([]string, len(d.Edges))
	for i, v := range d.Edges {
		delEdges[i] = v.Key
	}
	fmt.Printf("delete old edge:%s\n", delEdges)
	dgo.DeleteEdges(de, "uid(myId)", delEdges...)

	req := &api.Request{
		CommitNow: commitNow,
		Query:     q,
		Mutations: []*api.Mutation{
			de,
			{
				Cond:    cond,
				SetJson: d.genSetJson(),
			},
		},
	}

	res, err := txn.Do(ctx, req)
	if err != nil {
		return err
	}

	var ucr UniCheckRoot
	err = json.Unmarshal(res.Json, &ucr)
	if err != nil {
		return err
	}

	if len(ucr.Me) < 1 {
		return errors.New("要修改的资源不存在！")
	}
	if len(ucr.Others) > 0 {
		return errors.New("要修改的字段与已有数据产生冲突，请确认！")
	}
	return nil
}

func DeleteData(ctx context.Context, txn *dgo.Txn, uid, dType string, commitNow bool) error {
	q := fmt.Sprintf(`
	{
		exist(func: type(%s))
		@filter(uid("%s"))
		{v as uid}
	}
	`, dType, uid)

	req := &api.Request{
		CommitNow: commitNow,
		Query:     q,
		Mutations: []*api.Mutation{
			{
				Cond:       `@if(eq(len(v), 1))`,
				DeleteJson: []byte(fmt.Sprintf(`{"uid":"%s"}`, uid)),
			},
		},
	}

	res, err := txn.Do(ctx, req)
	if err != nil {
		return err
	}

	var ecr ExistCheckRoot
	err = json.Unmarshal(res.Json, &ecr)
	if err != nil {
		return err
	}

	if len(ecr.Exist) < 1 {
		return errors.New("要删除的资源不存在！")
	}

	return nil
}

func analysisField(jsonField, field string, i interface{}, tags []string) (dField, unique, edge *DField, err error) {
	attrs := map[string]bool{
		"required": false,
		"unique":   false,
		"edge":     false,
	}
	for _, tag := range tags {
		attrs[tag] = true
	}

	tv := ""
	switch v := i.(type) {
	case string:
		if v == "" {
			if attrs["required"] {
				err = errors.New(fmt.Sprintf("%s is required", field))
				return
			}
			tv = `""`
		} else {
			if attrs["edge"] {
				tv = fmt.Sprintf(`{uid:"%s"}`, v)
				edge = &DField{
					Key:   jsonField,
					Value: tv,
				}
			} else {
				tv = fmt.Sprintf(`"%s"`, v)
			}
			if attrs["unique"] {
				unique = &DField{
					Key:   jsonField,
					Value: tv,
				}
			}
		}
	case []string:
		if len(v) == 0 {
			if attrs["required"] {
				err = errors.New(fmt.Sprintf("%s is required", field))
				return
			}
		} else {
			if attrs["edge"] {
				edgeStr := ""
				for _, uid := range v {
					edgeStr += fmt.Sprintf(`{"uid":"%s"}`, uid)
				}
				tv = fmt.Sprintf(`[ %s ]`, edgeStr)
				edge = &DField{
					Key:   jsonField,
					Value: tv,
				}
			} else {
				tempStr := ""
				for _, uid := range v {
					tempStr += fmt.Sprintf(`"%s"`, uid)
				}
				tv = fmt.Sprintf(`[ %s ]`, tempStr)
			}
			if attrs["unique"] {
				unique = &DField{
					Key:   jsonField,
					Value: tv,
				}
			}
		}
	default:
		err = errors.New("类型解析出错")
		return
	}

	if tv != "" {
		dField = &DField{
			Key:   jsonField,
			Value: tv,
		}
	}

	return
}

func (d *DgModel) getUniFilter() (filterStr string, err error) {
	for _, f := range d.Uniques {
		if filterStr == "" {
			filterStr = fmt.Sprintf(`eq(%s, %s)`, f.Key, f.Value)
		} else {
			filterStr = filterStr + fmt.Sprintf(` OR eq(%s, %s)`, f.Key, f.Value)
		}
	}
	return
}

func (d *DgModel) genSetJson() []byte {

	dataStr := fmt.Sprintf(`"dgraph.type":"%s"`, d.DType)
	for _, v := range d.Fields {
		dataStr += fmt.Sprintf(` ,"%s":%v`, v.Key, v.Value)
	}

	pbStr := fmt.Sprintf("{%s}", dataStr)

	fmt.Println("保存结构体：", pbStr)

	return []byte(pbStr)
}
