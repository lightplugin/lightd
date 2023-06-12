package ldgo

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v210"
)

func GetInfoByUid(ctx context.Context, txn *dgo.Txn, dType, uid string, fields []string) ([]byte, error) {
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
