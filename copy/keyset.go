package copy

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"

	"github.com/jackc/pgx/v4"
)

type keysetSeq struct {
	pos   int
	pages []*IdRange
}

func (ks *keysetSeq) Load(file string) error {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	pages := []*IdRange{}
	err = json.Unmarshal(bytes, &pages)
	if err != nil {
		return err
	}
	ks.pos = 0
	ks.pages = pages
	return nil
}

func (ks *keysetSeq) Save(file string) error {
	bytes, err := json.Marshal(ks.pages)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, bytes, 0644)
}

func (ks *keysetSeq) Next() *IdRange {
	if ks.pos >= len(ks.pages) {
		return nil
	}
	next := ks.pages[ks.pos]
	ks.pos++
	return next
}

func KeysetPaginateTable(ctx context.Context, txn pgx.Tx, schemaName, tableName string, batchSize int) (IdRangeSeq, error) {
	sql := fmt.Sprintf("SELECT id FROM ( SELECT id, row_number() OVER(ORDER BY id) FROM %s.%s ) AS ks WHERE row_number %% %d = 1 ORDER BY id;",
		schemaName, tableName, batchSize)

	rows, err := txn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := []int64{}
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return &keysetSeq{pages: nil, pos: 0}, nil
	}

	ranges := []*IdRange{}
	for i := 0; i < len(ids); i++ {
		startAt := ids[i]
		var endAt int64
		if len(ids)-1 > i {
			endAt = ids[i+1] - 1
		} else {
			endAt = math.MaxInt64
		}

		ranges = append(ranges, &IdRange{StartAt: startAt, EndAt: endAt})
	}
	return &keysetSeq{pages: ranges, pos: 0}, nil
}
