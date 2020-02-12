package main

import (
	"fmt"
	"errors"
	"strings"

	// "github.com/kr/pretty"
	"github.com/film42/pghost/pgoutput"
	// "github.com/Masterminds/squirrel"
)

var (
	ErrNoRelationFound = errors.New("error: no relation found for given oid")
)

type PgOutputUtil struct {
	relcache map[uint32]*pgoutput.Relation
}

func NewPgOutputUtil() *PgOutputUtil {
	return &PgOutputUtil{
		relcache: map[uint32]*pgoutput.Relation{},
	}
}

func (pg *PgOutputUtil) CacheRelation(rel *pgoutput.Relation) {
	pg.relcache[rel.ID] = rel
}

func insertIntoSql(schema, table string, cols, values []string) string {
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		schema, table, strings.Join(cols, ","), strings.Join(values, ","))
}

func colAttToString(col *pgoutput.Column, data []byte) string {
	switch col.Type {
	case 0x14, 0x17:
		return string(data)
	default:
		return fmt.Sprintf("'%s'", string(data))
	}
}

func (pg *PgOutputUtil) HandleInsert(record *pgoutput.Insert) error {
	rel, exists := pg.relcache[record.RelationID]
	if !exists {
		return ErrNoRelationFound
	}

	if !record.New {
		return errors.New("Can't hanlde non-new insert for now")
	}

	if len(record.Row) != len(rel.Columns) {
		return errors.New("Can't handle mis-matched cols. Currently assuming it's handed in rel cols order")
	}

	//sql := squirrel.Insert(fmt.Sprintf("%s.%s", rel.Namespace, rel.Name))

	colStrs := make([]string, len(rel.Columns))
	for i, col := range rel.Columns {
		fmt.Println("Col:", col.Type)
		colStrs[i] = col.Name
	}
	//sql = sql.Columns(colStrs...)

	values := make([]string, len(record.Row))
	for i, tuple := range record.Row {
		col := rel.Columns[i]
		values[i] = colAttToString(&col, tuple.Value)
	}
	//sql = sql.Values(values...)

	fmt.Println("SQL:", insertIntoSql(rel.Namespace, rel.Name, colStrs, values))
	return nil
}
