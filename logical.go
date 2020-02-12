package main

import (
	"errors"
	"fmt"
	"log"
	"strings"

	// "github.com/kr/pretty"
	"github.com/film42/pghost/pgoutput"
	// "github.com/Masterminds/squirrel"
)

var (
	ErrNoRelationFound = errors.New("error: no relation found for given oid")
)

// SELECT oid, typname FROM pg_type where typcategory = 'N';
var NumericalOidTypeMap = map[uint32]string{
	20:    "int8",
	21:    "int2",
	23:    "int4",
	24:    "regproc",
	26:    "oid",
	700:   "float4",
	701:   "float8",
	790:   "money",
	1700:  "numeric",
	2202:  "regprocedure",
	2203:  "regoper",
	2204:  "regoperator",
	2205:  "regclass",
	2206:  "regtype",
	4096:  "regrole",
	4089:  "regnamespace",
	3734:  "regconfig",
	3769:  "regdictionary",
	13140: "cardinal_number",
}

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
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) DO NOTHING;",
		schema, table, strings.Join(cols, ","), strings.Join(values, ","))
}

func updateSql(schema, table string, colWithValues, whereColWithValues map[string]string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("UPDATE %s.%s SET ", schema, table))

	assignments := []string{}
	for col, val := range colWithValues {
		assignments = append(assignments, fmt.Sprintf("%s = %s", col, val))
	}
	b.WriteString(strings.Join(assignments, ", "))
	b.WriteString(" WHERE ")

	assignments = assignments[:0]
	for col, val := range whereColWithValues {
		assignments = append(assignments, fmt.Sprintf("%s = %s", col, val))
	}
	b.WriteString(strings.Join(assignments, " AND "))
	b.WriteString(";")
	return b.String()
}

func columnAttributeToString(colType uint32, data []byte) string {
	str := string(data)
	_, isNumeric := NumericalOidTypeMap[colType]
	if isNumeric {
		return str
	}
	return fmt.Sprintf("'%s'", str)
}

func (pg *PgOutputUtil) HandleUpdate(record *pgoutput.Update) error {
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

	colsWithValues := map[string]string{}
	whereColsWithValues := map[string]string{}

	for i, tuple := range record.Row {
		col := rel.Columns[i]
		value := columnAttributeToString(col.Type, tuple.Value)
		colsWithValues[col.Name] = value

		if col.Name == "id" {
			whereColsWithValues[col.Name] = value
		}
	}

	log.Println("SQL:", updateSql(rel.Namespace, rel.Name, colsWithValues, whereColsWithValues))
	return nil
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
		//fmt.Println("Col:", col.Type)
		colStrs[i] = col.Name
	}
	//sql = sql.Columns(colStrs...)

	values := make([]string, len(record.Row))
	for i, tuple := range record.Row {
		col := rel.Columns[i]
		values[i] = columnAttributeToString(col.Type, tuple.Value)
	}
	//sql = sql.Values(values...)

	log.Println("SQL:", insertIntoSql(rel.Namespace, rel.Name, colStrs, values))
	return nil
}
