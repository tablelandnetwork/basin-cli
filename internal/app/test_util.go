package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

// recvWAL reads one line from the reader and unmarshals it into a transaction.
func recvWAL(t *testing.T, jsonIn []byte, feed chan *pgrepl.Tx) {
	var tx pgrepl.Tx
	require.NoError(t, json.Unmarshal(jsonIn, &tx))
	feed <- &tx
}

type testRow struct {
	id   int
	name string
}

func importLocalDB(t *testing.T, file *os.File) *sql.Rows {
	db, err := sql.Open("duckdb", path.Join(t.TempDir(), "temp.db"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSTALL parquet; LOAD parquet;")
	if err != nil {
		t.Fatal(err)
	}

	parquetQuery := fmt.Sprintf(
		"SELECT * FROM read_parquet('%s')", file.Name())
	rows, err := db.Query(parquetQuery)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func queryResult(t *testing.T, rows *sql.Rows) (result []testRow) {
	var id int
	var name string
	for rows.Next() {
		require.NoError(t, rows.Scan(&id, &name))
		row := testRow{
			id:   id,
			name: name,
		}
		result = append(result, row)
	}

	return result
}

var numTypeCols = []Column{
	{Name: "bool_col", Typ: "boolean", IsNull: true, IsPrimary: false},
	{Name: "smallint_col", Typ: "smallint", IsNull: true, IsPrimary: false},
	{Name: "integer_col", Typ: "integer", IsNull: true, IsPrimary: false},
	{Name: "bigint_col", Typ: "bigint", IsNull: true, IsPrimary: false},
	{Name: "float_col", Typ: "real", IsNull: true, IsPrimary: false},
	{Name: "double_col", Typ: "double precision", IsNull: true, IsPrimary: false},
	{Name: "decimal_col", Typ: "numeric", IsNull: true, IsPrimary: false},
	{Name: "udecimal_col", Typ: "numeric", IsNull: true, IsPrimary: false},
}

var byteTypeCols = []Column{
	{Name: "char_default", Typ: "character", IsNull: true, IsPrimary: false},
	{Name: "char_1_col", Typ: "character", IsNull: true, IsPrimary: false},
	{Name: "char_9_col", Typ: "character", IsNull: true, IsPrimary: false},
	{Name: "varchar_1_col", Typ: "character varying", IsNull: true, IsPrimary: false},
	{Name: "varchar_9_col", Typ: "character varying", IsNull: true, IsPrimary: false},
	{Name: "text_col", Typ: "text", IsNull: true, IsPrimary: false},
	{Name: "blob_col", Typ: "bytea", IsNull: true, IsPrimary: false},
	{Name: "json_col_old", Typ: "json", IsNull: true, IsPrimary: false},
	{Name: "json_col_new", Typ: "jsonb", IsNull: true, IsPrimary: false},
	{Name: "uuid_col", Typ: "uuid", IsNull: true, IsPrimary: false},
}

var dateTypeCols = []Column{
	{Name: "date_col", Typ: "date", IsNull: true, IsPrimary: false},
	{Name: "time_col", Typ: "time without time zone", IsNull: true, IsPrimary: false},
	{Name: "timetz_col", Typ: "time with time zone", IsNull: true, IsPrimary: false},
	{Name: "timestamp_col", Typ: "timestamp without time zone", IsNull: true, IsPrimary: false},
	{Name: "timestamptz_col", Typ: "timestamp with time zone", IsNull: true, IsPrimary: false},
}

var numArrayTypeCols = []Column{
	{Name: "bool_col", Typ: "boolean[]", IsNull: true, IsPrimary: false},
	{Name: "smallint_col", Typ: "smallint[]", IsNull: true, IsPrimary: false},
	{Name: "integer_col", Typ: "integer[]", IsNull: true, IsPrimary: false},
	{Name: "bigint_col", Typ: "bigint[]", IsNull: true, IsPrimary: false},
	{Name: "float_col", Typ: "real[]", IsNull: true, IsPrimary: false},
	{Name: "double_col", Typ: "double precision[]", IsNull: true, IsPrimary: false},
	{Name: "numeric_col", Typ: "numeric[]", IsNull: true, IsPrimary: false},
	{Name: "unumeric_col", Typ: "numeric[]", IsNull: true, IsPrimary: false},
}

var byteArrayTypeCols = []Column{
	{Name: "char_col", Typ: "character[]", IsNull: true, IsPrimary: false},
	{Name: "bpchar_col", Typ: "bpchar[]", IsNull: true, IsPrimary: false},
	{Name: "varchar_col", Typ: "character varying[]", IsNull: true, IsPrimary: false},
	{Name: "uvarchar_col", Typ: "character varying[]", IsNull: true, IsPrimary: false},
	{Name: "text_col", Typ: "text[]", IsNull: true, IsPrimary: false},
	{Name: "blob_col", Typ: "bytea[]", IsNull: true, IsPrimary: false},
	{Name: "json_col", Typ: "json[]", IsNull: true, IsPrimary: false},
	{Name: "uuid_col", Typ: "uuid[]", IsNull: true, IsPrimary: false},
}

var macaddrTypeCols = []Column{
	{Name: "macaddr_col", Typ: "macaddr", IsNull: true, IsPrimary: false},
}

var enumArrayTypeCols = []Column{
	{Name: "enum_col", Typ: "enum_type_foo[]", IsNull: true, IsPrimary: false},
}

var customCompositeTypeCols = []Column{
	// column metadata query returns "USER-DEFINED" for custom composite types
	// such as:
	// CREATE TYPE inventory_item AS (
	//	name            text,
	//	supplier_id     integer,
	//	price           numeric
	// );
	{Name: "composite_col", Typ: "USER-DEFINED", IsNull: true, IsPrimary: false},
}

var dateArrayTypeCols = []Column{
	{Name: "date_col", Typ: "date[]", IsNull: true, IsPrimary: false},
	{Name: "time_col", Typ: "time without time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timetz_col", Typ: "time with time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timestamp_col", Typ: "timestamp without time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timestamptz_col", Typ: "timestamp with time zone[]", IsNull: true, IsPrimary: false},
}

var wal = `
{
	"commit_lsn":957398296,
	"records":[
		{
			"action":"I",
			"xid":1058,
			"lsn":"0/3910B898",
			"nextlsn":"",
			"timestamp":"2023-08-22 14:44:04.043586-03",
			"schema":"public",
			"table":"t",
			"columns":[
				{
					"name":"id",
					"type":"%s",
					"value":%s
				}				
			]			
		}
	]
}
`

var supportedTypeVals = map[string][]string{
	"boolean":                       {"true", "false", "null"},
	"bigint":                        {"42", "-42", "null"},
	"double precision":              {"42.01", "-42.01", "null"},
	"integer":                       {"42", "-42", "null"},
	"numeric(4, 7)":                 {"42.01", "-42.01", "null"},
	"real":                          {"42.01", "-42.01", "null"},
	"smallint":                      {"42", "-42", "null"},
	"oid":                           {"42.42", "null"},
	"macaddr":                       {"\"08:00:2b:01:02:03\"", "null"},
	"bytea":                         {"\"00010203\"", "null"},
	"bpchar":                        {"\"a\"", "\"Z\"", "null"},
	"character(1)":                  {"\"a\"", "\"Z\"", "null"},
	"character(5)":                  {"\"aaaaa\"", "\"ZZZZZ\"", "null"},
	"character varying":             {"\"a\"", "\"Zzzzzzzz\"", "null"},
	"character varying(5)":          {"\"aaaaa\"", "\"ZZZZZ\"", "null"},
	"json":                          {"{\"foo\": \"bar\"}", "{\"foo\": {\"bar\": 3}}", "null"},
	"jsonb":                         {"{\"foo\": \"bar\"}", "{\"foo\": {\"bar\": 3}}", "null"},
	"text":                          {"\"dpfkg\"", "null"},
	"uuid":                          {"\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\"", "null"},
	"date":                          {"\"2021-03-01\"", "null"},
	"time with time zone":           {"\"12:00:00-08\"", "null"},
	"time without time zone":        {"\"12:45:01\"", "null"},
	"timestamp with time zone":      {"\"2021-03-01 12:45:01+08\"", "null"},
	"timestamp without time zone":   {"\"2021-03-01 12:45:01\"", "null"},
	"interval":                      {"\"1 year\"", "\"2 mons\"", "\"21 days\"", "\"05:00:00\"", "\"-00:00:07\"", "\"1 year 2 mons 21 days 05:00:00\"", "\"-17 days\"", "null"}, // nolint
	"boolean[]":                     {"\"{t,f,NULL}\"", "null"},
	"bigint[]":                      {"\"{42,-42,NULL}\"", "null"},
	"double precision[]":            {"\"{42.01,-42.01,NULL}\"", "null"},
	"integer[]":                     {"\"{42,-42,NULL}\"", "null"},
	"numeric[]":                     {"\"{42.01,-42.01,NULL}\"", "null"},
	"real[]":                        {"\"{42.01,-42.01,NULL}\"", "null"},
	"smallint[]":                    {"\"{42,-42,NULL}\"", "null"},
	"character[]":                   {"\"{a,Z,NULL}\"", "null"},
	"character varying[]":           {"\"{a,Z,NULL}\"", "null"},
	"text[]":                        {"\"{dpfkg,NULL}\"", "null"},
	"bytea[]":                       {`"{\"\\\\x3030303130323033\",NULL}"`, "null"},
	"json[]":                        {`"{\"{\\\"key\\\": \\\"value\\\"}\",NULL}"`, `"{\"{\\\"key\\\": {\\\"key2\\\": \\\"value\\\"}}\",NULL}"`, "null"}, // nolint
	"jsonb[]":                       {`"{\"{\\\"key\\\": \\\"value\\\"}\",NULL}"`, "null"},
	"uuid[]":                        {"\"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL}\"", "null"},
	"date[]":                        {"\"{2021-03-01,NULL}\"", "null"},
	"time with time zone[]":         {"\"{12:45:01+08,NULL}\"", "null"},
	"time without time zone[]":      {"\"{12:45:01,NULL}\"", "null"},
	"timestamp with time zone[]":    {`"{\"2021-03-01 12:45:01+08\",NULL}"`, "null"},
	"timestamp without time zone[]": {`"{\"2021-03-01 12:45:01\",NULL}"`, "null"},
	"interval[]":                    {`"{\"1 day\",\"2 mons\",\"21 days\",05:00:00,\"-17 days\",NULL}"`, "null"},
}
