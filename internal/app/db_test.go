package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	//"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

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
	{Name: "char_col", Typ: "char[]", IsNull: true, IsPrimary: false},
	{Name: "bpchar_col", Typ: "character[]", IsNull: true, IsPrimary: false},
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
	// custom enum type
	{Name: "enum_col", Typ: "enum_type_foo[]", IsNull: true, IsPrimary: false},
}

var dateArrayTypeCols = []Column{
	{Name: "date_col", Typ: "date[]", IsNull: true, IsPrimary: false},
	{Name: "time_col", Typ: "time without time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timetz_col", Typ: "time with time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timestamp_col", Typ: "timestamp without time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timestamptz_col", Typ: "timestamp with time zone[]", IsNull: true, IsPrimary: false},
}

var multidiemnsionalArrayTypeCols = []Column{
	{Name: "i", Typ: "integer[]", IsNull: true, IsPrimary: false},
	{Name: "s", Typ: "character varying[]", IsNull: true, IsPrimary: false},
}

func TestGenCreateQuery(t *testing.T) {
	testCases := []struct {
		tableName          string
		cols               []Column
		expectedCreateStmt string
	}{
		{
			"number_types",
			numTypeCols,
			`CREATE TABLE IF NOT EXISTS number_types (
					bool_col boolean,
					smallint_col smallint,
					integer_col integer,
					bigint_col bigint,
					float_col float,
					double_col double,
					decimal_col double,
					udecimal_col double
				)`,
		},
		{
			"byte_types",
			byteTypeCols,
			`CREATE TABLE IF NOT EXISTS byte_types (
					char_default varchar,
					char_1_col varchar,
					char_9_col varchar,
					varchar_1_col varchar,
					varchar_9_col varchar,
					text_col varchar,
					blob_col blob,
					json_col_old varchar,
					json_col_new varchar,
					uuid_col uuid
				)`,
		},
		{
			"date_types",
			dateTypeCols,
			`CREATE TABLE IF NOT EXISTS date_types (
					date_col date,
					time_col time,
					timetz_col time with time zone,
					timestamp_col timestamp,
					timestamptz_col timestamp with time zone
				)`,
		},
		{
			"num_array_types",
			numArrayTypeCols,
			`CREATE TABLE IF NOT EXISTS num_array_types (
					bool_col boolean[],
					smallint_col smallint[],
					integer_col integer[],
					bigint_col bigint[],
					float_col float[],
					double_col double[],
					numeric_col double[],
					unumeric_col double[]
				)`,
		},
		{
			"byte_array_types",
			byteArrayTypeCols,
			`CREATE TABLE IF NOT EXISTS byte_array_types (
					char_col varchar[],
					bpchar_col varchar[],
					varchar_col varchar[],
					uvarchar_col varchar[],
					text_col varchar[],
					blob_col blob[],
					json_col varchar[],
					uuid_col uuid[]
				)`,
		},
		{
			"date_array_types",
			dateArrayTypeCols,
			`CREATE TABLE IF NOT EXISTS date_array_types (
					date_col date[],
					time_col time[],
					timetz_col time with time zone[],
					timestamp_col timestamp[],
					timestamptz_col timestamp with time zone[]
				)`,
		},
		{
			"multidimensional_array_types",
			multidiemnsionalArrayTypeCols,
			`CREATE TABLE IF NOT EXISTS multidimensional_array_types (
					i integer[],
					s varchar[]
				)`,
		},
		{
			"mac_addr_types",
			macaddrTypeCols,
			`CREATE TABLE IF NOT EXISTS mac_addr_types (
					macaddr_col varchar
				)`,
		},
	}

	for _, tc := range testCases {
		dbm := NewDBManager(
			t.TempDir(), tc.tableName, tc.cols, 3*time.Second, nil)
		query, err := dbm.genCreateQuery()
		require.NoError(t, err)

		// remove query formatting before comparison
		tc.expectedCreateStmt = strings.ReplaceAll(tc.expectedCreateStmt, "\n", "")
		tc.expectedCreateStmt = strings.ReplaceAll(tc.expectedCreateStmt, "\t", "")

		// assert correct create statement
		require.Equal(t, tc.expectedCreateStmt, query)

		// assert statement is correctly applied
		require.NoError(t, dbm.NewDB(context.Background()))

		_, err = dbm.db.Exec(query)
		require.NoError(t, err)
	}

}

func TestGenCreateQueryUnsupported(t *testing.T) {
	testCases := []struct {
		tableName   string
		cols        []Column
		expectedErr error
	}{
		{
			"enum_array_types",
			enumArrayTypeCols,
			errors.New("unsupported type: enum_type_foo[]"),
		},
	}

	for _, tc := range testCases {
		dbm := NewDBManager(
			t.TempDir(), tc.tableName, tc.cols, 3*time.Second, nil)
		_, err := dbm.genCreateQuery()
		fmt.Println(err)
		require.EqualError(t, err, tc.expectedErr.Error())
	}

}

var multidiemnsionalArrayTypeInsert = `
INSERT INTO multidimensional_arrays VALUES (
	ARRAY[ARRAY[[1, 2, 3]], ARRAY[[4, 5, 6]], ARRAY[[7, 8, 9]]],
	ARRAY[ARRAY['hello world', 'abc'], ARRAY['this is', 'an array']]);
`

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

var supportedTypes = map[string][]string{
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
	"interval":                      {"\"1 year\"", "\"2 mons\"", "\"21 days\"", "\"05:00:00\"", "\"-00:00:07\"", "\"1 year 2 mons 21 days 05:00:00\"", "\"-17 days\"", "null"},
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
	"json[]":                        {`"{\"{\\\"key\\\": \\\"value\\\"}\",NULL}"`, "null"},
	"jsonb[]":                       {`"{\"{\\\"key\\\": \\\"value\\\"}\",NULL}"`, "null"},
	"uuid[]":                        {"\"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL}\"", "null"},
	"date[]":                        {"\"{2021-03-01,NULL}\"", "null"},
	"time with time zone[]":         {"\"{12:45:01+08,NULL}\"", "null"},
	"time without time zone[]":      {"\"{12:45:01,NULL}\"", "null"},
	"timestamp with time zone[]":    {`"{\"2021-03-01 12:45:01+08\",NULL}"`, "null"},
	"timestamp without time zone[]": {`"{\"2021-03-01 12:45:01\",NULL}"`, "null"},
	"interval[]":                    {`"{\"1 day\",\"2 mons\",\"21 days\",05:00:00,\"-17 days\",NULL}"`, "null"},
}

func assertInsertQuery(t *testing.T, tx pgrepl.Tx, dbm *DBManager) {
	createQuery, err := dbm.genCreateQuery()
	require.NoError(t, err)
	require.NoError(t, dbm.NewDB(context.Background()))
	_, err = dbm.db.Exec(createQuery)
	require.NoError(t, err)

	insertQuery, err := dbm.queryFromWAL(&tx)
	require.NoError(t, err)

	_, err = dbm.db.Exec(insertQuery)
	require.NoError(t, err)

}

func TestQueryFromWAL(t *testing.T) {
	pgtypes := []string{}
	for pgtype := range supportedTypes {
		pgtypes = append(pgtypes, pgtype)
	}
	for _, typ := range pgtypes {
		for _, val := range supportedTypes[typ] {
			colsJSON := fmt.Sprintf(wal, typ, val)
			var tx pgrepl.Tx
			require.NoError(
				t, json.Unmarshal([]byte(colsJSON), &tx))

			valIsNull := val == "null"
			cols := []Column{
				{Name: "id", Typ: typ, IsNull: valIsNull, IsPrimary: false},
			}
			dbm := NewDBManager(
				t.TempDir(), "t", cols, 3*time.Second, nil)
			assertInsertQuery(t, tx, dbm)
		}
	}
}
