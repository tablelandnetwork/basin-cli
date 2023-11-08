package app

import (
	"bufio"
	"context"
	"encoding/json"
	"os"

	//"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

func TestQueryFromWAL(t *testing.T) {
	dbm := NewDBManager(
		t.TempDir(), testTable, cols, 3*time.Second, nil)

	f, err := os.Open("testdata/wal.input")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()
	reader := bufio.NewReader(f)

	wal1, _, err := reader.ReadLine()
	require.NoError(t, err)

	var tx pgrepl.Tx
	require.NoError(t, json.Unmarshal(wal1, &tx))

	expected := "insert into t (id, name) values (200232, 100), (200242, 400)"
	require.Equal(t, expected, dbm.queryFromWAL(&tx))
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
	{Name: "char_col", Typ: "char[]", IsNull: true, IsPrimary: false},
	{Name: "bpchar_col", Typ: "character[]", IsNull: true, IsPrimary: false},
	{Name: "varchar_col", Typ: "character varying[]", IsNull: true, IsPrimary: false},
	{Name: "uvarchar_col", Typ: "character varying[]", IsNull: true, IsPrimary: false},
	{Name: "text_col", Typ: "text[]", IsNull: true, IsPrimary: false},
	{Name: "blob_col", Typ: "bytea[]", IsNull: true, IsPrimary: false},
	{Name: "json_col", Typ: "json[]", IsNull: true, IsPrimary: false},
	{Name: "uuid_col", Typ: "uuid[]", IsNull: true, IsPrimary: false},
}

/*
column_name   |           data_type           | is_nullable | is_primary
-----------------+-------------------------------+-------------+------------
 date_col        | date[]                        | t           | f
 time_col        | time without time zone[]      | t           | f
 timetz_col      | time with time zone[]         | t           | f
 timestamp_col   | timestamp without time zone[] | t           | f
 timestamptz_col | timestamp with time zone[]    | t           | f
*/

var dateArrayTypeCols = []Column{
	{Name: "date_col", Typ: "date[]", IsNull: true, IsPrimary: false},
	{Name: "time_col", Typ: "time without time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timetz_col", Typ: "time with time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timestamp_col", Typ: "timestamp without time zone[]", IsNull: true, IsPrimary: false},
	{Name: "timestamptz_col", Typ: "timestamp with time zone[]", IsNull: true, IsPrimary: false},
}

var numberTypesInsert = `
INSERT INTO number_types
	(bool_col, smallint_col, integer_col, bigint_col, float_col, double_col, decimal_col, udecimal_col)
VALUES
	(false, 0, 0, 0, 0, 0, 0, 0),
	(false, -42, -42, -42, -42.01, -42.01, -42.01, -42.01),
	(true, 42, 42, 42, 42.01, 42.01, 42.01, 42.01),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

var byteTypesInsert = `
INSERT INTO byte_types
	(char_default, char_1_col, char_9_col, varchar_1_col, varchar_9_col, text_col, blob_col, json_col_old, uuid_col)
VALUES
	('a', 'a', '', '', '', '', '',  '42', '00000000-0000-0000-0000-000000000000'),
	('a', 'a', 'aaaaaaaaa', 'a', 'aaaaaaaaa', 'dpfkg', 'dpfkg', '{"a":42}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
	('Z', 'Z', 'ZZZZZZZZZ', 'Z', 'ZZZZZZZZZ', 'dpfkg', 'dpfkg', '{"a":42}',  'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

var dateTypeInsert = `
INSERT INTO date_types 
	(date_col, time_col, timetz_col, timestamp_col, timestamptz_col)
VALUES 
	('2021-03-01', '12:45:01', '12:45:01', '2021-03-01T12:45:01', '2021-03-01T12:45:01'),
	(NULL, NULL, NULL, NULL, NULL);
`

var numArrayTypeInsert = `
INSERT INTO num_array_types 
	(bool_col, smallint_col, integer_col, bigint_col, float_col, double_col, numeric_col, unumeric_col)
VALUES
	(list_value(true, false, NULL), list_value(-42, 42, NULL), list_value(-4200, 4200, NULL), list_value(-420000, 420000, NULL), list_value(-4.2, 4.2), list_value(-4.2, 4.2), list_value(-4.2, 4.2), list_value(-4.2, 4.2)),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

var byteArrayTypeInsert = `
insert into byte_array_types
	(char_col, bpchar_col , varchar_col, uvarchar_col, text_col, blob_col , json_col , uuid_col)
VALUES (
	list_value('a', 'Z', NULL), list_value('a', 'Z', NULL), list_value('aaaa', 'ZZZZ', NULL), list_value('aaaa', 'ZZZZ', NULL), list_value('aaaa', 'ZZZZ', NULL), list_value('\x00'::BLOB, '\xff'::BLOB, NULL), list_value('{"a":42}', NULL), list_value('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL)),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

var dateArrayTypeInsert = `
insert into date_array_types
	(date_col, time_col,timetz_col, timestamp_col, timestamptz_col)
VALUES
	(list_value('2019-11-26', '2021-03-01', NULL),list_value('14:42:43', '12:45:01', NULL),list_value('14:42:43', '12:45:01', NULL),list_value('2019-11-26T12:45:01', '2021-03-01T12:45:01', NULL),list_value('2019-11-26T12:45:01', '2021-03-01T12:45:01', NULL)),
	(NULL, NULL, NULL, NULL, NULL);
`

func TestGenCreateQuery(t *testing.T) {
	testCases := []struct {
		tableName string
		cols      []Column
		insertEx  string
		expected  string
	}{
		{
			"number_types",
			numTypeCols,
			numberTypesInsert,
			"CREATE TABLE IF NOT EXISTS number_types (bool_col boolean,smallint_col smallint,integer_col integer,bigint_col bigint,float_col real,double_col double precision,decimal_col numeric,udecimal_col numeric)",
		},
		{
			"byte_types",
			byteTypeCols,
			byteTypesInsert,
			"CREATE TABLE IF NOT EXISTS byte_types (char_default character,char_1_col character,char_9_col character,varchar_1_col character varying,varchar_9_col character varying,text_col text,blob_col bytea,json_col_old varchar,json_col_new varchar,uuid_col uuid)",
		},
		{
			"date_types",
			dateTypeCols,
			dateTypeInsert,
			"CREATE TABLE IF NOT EXISTS date_types (date_col date,time_col time without time zone,timetz_col time with time zone,timestamp_col timestamp without time zone,timestamptz_col timestamp with time zone)",
		},
		{
			"num_array_types",
			numArrayTypeCols,
			numArrayTypeInsert,
			"CREATE TABLE IF NOT EXISTS num_array_types (bool_col boolean[],smallint_col smallint[],integer_col integer[],bigint_col bigint[],float_col real[],double_col double precision[],numeric_col numeric[],unumeric_col numeric[])",
		},
		{
			"byte_array_types",
			byteArrayTypeCols,
			byteArrayTypeInsert,
			"CREATE TABLE IF NOT EXISTS byte_array_types (char_col char[],bpchar_col character[],varchar_col character varying[],uvarchar_col character varying[],text_col text[],blob_col bytea[],json_col varchar[],uuid_col uuid[])",
		},
		{
			"date_array_types",
			dateArrayTypeCols,
			dateArrayTypeInsert,
			"CREATE TABLE IF NOT EXISTS date_array_types (date_col date[],time_col time without time zone[],timetz_col time with time zone[],timestamp_col timestamp without time zone[],timestamptz_col timestamp with time zone[])",
		},
	}

	for _, tc := range testCases {
		dbm := NewDBManager(
			t.TempDir(), tc.tableName, tc.cols, 3*time.Second, nil)
		query, err := dbm.genCreateQuery()
		require.NoError(t, err)
		require.Equal(t, tc.expected, query)
		dbm.NewDB(context.Background())
		_, err = dbm.db.Exec(query)
		require.NoError(t, err)
		_, err = dbm.db.Exec(tc.insertEx)
		require.NoError(t, err)
		_, err = dbm.db.Query("select * from " + tc.tableName)
		require.NoError(t, err)
	}

}
