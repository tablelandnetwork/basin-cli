package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

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
					numeric_col integer[],
					unumeric_col integer[]
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
			"mac_addr_types",
			macaddrTypeCols,
			`CREATE TABLE IF NOT EXISTS mac_addr_types (
					macaddr_col varchar
				)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tableName, func(t *testing.T) {
			dbm := NewDBManager(
				t.TempDir(), []TableSchema{{tc.tableName, tc.cols}}, 3*time.Second, nil)
			query, err := dbm.genCreateQuery()
			require.NoError(t, err)

			// remove query formatting before comparison
			tc.expectedCreateStmt = strings.ReplaceAll(tc.expectedCreateStmt, "\n", "")
			tc.expectedCreateStmt = strings.ReplaceAll(tc.expectedCreateStmt, "\t", "")

			// assert correct create statement
			require.Equal(t, tc.expectedCreateStmt, query)
		})
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
		{
			"custom_composite_types",
			customCompositeTypeCols,
			errors.New("unsupported type: USER-DEFINED"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tableName, func(t *testing.T) {
			dbm := NewDBManager(
				t.TempDir(), []TableSchema{{tc.tableName, tc.cols}}, 3*time.Second, nil)
			_, err := dbm.genCreateQuery()
			require.EqualError(t, err, tc.expectedErr.Error())
		})
	}
}

func TestQueryFromWAL(t *testing.T) {
	testCases := []struct {
		typ                 string
		vals                []string
		expectedInsertStmts []string
	}{
		{
			"boolean",
			[]string{"true", "false", "null"},
			[]string{
				"insert into t (id) values (true)",
				"insert into t (id) values (false)",
				"insert into t (id) values (null)",
			},
		},
		{
			"bigint",
			[]string{"42", "-42", "null"},
			[]string{
				"insert into t (id) values (42)",
				"insert into t (id) values (-42)",
				"insert into t (id) values (null)",
			},
		},
		{
			"double precision",
			[]string{"42.01", "-42.01", "null"},
			[]string{
				"insert into t (id) values (42.01)",
				"insert into t (id) values (-42.01)",
				"insert into t (id) values (null)",
			},
		},
		{
			"integer",
			[]string{"42", "-42", "null"},
			[]string{
				"insert into t (id) values (42)",
				"insert into t (id) values (-42)",
				"insert into t (id) values (null)",
			},
		},
		{
			"numeric(4, 7)",
			[]string{"42.01", "-42.01", "null"},
			[]string{
				"insert into t (id) values (42.01)",
				"insert into t (id) values (-42.01)",
				"insert into t (id) values (null)",
			},
		},
		{
			"real",
			[]string{"42.01", "-42.01", "null"},
			[]string{
				"insert into t (id) values (42.01)",
				"insert into t (id) values (-42.01)",
				"insert into t (id) values (null)",
			},
		},
		{
			"smallint",
			[]string{"42", "-42", "null"},
			[]string{
				"insert into t (id) values (42)",
				"insert into t (id) values (-42)",
				"insert into t (id) values (null)",
			},
		},
		{
			"oid",
			[]string{"42.42", "null"},
			[]string{
				"insert into t (id) values (42.42)",
				"insert into t (id) values (null)",
			},
		},
		{
			"macaddr",
			[]string{"\"08:00:2b:01:02:03\"", "null"},
			[]string{
				"insert into t (id) values ('08:00:2b:01:02:03')",
				"insert into t (id) values (null)",
			},
		},
		{
			"bytea",
			[]string{"\"00010203\"", "null"},
			[]string{
				"insert into t (id) values ('00010203')",
				"insert into t (id) values (null)",
			},
		},
		{
			"bpchar",
			[]string{"\"a\"", "\"Z\"", "null"},
			[]string{
				"insert into t (id) values ('a')",
				"insert into t (id) values ('Z')",
				"insert into t (id) values (null)",
			},
		},
		{
			`\"char\"`,
			[]string{"\"a\"", "\"Z\"", "null"},
			[]string{
				"insert into t (id) values ('a')",
				"insert into t (id) values ('Z')",
				"insert into t (id) values (null)",
			},
		},
		{
			"character(1)",
			[]string{"\"a\"", "\"Z\"", "null"},
			[]string{
				"insert into t (id) values ('a')",
				"insert into t (id) values ('Z')",
				"insert into t (id) values (null)",
			},
		},
		{
			"character(5)",
			[]string{"\"aaaaa\"", "\"ZZZZZ\"", "null"},
			[]string{
				"insert into t (id) values ('aaaaa')",
				"insert into t (id) values ('ZZZZZ')",
				"insert into t (id) values (null)",
			},
		},
		{
			"character varying",
			[]string{"\"a\"", "\"Zzzzzzzz\"", "null"},
			[]string{
				"insert into t (id) values ('a')",
				"insert into t (id) values ('Zzzzzzzz')",
				"insert into t (id) values (null)",
			},
		},
		{
			"character varying(5)",
			[]string{"\"aaaaa\"", "\"ZZZZZ\"", "null"},
			[]string{
				"insert into t (id) values ('aaaaa')",
				"insert into t (id) values ('ZZZZZ')",
				"insert into t (id) values (null)",
			},
		},
		{
			"json",
			[]string{`"{\"foo\": \"bar\"}"`, `"{\"foo\": {\"bar\": 3}}"`, "null"},
			[]string{
				`insert into t (id) values ('{"foo": "bar"}')`,
				`insert into t (id) values ('{"foo": {"bar": 3}}')`,
				"insert into t (id) values (null)",
			},
		},
		{
			"jsonb",
			[]string{`"{\"foo\": \"bar\"}"`, `"{\"foo\": {\"bar\": 3}}"`, "null"},
			[]string{
				`insert into t (id) values ('{"foo": "bar"}')`,
				`insert into t (id) values ('{"foo": {"bar": 3}}')`,
				"insert into t (id) values (null)",
			},
		},
		{
			"text",
			[]string{"\"dpfkg\"", "null"},
			[]string{
				"insert into t (id) values ('dpfkg')",
				"insert into t (id) values (null)",
			},
		},
		{
			"uuid",
			[]string{"\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\"", "null"},
			[]string{
				"insert into t (id) values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')",
				"insert into t (id) values (null)",
			},
		},
		{
			"date",
			[]string{"\"2021-03-01\"", "null"},
			[]string{
				"insert into t (id) values ('2021-03-01')",
				"insert into t (id) values (null)",
			},
		},
		{
			"time with time zone",
			[]string{"\"12:00:00-08\"", "null"},
			[]string{
				"insert into t (id) values ('12:00:00-08')",
				"insert into t (id) values (null)",
			},
		},
		{
			"time without time zone",
			[]string{"\"12:45:01\"", "null"},
			[]string{
				"insert into t (id) values ('12:45:01')",
				"insert into t (id) values (null)",
			},
		},
		{
			"timestamp with time zone",
			[]string{"\"2021-03-01 12:45:01+08\"", "null"},
			[]string{
				"insert into t (id) values ('2021-03-01 12:45:01+08')",
				"insert into t (id) values (null)",
			},
		},
		{
			"timestamp without time zone",
			[]string{"\"2021-03-01 12:45:01\"", "null"},
			[]string{
				"insert into t (id) values ('2021-03-01 12:45:01')",
				"insert into t (id) values (null)",
			},
		},
		{
			"interval",
			[]string{"\"1 year\"", "\"2 mons\"", "\"21 days\"", "\"05:00:00\"", "\"-00:00:07\"", "\"1 year 2 mons 21 days 05:00:00\"", "\"-17 days\"", "null"}, // nolint
			[]string{
				"insert into t (id) values ('1 year')",
				"insert into t (id) values ('2 mons')",
				"insert into t (id) values ('21 days')",
				"insert into t (id) values ('05:00:00')",
				"insert into t (id) values ('-00:00:07')",
				"insert into t (id) values ('1 year 2 mons 21 days 05:00:00')",
				"insert into t (id) values ('-17 days')",
				"insert into t (id) values (null)",
			},
		},
		{
			"boolean[]",
			[]string{`"{t,f,NULL}"`, "null"},
			[]string{
				"insert into t (id) values (list_value(true,false,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"bigint[]",
			[]string{"\"{42,-42,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value(42,-42,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"double precision[]",
			[]string{"\"{42.01,-42.01,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value(42.01,-42.01,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"integer[]",
			[]string{"\"{42,-42,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value(42,-42,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"numeric[]",
			[]string{"\"{42.01,-42.01,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value(42.01,-42.01,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"real[]",
			[]string{"\"{42.01,-42.01,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value(42.01,-42.01,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"smallint[]",
			[]string{"\"{42,-42,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value(42,-42,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			`\"char\"[]`,
			[]string{"\"{a,Z,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('a','Z',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"character[]",
			[]string{"\"{a,Z,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('a','Z',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"character varying[]",
			[]string{"\"{a,Z,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('a','Z',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"text[]",
			[]string{"\"{dpfkg,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('dpfkg',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"bytea[]",
			[]string{`"{\"\\\\x3030303130323033\",NULL}"`, "null"},
			[]string{
				"insert into t (id) values (list_value('3030303130323033'::BLOB,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"json[]",
			[]string{`"{\"{\\\"key\\\": \\\"value\\\"}\",NULL}"`, "null"},
			[]string{
				`insert into t (id) values (list_value('{"key": "value"}',null))`,
				"insert into t (id) values (null)",
			},
		},
		{
			"jsonb[]",
			[]string{`"{\"{\\\"key\\\": \\\"value\\\"}\",NULL}"`, "null"},
			[]string{
				`insert into t (id) values (list_value('{"key": "value"}',null))`,
				"insert into t (id) values (null)",
			},
		},
		{
			"uuid[]",
			[]string{"\"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID,null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"date[]",
			[]string{"\"{2021-03-01,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('2021-03-01',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"time with time zone[]",
			[]string{"\"{12:45:01+08,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('12:45:01+08',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"time without time zone[]",
			[]string{"\"{12:45:01,NULL}\"", "null"},
			[]string{
				"insert into t (id) values (list_value('12:45:01',null))",
				"insert into t (id) values (null)",
			},
		},
		{
			"timestamp with time zone[]",
			[]string{`"{\"2021-03-01 12:45:01+08\",NULL}"`, "null"},
			[]string{
				`insert into t (id) values (list_value('2021-03-01 12:45:01+08',null))`,
				"insert into t (id) values (null)",
			},
		},
		{
			"timestamp without time zone[]",
			[]string{`"{\"2021-03-01 12:45:01\",NULL}"`, "null"},
			[]string{
				`insert into t (id) values (list_value('2021-03-01 12:45:01',null))`,
				"insert into t (id) values (null)",
			},
		},
		{
			"interval[]",
			[]string{`"{\"1 day\",\"2 mons\",\"21 days\",05:00:00,\"-17 days\",NULL}"`, "null"},
			[]string{
				`insert into t (id) values (list_value('1 day','2 mons','21 days','05:00:00','-17 days',null))`,
				"insert into t (id) values (null)",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.typ, func(t *testing.T) {
			for i, val := range tc.vals {
				colsJSON := fmt.Sprintf(wal, tc.typ, val)
				var tx pgrepl.Tx
				require.NoError(
					t, json.Unmarshal([]byte(colsJSON), &tx))

				valIsNull := val == "null"
				cols := []Column{
					{Name: "id", Typ: tc.typ, IsNull: valIsNull, IsPrimary: false},
				}
				dbm := NewDBManager(
					t.TempDir(), []TableSchema{{"t", cols}}, 3*time.Second, nil)
				insertQuery, err := dbm.queryFromWAL(&tx)
				require.NoError(t, err)
				require.Equal(t, tc.expectedInsertStmts[i], insertQuery)
			}
		})
	}
}

func TestQueryFromWALUnsupported(t *testing.T) {
	testCases := []struct {
		typ         string
		vals        []string
		expectedErr error
	}{
		{
			"public.enum_type_foo[]",
			[]string{"\"{}\"", "\"{foo,bar,baz}\"", "\"{foo,NULL}\"", "null"},
			errors.New("unsupported type: public.enum_type_foo[]"),
		},
		{
			"public.composite_type",
			[]string{"\"(foo,42,42.01)\"", "null"},
			errors.New("unsupported type: public.composite_type"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.typ, func(t *testing.T) {
			for _, val := range tc.vals {
				colsJSON := fmt.Sprintf(wal, tc.typ, val)
				var tx pgrepl.Tx
				require.NoError(
					t, json.Unmarshal([]byte(colsJSON), &tx))

				valIsNull := val == "null"
				cols := []Column{
					{Name: "id", Typ: tc.typ, IsNull: valIsNull, IsPrimary: false},
				}
				dbm := NewDBManager(
					t.TempDir(), []TableSchema{{"t", cols}}, 3*time.Second, nil)
				_, err := dbm.queryFromWAL(&tx)
				require.EqualError(t, err, tc.expectedErr.Error())
			}
		})
	}
}

func TestReplay(t *testing.T) {
	pgtypes := []string{}
	for pgtype := range supportedTypeVals {
		pgtypes = append(pgtypes, pgtype)
	}
	for _, typ := range pgtypes {
		t.Run(typ, func(t *testing.T) {
			for _, val := range supportedTypeVals[typ] {
				colsJSON := fmt.Sprintf(wal, typ, val)
				var tx pgrepl.Tx
				require.NoError(
					t, json.Unmarshal([]byte(colsJSON), &tx))

				valIsNull := val == jsonNULL
				cols := []Column{
					{Name: "id", Typ: typ, IsNull: valIsNull, IsPrimary: false},
				}
				// use a large window for testing
				dbm := NewDBManager(
					t.TempDir(), []TableSchema{{"t", cols}}, 3*time.Hour, nil)

				// assert new db setup (create queries are correctly applied)
				ctx := context.Background()
				require.NoError(t, dbm.NewDB(ctx))

				// assert replaying a transaction as insert query does not err
				require.NoError(t, dbm.Replay(ctx, &tx))
			}
		})
	}
}

func TestReplayUnsupported(t *testing.T) {
	typ := "integer[]" // unsupported multi-dimensional array.
	val := "\"{{1,2},{3,4}}\""
	colsJSON := fmt.Sprintf(wal, typ, val)
	var tx pgrepl.Tx
	require.NoError(
		t, json.Unmarshal([]byte(colsJSON), &tx))

	valIsNull := val == jsonNULL
	cols := []Column{
		{Name: "id", Typ: typ, IsNull: valIsNull, IsPrimary: false},
	}
	dbm := NewDBManager(
		t.TempDir(), []TableSchema{{"t", cols}}, 3*time.Hour, nil)
	// assert new db setup (create queries are correctly applied)
	ctx := context.Background()
	err := dbm.NewDB(ctx)
	require.NoError(t, err)

	// assert replaying a transaction gives error
	err = dbm.Replay(ctx, &tx)
	require.ErrorContains(t, err, errors.New("cannot replay WAL record").Error())
}
