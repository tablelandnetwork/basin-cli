package app

import (
	"fmt"
	"strings"
)

const (
	jsonNULL = "null"
	pgNULL   = "NULL"
)

func removeDoubleQuotes(s string) string {
	return strings.ReplaceAll(s, "\"", "")
}

func removeBackslashes(s string) string {
	return strings.ReplaceAll(s, "\\", "")
}

func removeOuterChars(s string) string {
	return s[1 : len(s)-1]
}

func replaceDoubleWithSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "\"", "'")
}

func wrapSingleQuotes(s string) string {
	if s == jsonNULL {
		return s
	}
	return fmt.Sprintf("'%s'", s)
}

func createBoolListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		switch v {
		case "t":
			vals = append(vals, "true")
		case "f":
			vals = append(vals, "false")
		case pgNULL:
			vals = append(vals, "null")
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createNumericListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			vals = append(vals, v)
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createCharListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			vals = append(vals, wrapSingleQuotes(v))
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createByteListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeBackslashes(s)
	s = strings.ReplaceAll(s, "x", "") // remove hex prefix
	s = removeOuterChars(s)            // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			vals = append(vals, fmt.Sprintf("'%s'::BLOB", v))
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createJSONValue(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeOuterChars(s) // remove outer quotes
	s = removeBackslashes(s)
	return wrapSingleQuotes(s)
}

func createJSONListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeOuterChars(s) // remove outer quotes
	s = removeBackslashes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			v = v[1 : len(v)-1]
			vals = append(vals, wrapSingleQuotes(v))
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createUUIDListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			vals = append(vals, fmt.Sprintf("'%s'::UUID", v))
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createDateListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			vals = append(vals, wrapSingleQuotes(v))
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

func createTimestampListValues(s string) string {
	if s == jsonNULL {
		return s
	}

	s = removeDoubleQuotes(s)
	s = removeBackslashes(s)
	s = removeOuterChars(s) // remove array literals

	var vals []string
	for _, v := range strings.Split(s, ",") {
		if v == pgNULL {
			vals = append(vals, "null")
		} else {
			vals = append(vals, wrapSingleQuotes(v))
		}
	}

	return fmt.Sprintf("list_value(%s)", strings.Join(vals, ","))
}

// duckdbType is a type in duckdb. It contains the type name and a function
// that transforms a PG type into a duckdb type.
type duckdbType struct {
	typeName    string
	transformFn func(s string) (val string)
}

// typeConversionMap maps PG types to duckdb types.
// currently, only a subset of PG types are supported.
// Custom enum types, sturcts, multi-dimensional arrays are not supported.
var typeConversionMap = map[string]duckdbType{
	// boolean
	"boolean": {"boolean", removeDoubleQuotes},

	// numbers
	"bigint":           {"bigint", removeDoubleQuotes},
	"double precision": {"double", removeDoubleQuotes},
	"integer":          {"integer", removeDoubleQuotes},
	"numeric":          {"double", removeDoubleQuotes},
	"oid":              {"uinteger", removeDoubleQuotes},
	"real":             {"float", removeDoubleQuotes},
	"smallint":         {"smallint", removeDoubleQuotes},

	// misc
	"macaddr": {"varchar", replaceDoubleWithSingleQuotes},

	// bytes
	"bytea":             {"blob", replaceDoubleWithSingleQuotes},
	"\"char\"":          {"varchar", replaceDoubleWithSingleQuotes},
	"character":         {"varchar", replaceDoubleWithSingleQuotes},
	"character varying": {"varchar", replaceDoubleWithSingleQuotes},
	"bpchar":            {"varchar", replaceDoubleWithSingleQuotes},
	"json":              {"varchar", createJSONValue},
	"jsonb":             {"varchar", createJSONValue},
	"text":              {"varchar", replaceDoubleWithSingleQuotes},
	"uuid":              {"uuid", replaceDoubleWithSingleQuotes},

	// dates
	"date":                        {"date", replaceDoubleWithSingleQuotes},
	"time with time zone":         {"time with time zone", replaceDoubleWithSingleQuotes},
	"time without time zone":      {"time", replaceDoubleWithSingleQuotes},
	"timestamp with time zone":    {"timestamp with time zone", replaceDoubleWithSingleQuotes},
	"timestamp without time zone": {"timestamp", replaceDoubleWithSingleQuotes},
	"interval":                    {"interval", replaceDoubleWithSingleQuotes},

	// number arrays
	"boolean[]":          {"boolean[]", createBoolListValues},
	"bigint[]":           {"bigint[]", createNumericListValues},
	"double precision[]": {"double[]", createNumericListValues},
	"integer[]":          {"integer[]", createNumericListValues},
	"numeric[]":          {"integer[]", createNumericListValues},
	"real[]":             {"float[]", createNumericListValues},
	"smallint[]":         {"smallint[]", createNumericListValues},

	// byte arrays
	"\"char\"[]":          {"varchar[]", createCharListValues},
	"character[]":         {"varchar[]", createCharListValues},
	"character varying[]": {"varchar[]", createCharListValues},
	"bpchar[]":            {"varchar[]", createCharListValues},
	"text[]":              {"varchar[]", createCharListValues},
	"bytea[]":             {"blob[]", createByteListValues},
	"json[]":              {"varchar[]", createJSONListValues},
	"jsonb[]":             {"varchar[]", createJSONListValues},
	"uuid[]":              {"uuid[]", createUUIDListValues},

	// date arrays
	"date[]":                        {"date[]", createDateListValues},
	"time with time zone[]":         {"time with time zone[]", createDateListValues},
	"time without time zone[]":      {"time[]", createDateListValues},
	"timestamp with time zone[]":    {"timestamp with time zone[]", createTimestampListValues},
	"timestamp without time zone[]": {"timestamp[]", createTimestampListValues},
	"interval[]":                    {"interval[]", createTimestampListValues},
}
