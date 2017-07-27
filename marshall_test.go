package SqlMarshall

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
)

// Company structure is all the data for a company.  It is a paremeter
// and what is returned when looking up a record in the companies table
type ummarshalledStruct1 struct {
	FieldInt64            int64
	FieldString           string
	FieldFloat            float64
	ArrayInt              []int64
	ArrayInt3             [3]int64
	FieldInt64sqlNull     int64
	FieldStringsqlNull    string
	FieldFloatsqlNull     float64
	FieldMapString1Mapped string
	FieldStringNull       string
	FieldInt64Null        int64
	FieldStringNonNull    string
	FieldInt64NonNull     int64
}

type marshalledStruct1 struct {
	FieldInt64         int64
	FieldString        string
	FieldFloat         float64
	ArrayIntE0         int `map:"ArrayInt[0]"`
	ArrayIntE1         int `map:"ArrayInt[1]"`
	ArrayInt3E0        int `map:"ArrayInt3[0]"`
	ArrayInt3E1        int `map:"ArrayInt3[1]"`
	ArrayInt3E2        int `map:"ArrayInt3[2]"`
	FieldInt64sqlNull  sql.NullInt64
	FieldStringsqlNull sql.NullString
	FieldFloatsqlNull  sql.NullFloat64
	FieldMapString1    sql.NullString `map:"FieldMapString1Mapped"`
	FieldStringNull    sql.NullString `null:"X"`
	FieldInt64Null     sql.NullInt64  `null:"-1"`
	FieldStringNonNull sql.NullString `null:"X"`
	FieldInt64NonNull  sql.NullInt64  `null:"-1"`
}

func TestUnMarshall(t *testing.T) {
	unms1 := ummarshalledStruct1{
		FieldInt64:            99,
		FieldString:           "XYZZY",
		FieldFloat:            66.6,
		ArrayInt:              []int64{23, 45, 67, 89},
		ArrayInt3:             [3]int64{55, 66, 77},
		FieldInt64sqlNull:     101,
		FieldStringsqlNull:    "Hello",
		FieldFloatsqlNull:     -3.33,
		FieldMapString1Mapped: "Mapped",
		FieldStringNull:       "Astring",
		FieldInt64Null:        266,
		FieldStringNonNull:    "NotherString",
		FieldInt64NonNull:     404,
	}
	ms1 := marshalledStruct1{
		FieldInt64:         1,
		FieldString:        "Good",
		FieldFloat:         2.2,
		ArrayIntE0:         30,
		ArrayIntE1:         31,
		ArrayInt3E0:        40,
		ArrayInt3E1:        41,
		ArrayInt3E2:        42,
		FieldInt64sqlNull:  sql.NullInt64{Int64: 5, Valid: true},
		FieldStringsqlNull: sql.NullString{String: "Nope", Valid: true},
		FieldFloatsqlNull:  sql.NullFloat64{Float64: 6.60, Valid: true},
		FieldMapString1:    sql.NullString{String: "GoodString", Valid: true},
		FieldStringNull:    sql.NullString{String: "Nope", Valid: false},
		FieldInt64Null:     sql.NullInt64{Int64: 7, Valid: false},
		FieldStringNonNull: sql.NullString{String: "Nope", Valid: true},
		FieldInt64NonNull:  sql.NullInt64{Int64: 8, Valid: true},
	}
	type args struct {
		source interface{}
		dest   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Test1",
			args{source: ms1, dest: &unms1},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnMarshall(tt.args.source, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("UnMarshallDBStruct() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldInt64", unms1.FieldInt64, 1)
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldString", unms1.FieldString, "Good")
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldFloat", unms1.FieldFloat, 2.2)
	expectVal(t, "UnMarshallDBStruct", "unms1.ArrayInt[0]", unms1.ArrayInt[0], 30)
	expectVal(t, "UnMarshallDBStruct", "unms1.ArrayInt[1]", unms1.ArrayInt[1], 31)
	expectVal(t, "UnMarshallDBStruct", "unms1.ArrayInt3[0]", unms1.ArrayInt3[0], 40)
	expectVal(t, "UnMarshallDBStruct", "unms1.ArrayInt3[1]", unms1.ArrayInt3[1], 41)
	expectVal(t, "UnMarshallDBStruct", "unms1.ArrayInt3[2]", unms1.ArrayInt3[2], 42)
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldInt64sqlNull", unms1.FieldInt64sqlNull, 5)
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldStringsqlNull", unms1.FieldStringsqlNull, "Nope")
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldFloatsqlNull", unms1.FieldFloatsqlNull, 6.60)
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldMapString1Mapped", unms1.FieldMapString1Mapped, "GoodString")
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldStringNull", unms1.FieldStringNull, "X")
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldInt64Null", unms1.FieldInt64Null, -1)
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldStringNonNull", unms1.FieldStringNonNull, "Nope")
	expectVal(t, "UnMarshallDBStruct", "unms1.FieldInt64NonNull", unms1.FieldInt64NonNull, 8)
}
func expectVal(t *testing.T, errfunc string, msg string, v1 interface{}, v2 interface{}) {
	if strings.Compare(fmt.Sprintf("%+v", v1), fmt.Sprintf("%+v", v2)) != 0 {
		t.Errorf("%+v() %+v expected %+v but got %+v", errfunc, msg, v2, v1)
	}
}
func TestFieldByPath(t *testing.T) {
	type args struct {
		v    reflect.Value
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    reflect.Value
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FieldByPath(tt.args.v, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("FieldByPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FieldByPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMarshall(t *testing.T) {
	type args struct {
		source interface{}
		dest   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Marshall(tt.args.source, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("Marshall() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCopyInterface(t *testing.T) {
	type args struct {
		destValue reflect.Value
		src       interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CopyInterface(tt.args.destValue, tt.args.src); (err != nil) != tt.wantErr {
				t.Errorf("CopyInterface() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_strconvErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := strconvErr(tt.args.err); (err != nil) != tt.wantErr {
				t.Errorf("strconvErr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_cloneBytes(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cloneBytes(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cloneBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_asString(t *testing.T) {
	type args struct {
		src interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := asString(tt.args.src); got != tt.want {
				t.Errorf("asString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_asBytes(t *testing.T) {
	type args struct {
		buf []byte
		rv  reflect.Value
	}
	tests := []struct {
		name   string
		args   args
		wantB  []byte
		wantOk bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotB, gotOk := asBytes(tt.args.buf, tt.args.rv)
			if !reflect.DeepEqual(gotB, tt.wantB) {
				t.Errorf("asBytes() gotB = %v, want %v", gotB, tt.wantB)
			}
			if gotOk != tt.wantOk {
				t.Errorf("asBytes() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestInsertRecord(t *testing.T) {
	type args struct {
		db        *sqlx.DB
		tableName string
		record    interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    sql.Result
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InsertRecord(tt.args.db, tt.args.tableName, tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InsertRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMarshallInsertRecord(t *testing.T) {
	type args struct {
		db         *sqlx.DB
		table      string
		data       interface{}
		dbTemplate interface{}
	}
	tests := []struct {
		name      string
		args      args
		wantResID int64
		wantErr   bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResID, err := MarshallInsertRecord(tt.args.db, tt.args.table, tt.args.data, tt.args.dbTemplate)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshallInsertRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResID != tt.wantResID {
				t.Errorf("MarshallInsertRecord() = %v, want %v", gotResID, tt.wantResID)
			}
		})
	}
}
