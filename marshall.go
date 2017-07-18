package SqlMarshall

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

//ErrBadTableName used in case of illegal characters in the table definition in GetTableLength
var ErrBadTableName = errors.New("The table name inputted does not contain legal characters")

var errNilPtr = errors.New("destination pointer is nil") // embedded in descriptive error

// UnMarshall copies the values from a database structure into an internal representation structure
// using the annotations on the database structure to indicate which field to copy to.  This maps
// null fields from the database (stored in the sql.NullString... structures) into an appropriate null value.
func UnMarshall(source interface{}, dest interface{}) error {
	// We need both the Value (for the source values) and the Type (to get structure and annotations) of the source.
	sourceValue := reflect.Indirect(reflect.ValueOf(source))
	sourceType := sourceValue.Type()
	// For the dest we just need the Value structure.
	destValue := reflect.ValueOf(dest)
	// Make sure we have a pointer to the structure
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("Destination is not a pointer")
	}
	// Dereference the pointer to get to the raw type
	destValue = destValue.Elem()
	// Sanity check to make sure we are working with structures
	if sourceType.Kind() != reflect.Struct {
		return fmt.Errorf("Source is not a struct")
	}
	if destValue.Kind() != reflect.Struct {
		return fmt.Errorf("Destination is not a pointer to a struct")
	}
	// We have a structure, so iterate through all the fields in the structure
	for fieldNum := 0; fieldNum < sourceType.NumField(); fieldNum++ {
		// For the source field we need the Value and the Type
		fieldValue := sourceValue.Field(fieldNum)
		fieldType := sourceType.Field(fieldNum)
		// Are we using an alternate name to save into or the same name
		mappedName, hasMappedName := fieldType.Tag.Lookup("map")
		if !hasMappedName {
			mappedName = fieldType.Name
		}
		// Check to see if they are indexing the field
		// This will be indicated by a mapped name like NAICSCode[1]
		// in which case we need to parse out the index.
		// NOTE: we don't require the closing ] but we need to make sure that the index is a valid number.
		strslice := strings.Split(mappedName, "[")
		// Assume we don't have an index
		hasindex := false
		sliceindex := 0
		// Unless we actually have something after the []
		if len(strslice) > 1 {
			// Get rid of any trailing ] and parse the number as an integer
			piece2 := strings.Split(strslice[1], "]")
			newspot, err := strconv.Atoi(piece2[0])
			// As long as it parses correctly, we will use that index and clean up the mapped name
			if err == nil {
				sliceindex = newspot
				hasindex = true
				mappedName = strslice[0]
			}
		}
		// Find any default custom nil value.
		nullDefault, hasNullDefault := fieldType.Tag.Lookup("null")

		// Figure out our source value type and look for the special cases of the special sql null items
		sourceInterface := fieldValue.Interface()
		// Make sure it is something we can actually manipulate
		if !fieldValue.IsValid() {
			return fmt.Errorf("Field %+v is not valid", mappedName)
		}

		switch sourceInterface.(type) {
		case sql.NullString:
			// Default is "" unless they override
			if sourceInterface.(sql.NullString).Valid {
				sourceInterface = sourceInterface.(sql.NullString).String
			} else if hasNullDefault {
				sourceInterface = nullDefault
			} else {
				sourceInterface = ""
			}
			// Commented out because a NullBool really doesn't make any sense
			//		case sql.NullBool:
			//			// Default is False unless they override with the string "true"
			//			if sourceInterface.(sql.NullString).Valid {
			//				sourceInterface = sourceInterface.(sql.NullBool).Bool
			//			} else if strings.EqualFold(nullDefault, "true") {
			//				sourceInterface = true
			//			} else {
			//				sourceInterface = false
			//			}
		case sql.NullInt64:
			// Default is 0 unless they override with a value
			if sourceInterface.(sql.NullInt64).Valid {
				sourceInterface = sourceInterface.(sql.NullInt64).Int64
			} else {
				sourceInterface = 0
				// If they gave us a null value, make sure it parses as a number, otherwise we will
				// stick with the default override.
				if hasNullDefault {
					altValue, err := strconv.ParseInt(nullDefault, 10, 64)
					if err == nil {
						sourceInterface = altValue
					}
				}
			}
		case sql.NullFloat64:
			// Default is 0.0 unless they override with a value
			if sourceInterface.(sql.NullFloat64).Valid {
				sourceInterface = sourceInterface.(sql.NullFloat64).Float64
			} else {
				// If they gave us a null value, make sure it parses as a float, otherwise we will
				// stick with the default override.
				sourceInterface = 0.0
				if hasNullDefault {
					altValue, err := strconv.ParseFloat(nullDefault, 64)
					if err == nil {
						sourceInterface = altValue
					}
				}
			}
		}
		//
		// Figure out what we are going to write to
		//
		destFieldValue, err := FieldByPath(destValue, mappedName)
		if err != nil {
			return err
		}
		if !destFieldValue.IsValid() {
			return fmt.Errorf("Mapped Field %+v is not valid for %+v", mappedName, fieldType.Name)
		}
		// We have the output field we want to write to.
		// Check to see if we are going into an array for the output
		if hasindex && (destFieldValue.Kind() == reflect.Array || destFieldValue.Kind() == reflect.Slice) {
			// Make sure that the array index is in bounds
			if sliceindex >= destFieldValue.Len() {
				if destFieldValue.Kind() == reflect.Array {
					// it is an array, and what they gave us is out of bounds so we have to generate an error
					return fmt.Errorf("Mapped Field %+v[%+v] is out of bounds (>%+v) for %+v",
						mappedName, sliceindex, destFieldValue.Len(), fieldType.Name)
				}
				zeroElem := reflect.Zero(destFieldValue.Type().Elem())
				// We have a slice, we need to extend the slice
				for destFieldValue.Len() <= sliceindex {
					destFieldValue.Set(reflect.Append(destFieldValue, zeroElem))
				}
			}
			destFieldValue = destFieldValue.Index(sliceindex)
		}
		err = CopyInterface(destFieldValue, sourceInterface)
		if err != nil {
			return err
		}
	}
	return nil
}

// FieldByPath returns the struct field with the given path
// It returns the zero Value if no field was found.
// Note that if there is no period in the name then this is exactly
// the same as FieldMyName (except it doesn't panic)
func FieldByPath(v reflect.Value, name string) (reflect.Value, error) {
	for name != "" {
		// Avoid getting a Panic in FieldByName by checking to make sure we have a structure to deal with
		if v.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("FieldByPath expected %+v to be a structure to find field %+v", v, name)
		}
		// Split off everything after the period (if there actually is one)
		strslice := strings.SplitN(name, ".", 2)
		// And get the Field for the first portion
		v = v.FieldByName(strslice[0])
		// Use what is left over after the . to repeat the process
		name = ""
		if len(strslice) > 1 && v.IsValid() {
			name = strslice[1]
		}
	}
	return v, nil
}

// Marshall copies the values to a database structure from an internal representation structure
// using the annotations on the database structure to indicate which field to copy to.  This maps
// null fields from the database (stored in the sql.NullString... structures) into an appropriate null value.
func Marshall(source interface{}, dest interface{}) error {
	// For the source we just need the Value structure.
	sourceValue := reflect.Indirect(reflect.ValueOf(source))
	// For the dest we need both the Value (for the source values) and the Type (to get structure and annotations) of the source.
	destValue := reflect.ValueOf(dest)
	// Make sure we have a pointer to the structure
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("Destination is not a pointer")
	}
	// Derefence the pointer to get to the raw type
	destValue = destValue.Elem()
	destType := destValue.Type()

	// Sanity check to make sure we are working with structures
	if sourceValue.Kind() != reflect.Struct {
		return fmt.Errorf("Source is not a struct")
	}
	// Remember we already dereferenced the pointer, so the warning is different
	if destType.Kind() != reflect.Struct {
		return fmt.Errorf("Destination is not a pointer to a struct")
	}
	// We have a structure, so iterate through all the fields in the destination type structure
	// because that structure has all of the annotations on it
	for fieldNum := 0; fieldNum < destType.NumField(); fieldNum++ {
		// For the dest field we need the Value and the Type
		fieldValue := destValue.Field(fieldNum)
		fieldType := destType.Field(fieldNum)
		// Are we using an alternate name to save into or the same name
		mappedName, hasMappedName := fieldType.Tag.Lookup("map")
		if !hasMappedName {
			mappedName = fieldType.Name
		}
		// Check to see if they are indexing the field
		// This will be indicated by a mapped name like NAICSCode[1]
		// in which case we need to parse out the index.
		// NOTE: we don't require the closing ] but we need to make sure that the index is a valid number.
		strslice := strings.Split(mappedName, "[")
		// Assume we don't have an index
		hasindex := false
		sliceindex := 0
		// Unless we actually have something after the []
		if len(strslice) > 1 {
			// Get rid of any trailing ] and parse the number as an integer
			piece2 := strings.Split(strslice[1], "]")
			newspot, err := strconv.Atoi(piece2[0])
			// As long as it parses correctly, we will use that index and clean up the mapped name
			if err == nil {
				sliceindex = newspot
				hasindex = true
				mappedName = strslice[0]
			}
		}
		// Find any default custom null value.
		nullDefault, hasNullDefault := fieldType.Tag.Lookup("null")
		//
		// Figure out what the source value is
		//
		srcFieldValue, err := FieldByPath(sourceValue, mappedName)
		if err != nil {
			return err
		}
		if !srcFieldValue.IsValid() {
			return fmt.Errorf("Mapped Field %+v is not valid for %+v", mappedName, fieldType.Name)
		}
		// We have the output field we want to write to.
		// Check to see if we are going into an array for the output
		if hasindex && (srcFieldValue.Kind() == reflect.Array || srcFieldValue.Kind() == reflect.Slice) {
			// Make sure that the array index is in bounds
			if sliceindex >= srcFieldValue.Len() {
				srcFieldValue = reflect.Zero(srcFieldValue.Type().Elem())
			} else {
				srcFieldValue = srcFieldValue.Index(sliceindex)
			}
		}
		// Make sure it is something we can actually manipulate
		if !srcFieldValue.IsValid() {
			return fmt.Errorf("Source Field %+v is not valid", mappedName)
		}
		// We have the source value, figure out if we have to do something special for the output field
		// to handle the special sql null items
		destInterface := fieldValue.Interface()
		switch destInterface.(type) {
		case sql.NullString:
			String := asString(srcFieldValue.Interface())
			if !hasNullDefault {
				nullDefault = ""
			}
			Valid := (String != nullDefault)
			CopyInterface(fieldValue.FieldByName("String"), String)
			CopyInterface(fieldValue.FieldByName("Valid"), Valid)
			// Commented out because a NullBool makes no sense
			//		case sql.NullBool:
			//			// Default is False unless they override with the string "true"
			//			bool := false
			//			defaultbool := false
			//			if sourceInterface.(sql.NullString).Valid {
			//				sourceInterface = sourceInterface.(sql.NullBool).Bool
			//			} else if strings.EqualFold(nullDefault, "true") {
			//				sourceInterface = true
			//			} else {
			//				sourceInterface = false
			//			}
		case sql.NullInt64:
			// Default is 0 unless they override with a value
			Int64 := srcFieldValue.Int()
			nullInt64 := int64(0)
			if hasNullDefault {
				altValue, err := strconv.ParseInt(nullDefault, 10, 64)
				if err == nil {
					nullInt64 = altValue
				}
			}
			Valid := (Int64 != nullInt64)
			CopyInterface(fieldValue.FieldByName("Int64"), Int64)
			CopyInterface(fieldValue.FieldByName("Valid"), Valid)
		case sql.NullFloat64:
			// Default is 0.0 unless they override with a value
			Float64 := srcFieldValue.Float()
			nullFloat64 := float64(0.0)
			if hasNullDefault {
				altValue, err := strconv.ParseFloat(nullDefault, 64)
				if err == nil {
					nullFloat64 = altValue
				}
			}
			Valid := (Float64 != nullFloat64)
			CopyInterface(fieldValue.FieldByName("Float64"), Float64)
			CopyInterface(fieldValue.FieldByName("Valid"), Valid)
		default:
			err := CopyInterface(fieldValue, srcFieldValue.Interface())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Query executes a query on the database for a single record into a database template and then
// marshalls the result into a data interface.
// If no records are found for the query, sql.ErrNoRows is returned
//
func Query(db *sqlx.DB, query string, dbTemplate interface{}, data interface{}, args ...interface{}) (err error) {
	var rows *sqlx.Rows
	rows, err = db.Queryx(query, args...)
	// Close off the rows when we are done (since we may not have read to the end of the list)
	defer rows.Close()
	// If we didn't get any rows, let them know
	if !rows.Next() {
		err = sql.ErrNoRows
	}
	if err == nil {
		// Scan only the first result into our database template
		err = rows.StructScan(dbTemplate)
		if err == nil {
			// If that works, then UnMarshall the results into the program template
			err = UnMarshall(dbTemplate, data)
		}
	}
	return
}

// VerifySQLTableName checks for potential SQL injection errors by confirming that the
// table name does not contain anything but alpha numeric and underscore characters
//
func VerifySQLTableName(tableName string) (err error) {
	err = nil
	// The name of the table that they are going to use must not have anything that
	// could cause a SQL Injection error
	tableRegex := regexp.MustCompile(`^[A-Za-z0-9_\.]+$`)
	if !tableRegex.MatchString(tableName) {
		err = ErrBadTableName
	}
	return
}

// InsertRecord allows you to insert a new record into a table respecting nulls
func InsertRecord(db *sqlx.DB, tableName string, record interface{}) (sql.Result, error) {
	// First check for any potential SQL Injection
	err := VerifySQLTableName(tableName)
	if err != nil {
		return nil, err
	}
	sqlStr := "INSERT INTO " + tableName + " ("
	valStr := ""
	extra := ""
	args := []interface{}{}

	// We need both the Value (for the source values) and the Type (to get structure and annotations) of the source.
	recordValue := reflect.Indirect(reflect.ValueOf(record))
	recordType := recordValue.Type()
	// Sanity check to make sure we are working with structures
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("record is not a struct")
	}

	// Use the standard mapper that pulls out the database mappings based on the db annotations in the structure
	mapper := db.Mapper
	typemap := mapper.TypeMap(recordValue.Type())
	topLevel := typemap.Tree

	// Iterate through all the members of the structure.
	for _, fieldInfo := range topLevel.Children {
		if fieldInfo == nil {
			continue
		}
		addfield := false
		field := fieldInfo.Name
		// See if this field is an autoincrement field.  If so, we just ignore and don't add it
		_, hasAI := fieldInfo.Options["ai"]
		if hasAI {
			continue
		}
		// Get the value for this field
		val := reflectx.FieldByIndexes(recordValue, fieldInfo.Index)

		// Check to see if we are dealing with one of the SQL null structures
		if val.Kind() == reflect.Struct {
			fieldInterface := val.Interface()
			switch fieldInterface.(type) {
			case sql.NullString:
				if fieldInterface.(sql.NullString).Valid {
					addfield = true
					val = reflect.ValueOf(fieldInterface.(sql.NullString).String)
				}
			case sql.NullInt64:
				if fieldInterface.(sql.NullInt64).Valid {
					addfield = true
					val = reflect.ValueOf(fieldInterface.(sql.NullInt64).Int64)
				}
			case sql.NullFloat64:
				if fieldInterface.(sql.NullFloat64).Valid {
					addfield = true
					val = reflect.ValueOf(fieldInterface.(sql.NullFloat64).Float64)
				}
			default:
				addfield = true
			}
		} else {
			addfield = true
		}
		// If this is a field we know how to handle with the database, then we can add it to the list of fields in the SQL
		if addfield {
			sqlStr = sqlStr + extra + field
			valStr = valStr + extra + "?"
			extra = ","
			args = append(args, val.Interface())
		}
	}
	// If we get here and the extra flag is not blank was set, it indicates that we had added at least one field to the
	// structure to save.  We can no generate the query and execute it.
	if len(extra) > 0 {
		sqlStr = sqlStr + ") VALUES (" + valStr + ")"
		return db.Exec(sqlStr, args...)
	}
	// Apparently we had nothing to do, so just return quietly
	return nil, nil
}

// UpdateRecord allows you to update an existing record in the database
func UpdateRecord(db *sqlx.DB, tableName string, record interface{}) (sql.Result, error) {
	// First check for any potential SQL Injection
	err := VerifySQLTableName(tableName)
	if err != nil {
		return nil, err
	}
	sqlStr := "UPDATE " + tableName + " SET "
	extra := ""
	where := ""
	var whereval interface{}
	args := []interface{}{}

	// We need both the Value (for the source values) and the Type (to get structure and annotations) of the source.
	recordValue := reflect.Indirect(reflect.ValueOf(record))
	recordType := recordValue.Type()
	// Sanity check to make sure we are working with structures
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("record is not a struct")
	}

	// Use the standard mapper that pulls out the database mappings based on the db annotations in the structure
	mapper := db.Mapper
	typemap := mapper.TypeMap(recordValue.Type())
	topLevel := typemap.Tree

	// Iterate through all the top level fields in the structure
	for _, fieldInfo := range topLevel.Children {
		if fieldInfo == nil {
			continue
		}
		addfield := false
		field := fieldInfo.Name

		val := reflectx.FieldByIndexes(recordValue, fieldInfo.Index)
		if val.Kind() == reflect.Struct {
			fieldInterface := val.Interface()
			switch fieldInterface.(type) {
			case sql.NullString:
				if fieldInterface.(sql.NullString).Valid {
					addfield = true
					val = reflect.ValueOf(fieldInterface.(sql.NullString).String)
				}
			case sql.NullInt64:
				if fieldInterface.(sql.NullInt64).Valid {
					addfield = true
					val = reflect.ValueOf(fieldInterface.(sql.NullInt64).Int64)
				}
			case sql.NullFloat64:
				if fieldInterface.(sql.NullFloat64).Valid {
					addfield = true
					val = reflect.ValueOf(fieldInterface.(sql.NullFloat64).Float64)
				}
			default:
				addfield = true
			}
		} else {
			addfield = true
		}
		// See if this field is an autoincrement field.  If so, then it is the key we are matching on
		_, hasAI := fieldInfo.Options["ai"]
		if hasAI {
			where = " WHERE " + field + "=?"
			whereval = val.Interface()
			// If this is a field we know how to handle with the database, then we can add it to the list of fields in the SQL
		} else if addfield {
			sqlStr = sqlStr + extra + field + "=?"
			extra = ","
			args = append(args, val.Interface())
		}
	}
	// If we get here and the extra flag is not blank was set, it indicates that we had added at least one field to the
	// structure to save.  We can no generate the query and execute it.
	if len(extra) > 0 {
		sqlStr = sqlStr + where
		args = append(args, whereval)
		return db.Exec(sqlStr, args...)
	}
	// Apparently we had nothing to do, so just return quietly
	return nil, nil
}

// MarshallInsertRecord inserts a record of any type into the database and returns the insertId from
// that record if it succeeds or any errors if it fails
// For the dbTemplate you must pass in an empty structure by address to allow the data to be marshalled
//
func MarshallInsertRecord(db *sqlx.DB, table string, data interface{}, dbTemplate interface{}) (resID int64, err error) {
	// Default id returned is -1
	resID = int64(0)
	// Convert the data to a format the database likes
	err = Marshall(data, dbTemplate)
	if err != nil {
		return
	}
	// Attempt to insert into the database
	var res sql.Result
	res, err = InsertRecord(db, table, dbTemplate)
	if err != nil {
		return
	}
	// If we succeeded, get the id of the last inserted record
	resID, err = res.LastInsertId()
	return
}

// MarshallUpdateRecord finds the record based on the table name and the autoIncrementing key.  NOTE: Whatever structure
// is inserted into the table, the incrementing key must be the ONLY incrementing key in the table and it must be the same
// as the record you are hoping to update
//
func MarshallUpdateRecord(db *sqlx.DB, table string, data interface{}, dbTemplate interface{}) (resID int64, err error) {
	// Default id returned is -1
	resID = int64(0)
	// Convert the data to a format the database likes
	err = Marshall(data, dbTemplate)
	if err != nil {
		return
	}
	// Attempt to insert into the database
	var res sql.Result
	res, err = UpdateRecord(db, table, dbTemplate)
	if err != nil {
		return
	}
	// If we succeeded, get the id of the last inserted record
	resID, err = res.LastInsertId()
	return
}

//***************************************************************************************

//
// CopyInterface is a copy of convertAssign from the sql package
// It copies a value from a src to a destination interface converting types as appropriate.
// Ideally we would want to have this interface exported.. but the Powers-that-be decided
// to keep it private so we have to clone it here.
func CopyInterface(destValue reflect.Value, src interface{}) error {
	// Common cases, without reflect.
	destInterface := destValue.Interface()
	switch srcType := src.(type) {
	case string:
		switch d := destInterface.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = srcType
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(srcType)
			return nil
		}
	case []byte:
		switch d := destInterface.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = string(srcType)
			return nil
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(srcType)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(srcType)
			return nil
		}
	case time.Time:
		switch d := destInterface.(type) {
		case *string:
			*d = srcType.Format(time.RFC3339Nano)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(srcType.Format(time.RFC3339Nano))
			return nil
		}
	case nil:
		switch d := destInterface.(type) {
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		}
	}

	var sourceValue reflect.Value

	switch d := destInterface.(type) {
	case *string:
		sourceValue = reflect.ValueOf(src)
		switch sourceValue.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			*d = asString(src)
			return nil
		}
	case *[]byte:
		sourceValue = reflect.ValueOf(src)
		if b, ok := asBytes(nil, sourceValue); ok {
			*d = b
			return nil
		}
	case *bool:
		bv, err := driver.Bool.ConvertValue(src)
		if err == nil {
			*d = bv.(bool)
		}
		return err
	case *interface{}:
		*d = src
		return nil
	}

	if !sourceValue.IsValid() {
		sourceValue = reflect.ValueOf(src)
	}

	if sourceValue.IsValid() && sourceValue.Type().AssignableTo(destValue.Type()) {
		switch b := src.(type) {
		case []byte:
			destValue.Set(reflect.ValueOf(cloneBytes(b)))
		default:
			destValue.Set(sourceValue)
		}
		return nil
	}

	if destValue.Kind() == sourceValue.Kind() && sourceValue.Type().ConvertibleTo(destValue.Type()) {
		destValue.Set(sourceValue.Convert(destValue.Type()))
		return nil
	}

	switch destValue.Kind() {
	case reflect.Ptr:
		if src == nil {
			destValue.Set(reflect.Zero(destValue.Type()))
			return nil
		}
		destValue.Set(reflect.New(destValue.Type().Elem()))
		return CopyInterface(destValue, src)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := asString(src)
		i64, err := strconv.ParseInt(s, 10, destValue.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, destValue.Kind(), err)
		}
		destValue.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := asString(src)
		u64, err := strconv.ParseUint(s, 10, destValue.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, destValue.Kind(), err)
		}
		destValue.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		s := asString(src)
		f64, err := strconv.ParseFloat(s, destValue.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, destValue.Kind(), err)
		}
		destValue.SetFloat(f64)
		return nil
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, destInterface)
}

// The following routines were also taken from the sql package to support CopyInterface
// which of course was private..
func strconvErr(err error) error {
	if ne, ok := err.(*strconv.NumError); ok {
		return ne.Err
	}
	return err
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

func asBytes(buf []byte, rv reflect.Value) (b []byte, ok bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}
	return
}
