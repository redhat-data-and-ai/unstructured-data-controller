package snowflake

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// scanRows scans SQL rows into a slice of structs.
// The dest parameter must be a pointer to a slice of structs.
// Struct fields should have `db` tags that match the column names in the result set.
// Column names are matched case-insensitively with struct field tags.
func scanRows(rows *sql.Rows, dest any) error {
	if rows == nil {
		return errors.New("rows cannot be nil")
	}

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	// Ensure dest is a pointer to a slice
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Slice {
		return errors.New("dest must be a pointer to a slice")
	}

	sliceElem := v.Elem()
	elemType := sliceElem.Type().Elem()

	// Validate that the slice element type is a struct
	if elemType.Kind() != reflect.Struct {
		return errors.New("slice elements must be structs")
	}

	// Process each row
	for rows.Next() {
		item := reflect.New(elemType).Elem()

		// Prepare placeholders for column values
		values := make([]any, len(columns))
		for i := range values {
			var placeholder any
			values[i] = &placeholder
		}
		// Scan the current row into the values slice
		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Map column values to struct fields
		if err := setColumnValuesToFields(item, columns, values); err != nil {
			return fmt.Errorf("failed to set column values to fields: %w", err)
		}

		// Append the populated struct to the slice
		sliceElem = reflect.Append(sliceElem, item)
	}

	// Set the populated slice back to dest
	v.Elem().Set(sliceElem)

	// Check for any iteration errors
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during row iteration: %w", err)
	}

	return nil
}

// setColumnValuesToFields maps database column values to struct fields based on `db` tags.
// It performs case-insensitive matching between column names and struct field tags.
func setColumnValuesToFields(item reflect.Value, columns []string, values []any) error {
	for i, colName := range columns {
		if err := setColumnToMatchingField(item, colName, values[i]); err != nil {
			return fmt.Errorf("failed to set column %s: %w", colName, err)
		}
	}
	return nil
}

// setColumnToMatchingField finds the struct field with a matching `db` tag and sets its value.
func setColumnToMatchingField(item reflect.Value, colName string, value any) error {
	for j := 0; j < item.NumField(); j++ {
		field := item.Type().Field(j)
		tag := field.Tag.Get("db")

		// Check if the struct field tag matches the current column name (case-insensitive)
		if strings.EqualFold(tag, colName) {
			fieldValue := item.Field(j)
			colValue := reflect.ValueOf(*(value.(*any)))

			if err := assignFieldValue(item, field, colValue, fieldValue); err != nil {
				return fmt.Errorf("failed to assign value to field %s: %w", field.Name, err)
			}

			// Break once a match is found
			break
		}
	}
	return nil
}

// assignFieldValue assigns a column value to a struct field, handling type conversion.
func assignFieldValue(item reflect.Value, field reflect.StructField, colValue reflect.Value, fieldValue reflect.Value) error {
	if !colValue.IsValid() || !fieldValue.CanSet() {
		return nil // Skip invalid or non-settable values
	}

	// Direct assignment if types are compatible
	if colValue.Type().AssignableTo(fieldValue.Type()) {
		fieldValue.Set(colValue.Convert(fieldValue.Type()))
		return nil
	}

	// Type conversion if possible
	if colValue.Type().ConvertibleTo(fieldValue.Type()) {
		fieldValue.Set(colValue.Convert(fieldValue.Type()))
		return nil
	}

	// Handle sql.NullString specifically
	if fieldValue.Type() == reflect.TypeOf(sql.NullString{}) {
		fieldValue.Set(reflect.ValueOf(sql.NullString{
			String: colValue.String(),
			Valid:  true,
		}))
		return nil
	}

	// Fallback to manual field setting for complex types
	return setFieldValueByReflection(item.Addr().Interface(), field.Name, colValue.Interface())
}

// setFieldValueByReflection is a helper function to set struct fields using reflection with type conversion.
// This function handles various Go primitive types and performs necessary conversions.
func setFieldValueByReflection(obj any, fieldName string, value any) error {
	v := reflect.ValueOf(obj).Elem()
	f := v.FieldByName(fieldName)

	if !f.IsValid() {
		return fmt.Errorf("field %s does not exist", fieldName)
	}

	if !f.CanSet() {
		return fmt.Errorf("field %s cannot be set", fieldName)
	}

	return setValueByKind(f, value)
}

// setValueByKind sets a reflect.Value based on its Kind, performing type conversion as needed.
func setValueByKind(f reflect.Value, value any) error {
	valueStr := fmt.Sprintf("%v", value)

	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return setIntValue(f, valueStr)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return setUintValue(f, valueStr)
	case reflect.Float32, reflect.Float64:
		return setFloatValue(f, valueStr)
	case reflect.Bool:
		return setBoolValue(f, valueStr)
	case reflect.String:
		f.SetString(valueStr)
		return nil
	case reflect.Ptr:
		return setPtrValue(f, value)
	case reflect.Struct:
		return setStructValue(f, value)
	default:
		return fmt.Errorf("unsupported field type: %v", f.Kind())
	}
}

// setIntValue sets an integer value with proper error handling.
func setIntValue(f reflect.Value, valueStr string) error {
	newVal, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse int value %s: %w", valueStr, err)
	}
	f.SetInt(newVal)
	return nil
}

// setUintValue sets an unsigned integer value with proper error handling.
func setUintValue(f reflect.Value, valueStr string) error {
	newVal, err := strconv.ParseUint(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse uint value %s: %w", valueStr, err)
	}
	f.SetUint(newVal)
	return nil
}

// setFloatValue sets a float value with proper error handling.
func setFloatValue(f reflect.Value, valueStr string) error {
	newVal, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return fmt.Errorf("failed to parse float value %s: %w", valueStr, err)
	}
	f.SetFloat(newVal)
	return nil
}

// setBoolValue sets a boolean value with proper error handling.
func setBoolValue(f reflect.Value, valueStr string) error {
	newVal, err := strconv.ParseBool(valueStr)
	if err != nil {
		return fmt.Errorf("failed to parse bool value %s: %w", valueStr, err)
	}
	f.SetBool(newVal)
	return nil
}

// setPtrValue sets a pointer value, handling nil cases and type conversion.
func setPtrValue(f reflect.Value, value any) error {
	if value == nil {
		f.Set(reflect.Zero(f.Type()))
		return nil
	}

	val := reflect.ValueOf(value)
	if val.Type().AssignableTo(f.Type()) {
		f.Set(val)
		return nil
	}

	if val.Type().ConvertibleTo(f.Type().Elem()) {
		ptr := reflect.New(f.Type().Elem())
		ptr.Elem().Set(val.Convert(f.Type().Elem()))
		f.Set(ptr)
		return nil
	}

	return fmt.Errorf("cannot assign %v to pointer type %v", val.Type(), f.Type())
}

// setStructValue sets a struct value with type compatibility checking.
func setStructValue(f reflect.Value, value any) error {
	val := reflect.ValueOf(value)
	if !val.Type().AssignableTo(f.Type()) {
		return fmt.Errorf("cannot assign struct of type %v to %v", val.Type(), f.Type())
	}
	f.Set(val)
	return nil
}
