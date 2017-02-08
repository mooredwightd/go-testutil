package awsdynamodb

import (
	"fmt"
	"reflect"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"repository"
	"strconv"
)

type AwsDocumentKeyRetriever interface {
	GetPartitionKeyField() (*dynamodb.AttributeDefinition)
	GetSortKeyField() (*dynamodb.AttributeDefinition)
}

type AwsAttributeMarshaler  interface {
	GetAttributeValue(fieldName string) (*dynamodb.AttributeValue)
}

type AwsMarshaler interface {
	MarshalAWS() (map[string]*dynamodb.AttributeValue)
}
type AwsUnmarshaler interface {
	UnmarshalAWS(doc map[string]*dynamodb.AttributeValue) (interface{})
}

var (
	// Interfaces
	keyRetrieverType = reflect.TypeOf(new(AwsDocumentKeyRetriever)).Elem()
	attrMashalerType = reflect.TypeOf(new(AwsAttributeMarshaler)).Elem()
	marshalerType = reflect.TypeOf(new(AwsMarshaler)).Elem()
	unmarshalerType = reflect.TypeOf(new(AwsUnmarshaler)).Elem()
)

// Performs a reflection on a structure for the tag "key-type" for value "primary".
// This is the AWS partition/hash key field. it must be an specified on the table creation.
// Returns nil if not found.
func GetPartitionKeyField(doc interface{}) (attrDef *dynamodb.AttributeDefinition) {
	krt := getInterface(doc, keyRetrieverType)
	if krt != nil {
		attrDef = krt.(AwsDocumentKeyRetriever).GetPartitionKeyField()
	} else {
		attrDef = getAttributeDefinitionByTag(doc, "key-type", "primary")
	}
	return
}

// Performs a reflection on a structure for the tag "aws-key-type" for value "sort"
// Returns the field name, and the AWS scalar attribute type.
// Returns nil if not found.
func GetSortKeyField(doc interface{}) (attrDef *dynamodb.AttributeDefinition) {
	krt := getInterface(doc, keyRetrieverType)
	if krt != nil {
		attrDef = krt.(AwsDocumentKeyRetriever).GetSortKeyField()
	} else {
		attrDef = getAttributeDefinitionByTag(doc, "key-type", "secondary")
	}
	return
}

// Marshal the value of a struct field, map or array
// If the doc is a struct, returns the AttributeValue for the field.
// If the doc is a map, returns the AttributeValue for the item at that key.
// If the doc is an array/slice, returns the AttributeValue for the item at that index.
// Returns nil if nto a struct, map, array or slice, or if the field/index is not found.
func GetAttributeValue(doc interface{}, fieldName string) (av *dynamodb.AttributeValue) {
	docVal := reflect.ValueOf(doc)

	// FieldByName requires a struct
	if docVal.Kind() == reflect.Ptr {
		docVal = docVal.Elem()
	}

	amt := getInterface(doc, attrMashalerType)
	if amt != nil {
		return amt.(AwsAttributeMarshaler).GetAttributeValue(fieldName)
	}

	var fieldVal reflect.Value
	switch docVal.Kind() {
	case reflect.Struct:
		fieldVal = docVal.FieldByName(fieldName)
		if !fieldVal.IsValid() {
			return nil
		}
	case reflect.Map:
		fieldVal = reflect.ValueOf(doc).MapIndex(reflect.ValueOf(fieldName))
		if !fieldVal.IsValid() {
			return nil
		}
	case reflect.Array, reflect.Slice:
		ndx, err := strconv.Atoi(fieldName)
		if err != nil {
			return nil
		}
		fV := reflect.ValueOf(doc)
		if ndx < 0 || ndx >= fV.Len() {
			return nil
		}
		fieldVal = reflect.ValueOf(doc).Index(ndx)
	default:
		return nil
	}

	return marshalToAttributeValue(fieldVal)
}

// Default Marshaler for AWS.
// This parses a struct, map or an array, and creates the AWS AttributeValue structure for use in PutItem().
// This method is recursive descending a structure, map, or array
func MarshalAWS(doc interface{}) (avMap map[string]*dynamodb.AttributeValue) {

	krt := getInterface(doc, marshalerType)
	if krt != nil {
		return krt.(AwsMarshaler).MarshalAWS()
	}

	aValue := reflect.ValueOf(doc)
	if aValue.Kind() == reflect.Ptr {
		aValue = aValue.Elem()
	}

	avMap = make(map[string]*dynamodb.AttributeValue, 1)
	switch aValue.Kind() {

	case reflect.Struct:
		for i := 0; i < aValue.Type().NumField(); i++ {
			if aValue.Field(i).CanInterface() {
				av := marshalToAttributeValue(aValue.Field(i))
				if av != nil {
					name := lookupExpressionAttributeName(aValue.Interface(), aValue.Type().Field(i).Name)
					if name == nil {
						name = aws.String(fmt.Sprintf("%v", aValue.Type().Field(i).Name))
					}
					avMap[*name] = av
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if aValue.CanInterface() {
			for i := 0; i < aValue.Len(); i++ {
				if v := marshalToAttributeValue(aValue.Index(i)); v != nil {
					mKey := fmt.Sprintf("%v", i)
					avMap[mKey] = v
				}
			}
		}
	case reflect.Map:
		if aValue.CanInterface() {
			for _, v := range aValue.MapKeys() {
				if x := marshalToAttributeValue(aValue.MapIndex(v)); x != nil {
					mKey := fmt.Sprintf("%v", v.Interface())
					avMap[mKey] = x
				}
			}
		}
	default:
		avMap["root"] = marshalToAttributeValue(aValue)
	}

	return
}

// Default Unmarshaler for an AWS map[string]*AttributeValue.
// This parses the Go data type and the AttributeValues, creating a new document with the values from AWS.
// For a struct, if the AttributeValue map has a key that does not match a struct field, it is ignored.
// For a map, each key in the AttributeValue map, creates a correspoding key in the document.
func UnmarshalAWS(doc interface{}, attrBuf map[string]*dynamodb.AttributeValue) (interface{}) {
	var docVal reflect.Value

	// Create a new "document"
	dType := reflect.TypeOf(doc)
	if dType.Kind() == reflect.Ptr {
		// If the caller  passed a ptr, we need the actual Go data type
		dType = dType.Elem()
	}
	docVal = reflect.New(dType).Elem()

	switch docVal.Kind() {
	case reflect.Struct:
		// Lookup a specific tag on a struct field.
		var tagLookup = func(v interface{}, fName string, tag string) string {
			tagV := repository.GetTagByFieldName(v, fName, tag)
			if tagV == nil {
				return fName
			}
			return *tagV
		}

		for i := 0; i < dType.NumField(); i++ {
			if docVal.Field(i).CanInterface() {
				fieldName := tagLookup(docVal, docVal.Type().Field(i).Name, "aws-name")
				// Get the AttributeValue
				x, found := attrBuf[fieldName]
				if !found {
					continue
				}
				if v, mErr := marshalToGoValue(x, docVal.Field(i).Type()); mErr == nil {
					docVal.Field(i).Set(v)
				} else {
					fmt.Printf("marshalToGoValue() mErr: %v", mErr)
				}
			}
		}
	case reflect.Map:
		docVal = reflect.MakeMap(dType)
		for k, mapItem := range attrBuf {
			fmt.Printf("...key %v\n", k)
			if v, mErr := marshalToGoValue(mapItem, dType.Elem()); mErr == nil {
				docVal.SetMapIndex(reflect.ValueOf(k), v)
			} else {
				fmt.Printf("marshalToGoValue() mErr: %v", mErr)
			}
		}
	}

	return docVal
}

// This takes a reflect.Value object, and creates the corresponding AWS *AttributeValue
// Any exportable field that is Chan, Func, ComplexN are ignored.
func marshalToAttributeValue(aValue reflect.Value) *dynamodb.AttributeValue {
	var av *dynamodb.AttributeValue = new(dynamodb.AttributeValue)
	var avNull = &dynamodb.AttributeValue{NULL: aws.Bool(true)}

	//fmt.Printf("marshalToAttributeValue() Name:\"%s\" (type %s) ", aValue.Type().String(), aValue.Kind())
	switch aValue.Kind() {
	case reflect.Chan, reflect.Func, reflect.Complex64, reflect.Complex128:
		return nil
	// Basic types
	case reflect.String:
		av.S = aws.String(aValue.Interface().(string))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fallthrough
	case reflect.Float32, reflect.Float64:
		av.N = aws.String(fmt.Sprintf("%v", aValue.Interface()))
	case reflect.Bool:
		av.BOOL = aws.Bool(aValue.Interface().(bool))
	case reflect.Struct:
		av.M = make(map[string]*dynamodb.AttributeValue, aValue.NumField())
		for i := 0; i < aValue.NumField(); i++ {
			if aValue.Field(i).CanInterface() {
				if v := marshalToAttributeValue(aValue.Field(i)); v != nil {
					name := lookupExpressionAttributeName(aValue.Interface(), aValue.Type().Field(i).Name)
					if name == nil {
						name = aws.String(fmt.Sprintf("%v", aValue.Type().Field(i).Name))
					}
					av.M[*name] = v
				}
			}
		}
		if len(av.M) == 0 {
			av.NULL = aws.Bool(true)
		}
	case reflect.Map:
		if aValue.IsNil() {
			av.NULL = aws.Bool(true)
		} else {
			av.M = make(map[string]*dynamodb.AttributeValue, aValue.Len())
			for _, v := range aValue.MapKeys() {
				if x := marshalToAttributeValue(aValue.MapIndex(v)); x != nil {
					mKey := fmt.Sprintf("%v", v.Interface())
					av.M[mKey] = x
				}
			}
		}
	case reflect.Ptr: fallthrough
	case reflect.Interface:
		if aValue.IsNil() {
			av = avNull
		} else {
			av = marshalToAttributeValue(aValue.Elem())
		}

	// Array/Slice
	case reflect.Slice, reflect.Array:
		if aValue.Len() == 0 {
			av.NULL = aws.Bool(true)
			break
		}

		switch aValue.Type().Elem().Kind() {
		default:
			av.NULL = aws.Bool(true)
		case reflect.Chan, reflect.Func, reflect.Complex64, reflect.Complex128:
			return nil
		case reflect.String:
			av.SS = aws.StringSlice(aValue.Interface().([]string))
		case reflect.Int8: // SDK encodes as base64
			copy(av.B, aValue.Interface().([]byte))
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
			fallthrough
		case reflect.Float32, reflect.Float64:
			av.NS = make([]*string, aValue.Len())
			for i := 0; i < aValue.Len(); i++ {
				av.NS[i] = aws.String(fmt.Sprintf("%v", aValue.Index(i).Interface()))
			}
		case reflect.Bool: fallthrough
		case reflect.Interface: fallthrough
		case reflect.Map: fallthrough
		case reflect.Ptr: fallthrough
		case reflect.Struct:
			av.L = make([]*dynamodb.AttributeValue, aValue.Len())
			for i := 0; i < aValue.Len(); i++ {
				av.L[i] = marshalToAttributeValue(aValue.Index(i))
			}
		}
	}

	//fmt.Printf(" marshalToAttributeValue, Value:\"%v\"\n", av)
	return av
}

var nSize = map[reflect.Kind]int{
	reflect.Int:0, reflect.Int8: 8, reflect.Int16: 16, reflect.Int32:32, reflect.Int64:64,
	reflect.Uint:0, reflect.Uint8: 8, reflect.Uint16: 16, reflect.Uint32:32, reflect.Uint64:64,
	reflect.Float32: 32, reflect.Float64:64, reflect.Interface:64,
}

// Marshal a dynamodb.AttributeValue to a Go data type
// av is the AttributeValue, which could be hierarchial
// varType is the data type of the receiving struct.
//
// Returns the reflection Value object, and error. On success error is non-nil.
// For Maps, this will only converts keys of type string, or numeric
func marshalToGoValue(av *dynamodb.AttributeValue, varType reflect.Type) (reflect.Value, error) {
	zValue := reflect.Zero(varType)
	valuePtr := reflect.New(varType)
	value := valuePtr.Elem()

	switch k := varType.Kind(); {
	case k == reflect.Interface && av.S != nil: fallthrough
	case k == reflect.String:
		value.Set(reflect.ValueOf(*av.S))

	case k == reflect.Interface && av.BOOL != nil: fallthrough
	case k == reflect.Bool:
		value.Set(reflect.ValueOf(*av.BOOL))

	case k == reflect.Uint, k == reflect.Uint8, k == reflect.Uint16, k == reflect.Uint32, k == reflect.Uint64:
		n, err := strconv.ParseUint(*av.N, 0, nSize[varType.Kind()])
		if err != nil {
			return zValue, err
		}
		value.SetUint(n)

	case k == reflect.Int, k == reflect.Int8, k == reflect.Int16, k == reflect.Int32, k == reflect.Int64:
		n, err := strconv.ParseInt(*av.N, 0, nSize[varType.Kind()])
		if err != nil {
			fmt.Printf("Uint converstion error for %s", *av.N)
			return zValue, err
		}
		value.SetInt(n)

	// The variable/field is interface{}, and doesn't have a specific number type, so we'll assume float
	case k == reflect.Interface && av.N != nil: fallthrough
	case k == reflect.Float32, k == reflect.Float64:
		n, err := strconv.ParseFloat(*av.N, nSize[varType.Kind()])
		if err != nil {
			return zValue, err
		}
		if k == reflect.Interface {
			value.Set(reflect.ValueOf(n))
		} else {
			value.SetFloat(n)
		}

	case k == reflect.Interface && len(av.M) > 0: fallthrough
	case k == reflect.Map:
		var itemType, keyType reflect.Type
		if varType.Kind() == reflect.Interface {
			keyType = reflect.TypeOf("")
			itemType = varType
		} else {
			keyType = varType.Key()
			itemType = varType.Elem()
		}
		value = reflect.MakeMap(reflect.MapOf(keyType, itemType))

		for k, v := range av.M {
			keyV, kErr := convertKey(k, keyType)
			if kErr != nil {
				return zValue, kErr
			}
			x, mErr := marshalToGoValue(v, itemType)
			if mErr != nil {
				return zValue, mErr
			}
			value.SetMapIndex(keyV, x)
		}

	case k == reflect.Array: fallthrough
	case k == reflect.Interface && len(av.NS) > 0: fallthrough
	case k == reflect.Interface && len(av.SS) > 0: fallthrough
	case k == reflect.Interface && len(av.BS) > 0: fallthrough
	case k == reflect.Interface && len(av.L) > 0: fallthrough
	case k == reflect.Slice:
		var itemType reflect.Type
		if varType.Kind() == reflect.Interface {
			itemType = varType
		} else {
			itemType = varType.Elem()
		}
		switch {
		case len(av.SS) > 0:
			sLen := len(av.SS)
			value = reflect.MakeSlice(reflect.SliceOf(itemType), sLen, sLen)
			for i := 0; i < sLen; i++ {
				value.Index(i).Set(reflect.ValueOf(*av.SS[i]))
			}
		case len(av.NS) > 0:
			sLen := len(av.NS)
			value = reflect.MakeSlice(reflect.SliceOf(itemType), sLen, sLen)

			switch itemType.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				for i := 0; i < sLen; i++ {
					x, cErr := strconv.ParseUint(*av.NS[i], 0, nSize[itemType.Kind()])
					if cErr == nil {
						return zValue, fmt.Errorf("Error converting NS array: %v", av.NS)
					}
					if itemType.Kind() == reflect.Interface {
						value.Index(i).Set(reflect.ValueOf(x))
					} else {
						value.Index(i).SetUint(x)
					}
				}

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				for i := 0; i < sLen; i++ {
					x, cErr := strconv.ParseInt(*av.NS[i], 0, nSize[itemType.Kind()])
					if cErr != nil {
						return zValue, fmt.Errorf("Error converting NS array: %v", av.NS)
					}
					if itemType.Kind() == reflect.Interface {
						value.Index(i).Set(reflect.ValueOf(x))
					} else {
						value.Index(i).SetInt(x)
					}
				}

			case reflect.Interface: fallthrough
			case reflect.Float32, reflect.Float64:
				for i := 0; i < sLen; i++ {
					x, cErr := strconv.ParseFloat(*av.NS[i], nSize[itemType.Kind()])
					if cErr != nil {
						return zValue, fmt.Errorf("Error converting NS array: %v", av.NS)
					}
					if itemType.Kind() == reflect.Interface {
						value.Index(i).Set(reflect.ValueOf(x))
					} else {
						value.Index(i).SetFloat(x)
					}
				}
			}

		case len(av.BS) > 0:
			return zValue, fmt.Errorf("Not yet implemented for AWS type %s.", "B or BS")
		case len(av.L) > 0:
			sLen := len(av.L)
			value = reflect.MakeSlice(reflect.SliceOf(itemType), sLen, sLen)
			for i := 0; i < sLen; i++ {
				x, mErr := marshalToGoValue(av.L[i], itemType)
				if mErr != nil {
					return zValue, mErr
				}
				value.Index(i).Set(x)
			}
		}
	}

	return value, nil
}

func awsToNumericValue(av *dynamodb.AttributeValue, t reflect.Type) reflect.Value {
	zValue := reflect.Zero(t)
	value := reflect.New(t).Elem()

	switch  k := t.Kind(){
	case k == reflect.Uint, k == reflect.Uint8, k == reflect.Uint16, k == reflect.Uint32, k == reflect.Uint64:
		n, err := strconv.ParseUint(*av.N, 0, nSize[k])
		if err != nil {
			return zValue
		}
		value.SetUint(n)

	case k == reflect.Int, k == reflect.Int8, k == reflect.Int16, k == reflect.Int32, k == reflect.Int64:
		n, err := strconv.ParseInt(*av.N, 0, nSize[k])
		if err != nil {
			fmt.Printf("Uint converstion error for %s", *av.N)
			return zValue
		}
		value.SetInt(n)

	// The variable/field is interface{}, and doesn't have a specific number type, so we'll assume float
	case k == reflect.Interface && av.N != nil: fallthrough
	case k == reflect.Float32, k == reflect.Float64:
		n, err := strconv.ParseFloat(*av.N, nSize[k])
		if err != nil {
			return zValue
		}
		if k == reflect.Interface {
			value.Set(reflect.ValueOf(n))
		} else {
			value.SetFloat(n)
		}
	}
	return value
}

// Convert a M (map) key in an AttributeValue to the appropriate Go map key data type
// This currently only works with strings or numbers due to AWS data is transferred via JSON.
func convertKey(key string, keyType reflect.Type) (reflect.Value, error) {
	zValue := reflect.Zero(keyType)
	var kErr error

	keyV := reflect.ValueOf(key)

	switch keyType.Kind() {
	case reflect.String:
		break
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64: fallthrough
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64: fallthrough
	case reflect.Float32, reflect.Float64:
		keyV, kErr = marshalToGoValue(&dynamodb.AttributeValue{N: aws.String(key)}, keyType)
	default:
		return zValue, fmt.Errorf("Cannnot convert map key to type %v.", keyType)
	}
	if kErr != nil {
		return zValue, fmt.Errorf("Error converting map key to type %v.", keyType)
	}
	return keyV, nil
}

// Evalutes the field in a struct, and returns the AWs type based on the Go type
func goTypeToAwsType(v interface{}, fieldName string) string {
	dType := reflect.TypeOf(v)
	if dType.Kind() == reflect.Ptr {
		dType = dType.Elem()
	}
	field, ok := dType.FieldByName(fieldName)
	if !ok {
		return ""
	}
	x := mapToAwsType(field.Type)
	return x
}

// Based on the data type, returns the equivalent AWS type.
func mapToAwsType(t reflect.Type) string {
	switch t.Kind() {
	default:
		return "NULL"

	case reflect.Chan, reflect.Func:
		return "NULL"

	case reflect.Bool:
		return "BOOL"

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return "N"

	case reflect.String:
		return "S"

	case reflect.Map, reflect.Struct:
		return "M"

	case reflect.Ptr, reflect.Interface:
		return mapToAwsType(reflect.TypeOf(t).Elem())

	case reflect.Array, reflect.Slice:
		// Determine what type of array/slice
		switch e := t.Elem(); e.Kind() {
		case reflect.String:
			return "SS"
		case reflect.Int8:
			return "B"
		case reflect.Int, reflect.Int16, reflect.Int32,
			reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
			reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64,
			reflect.Complex64, reflect.Complex128:
			return "NS"
		case reflect.Ptr:
			return mapToAwsType(reflect.TypeOf(t).Elem())
		case reflect.Bool, reflect.Interface, reflect.Map, reflect.Struct:
			return "L"
		default:
			return "L"
		}
	}
}

// Return the tag value for "aws-name", or nil
// The tag value should begin with a "#" character per AWS specification.
// For example, the struct tag would have a value such as aws-name:"#Price"
func lookupExpressionAttributeName(doc interface{}, fieldName string) *string {
	expName := repository.GetTagByFieldName(doc, fieldName, "aws-name")
	if expName == nil {
		return nil
	}
	return expName
}

// Returns a map of field names to expression attribute values.
// This is a mapping for AWS substitution used when
//     To access an attribute whose name conflicts with a DynamoDB reserved word.
//     To create a placeholder for repeating occurrences of an attribute name in an expression.
//     To prevent special characters in an attribute name from being misinterpreted in an expression.
func getExpressionAttributeNames(doc interface{}) (map[string]*string) {
	docVal := reflect.ValueOf(doc)
	if docVal.Kind() == reflect.Ptr {
		docVal = docVal.Elem()
	}
	if docVal.Kind() != reflect.Struct {
		return map[string]*string{}
	}

	var eanMap = make(map[string]*string, 1)
	for i := 0; i < docVal.Type().NumField(); i++ {
		// Can only convert exposed (exportable) fields
		if docVal.Field(i).CanInterface() {
			if docVal.Field(i).Kind() == reflect.Struct {

			}
			fName := docVal.Type().Field(i).Name
			if an := lookupExpressionAttributeName(doc, fName); an != nil {
				eanMap[fName] = an
			}
		}
	}

	return eanMap
}

// Get the field name and AWS scalar type for the tag "key-type", and "aws-type".
// Valid values for the tag "aws-type" are "S", "N", or "B".
// Returns *dynamodb.AttributeDefinition, or if a tag is not found, or there is an error nil is returned.
func getFieldNameByTag(v interface{}, tagName string, targetValue string) (fieldName *string) {
	var tagValue *string
	var pos int = 0

	// Find the "key-type" tag.
	for pos = 0; pos >= 0 && (tagValue == nil || *tagValue != targetValue); pos++ {
		fieldName, tagValue, pos = repository.GetFieldByTagName(v, tagName, pos)
		if pos < 0 {
			// No more "key-type" tags found.
			break
		}
	}
	return fieldName
}