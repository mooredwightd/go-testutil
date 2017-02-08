package awsdynamodb

import (
	"testing"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"fmt"
	"github.com/go-testutil/testutil"
	"reflect"
)

// Test looking at the struct tags for the tag "primary" to identify the hash key.
// The hash (primary) key is required to create a collection.
func TestGetPartitionKeyField(t *testing.T) {
	t.Run("A=1", func(t *testing.T) {
		// Test finding "key-type": "primary"
		attrDef := GetPartitionKeyField(testDoc1)
		testutil.AssertNotNil(t, attrDef, "Expected valid hash key field name *string. Actual <nil>.")
		testutil.AssertStringsEqual(t, *attrDef.AttributeName, "DocName", "Expected primary key \"Name\".")
		testutil.AssertStringsEqual(t, *attrDef.AttributeType, "S", "Expected key data type key \"S\".")
	})
	t.Run("B=1", func(t *testing.T) {
		// Test finding "key-type": "primary" w/o defined tag
		attrDef := GetPartitionKeyField(testDocBad1)
		testutil.AssertNil(t, attrDef, "Expected <nil> for primary key attribute *string.")
	})
}

// Test looking at the struct tags for the tag "secondary" to identify the sort key.
func TestGetSortKeyField(t *testing.T) {
	t.Run("A=1", func(t *testing.T) {
		// Test finding "key-type": "secondary"
		attrDef := GetSortKeyField(testDoc1)
		testutil.AssertNotNil(t, attrDef, "Expected valid hash key field name *string. Actual <nil>.")
		testutil.AssertStringsEqual(t, *attrDef.AttributeName, "Owner", "Expected primary key \"Owner\".")
		testutil.AssertStringsEqual(t, *attrDef.AttributeType, "S", "Expected key data type key \"S\".")
	})
	t.Run("B=1", func(t *testing.T) {
		// Test finding "key-type": "primary" w/o defined tag
		attrDef := GetSortKeyField(testDocBad1)
		testutil.AssertNil(t, attrDef, "Expected <nil> for secondary key attribute *string.")
	})
}

func TestGetAttributeValue(t *testing.T) {
	t.Run("A=1", func(t *testing.T) {
		attrVal := GetAttributeValue(testDoc1, "DocName")
		testutil.AssertNotNil(t, attrVal, "Expected valid hash key field name *string. Actual <nil>.")
		testutil.AssertStringsEqual(t, *attrVal.S, "Passengers", "Expected primary key \"Name\".")
	})
	t.Run("B=1", func(t *testing.T) {
		attrVal := GetAttributeValue(testDoc1, "NotAValidName")
		testutil.AssertNil(t, attrVal, "Expected valid hash key field name *string. Actual <nil>.")
	})
}

func TestLookupExpressionName(t *testing.T) {
	t.Run("A=1", func(t *testing.T) {
		expName := lookupExpressionAttributeName(testDoc1, "Value")
		testutil.AssertNotNil(t, expName, "Expected valid string ptr (not nil)")
		testutil.AssertStringsEqual(t, *expName, "#item_price", "Expected string \"%s\".", "item_price")
	})
	// No tag found
	t.Run("B=1", func(t *testing.T) {
		expName := lookupExpressionAttributeName(testDoc1, "DocName")
		testutil.AssertNil(t, expName, "Expected nil value for \"DocName\".")
	})
	// Invalid field name
	t.Run("B=2", func(t *testing.T) {
		expName := lookupExpressionAttributeName(testDoc1, "Ooops")
		testutil.AssertNil(t, expName, "Expected nil value for \"Ooops\".")
	})
}

func TestMarshalAWS(t *testing.T) {
	t.Run("A=1", func(t *testing.T) {
		expValues := [...]struct {
			FieldKey  string
			FieldType string
			FieldName string
		}{
			{"DocName", "S", "DocName"},
			{"Owner", "S", "Owner"},
			{"Description", "S", "Description"},
			{"Value", "N", "#item_price"},
			{"Size", "N", "#amount"},
			{"SArray", "SS", "SArray"},
			{"IArray", "NS", "IArray"},
			{"FArray", "NS", "FArray"},
			{"IMap", "M", "IMap"},
			{"SMap", "M", "SMap"},
			{"IntfArray", "L", "IntfArray"},
			{"StArray", "L", "StArray"},
		}

		av := MarshalAWS(testDoc1)
		testutil.AssertGreaterThan(t, len(av), 0, "Expected at least one value in map.")
		for i := 0; i < len(expValues); i++ {
			avx, ok := av[expValues[i].FieldName]
			testutil.AssertTrue(t, ok, "Expected map key \"%s\".", expValues[i].FieldName)

			//t.Logf("    Field \"%s\": %v.", expValues[i].FieldName, avx)
			switch expValues[i].FieldType {
			case "S":
				testutil.AssertNotNil(t, avx.S, "Expected valid S *string for \"%s\".", expValues[i].FieldName)
			case "N":
				testutil.AssertNotNil(t, avx.N, "Expected valid N *string for \"%s\".", expValues[i].FieldName)
			case "B":
				testutil.AssertGreaterThan(t, avx.B, 0, "Expected B valid length > 0 for \"%s\".", expValues[i].FieldName)
			case "BOOL": fallthrough
			case "NULL":
				testutil.AssertNotNil(t, avx.NULL, "Expected BOOL/NULL valid *bool for \"%s\".", expValues[i].FieldName)
				testutil.AssertTrue(t, *avx.NULL, "Expected BOOL/NULL true for \"%s\".", expValues[i].FieldName)
			case "L":
				testutil.AssertGreaterThan(t, len(avx.L), 0, "Expected L length > 0 for \"%s\".", expValues[i].FieldName)
			case "M":
				testutil.AssertGreaterThan(t, len(avx.M), 0, "Expected M length > 0 for \"%s\".", expValues[i].FieldName)
			case "SS":
				testutil.AssertGreaterThan(t, len(avx.SS), 0, "Expected SS length > 0 for \"%s\".", expValues[i].FieldName)
			case "NS":
				testutil.AssertGreaterThan(t, len(avx.NS), 0, "Expected NS length > 0 for \"%s\".", expValues[i].FieldName)
			case "BS":
			default:
				t.Fatalf("Invalid Attributevalue for \"%s\".", expValues[i].FieldName)
			}
		}

	})
}

func TestMarshalAWS2(t *testing.T) {

	var expValues = map[string]string{
		"DocName": "S",
		"Field_1": "BOOL",
		"Amount": "N",
		"inventory_count": "NS",
		"items": "L",
		"description": "M",
		"s1": "L",
		"bool1": "L",
	}
	av := MarshalAWS(docMap)
	testutil.AssertNotNil(t, av, "Expected valid *AttributeDefinition for %s", "docMap")
	//printDocItems("docMap", av, 0)

	for k, v := range expValues {
		val, ok := av[k]
		testutil.AssertTrue(t, ok, "Expeced to find field \"%s\".", k)
		switch v {
		case "S":
			testutil.AssertStringsEqual(t, docMap[k].(string), *val.S, "Expected value match for %s", k)
		case "N":
			testutil.AssertStringsEqual(t, fmt.Sprintf("%v", docMap[k]), *val.N,
				"Expected value match for %s", k)
		case "BOOL":
			testutil.AssertEqual(t, docMap[k], *val.BOOL, "Expected value match for %s. Actual: %v", k, val)
		case "B":
			testutil.AssertEqual(t, docMap[k].(bool), *val.BOOL, "Expected value match for %s", k)
		case "NS":
			testutil.AssertGreaterThan(t, len(val.NS), 0, "Expected length to be greater than 0 for %s", k)
		case "L":
			testutil.AssertGreaterThan(t, len(val.L), 0, "Expected length to be greater than 0 for %s", k)
		case "M":
			testutil.AssertGreaterThan(t, len(val.M), 0, "Expected length to be greater than 0 for %s", k)
		default:
			t.Fatalf("Field type mismatch for \"%s\".", k)
		}
	}

}

func TestUnmarshalAWS(t *testing.T) {

	t.Run("Scalar1=1", func(t *testing.T) {
		var a scalar1
		var jData = map[string]*dynamodb.AttributeValue{
			"BoolValue": {BOOL: aws.Bool(true) },
			"DocName": {S: aws.String("Scalar=1") },
			"FValue": {N: aws.String("2.2") },
			"IValue": {N: aws.String("1") },
		}
		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(scalar1)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "scalar1")
		testutil.AssertTrue(t, docX.BoolValue, "Expected %s as true. %+v", "BoolValue", docX)
		testutil.AssertStringsEqual(t, docX.DocName, *jData["DocName"].S,
			"Expected %s to match. %+v", "DocName", docX)
		testutil.AssertEqual(t, docX.FValue, float32(2.2), "Expected %s to match. %+v", "FValue", docX)
		testutil.AssertEqual(t, docX.IValue, 1, "Expected %s to match. %+v", "IValue", docX)
		t.Logf("UnmarshallAWS() Scalar=1 returned %+v", docX)
	})

	t.Run("Slice=1", func(t *testing.T) {
		var a sliceStruct1
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {SS: []*string{aws.String("a"), aws.String("b"), aws.String("c"), aws.String("d")}},
			"DocName": {S: aws.String("Slice=1")},
		}
		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(sliceStruct1)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "sliceStruct1")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].SS),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=1 returned %+v", docX)
	})

	t.Run("Slice=2", func(t *testing.T) {
		var a sliceStruct2
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {NS: []*string{aws.String("3.3"), aws.String("2.2"), aws.String("4.4"), aws.String("1.1")}},
			"DocName": {S: aws.String("Slice=2")},
		}
		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(sliceStruct2)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "sliceStruct2")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].NS),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=2 returned %+v", docX)
	})
	t.Run("Slice=3", func(t *testing.T) {
		var a sliceStruct3
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {NS: []*string{aws.String("10"), aws.String("20"), aws.String("30"), aws.String("40")}},
			"DocName": {S: aws.String("Slice=3")},
		}
		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(sliceStruct3)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "sliceStruct3")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].NS),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=3 returned %+v", docX)
	})
	t.Run("Slice=4", func(t *testing.T) {
		var a sliceStruct4
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {L: []*dynamodb.AttributeValue{{BOOL:aws.Bool(true)}, {BOOL:aws.Bool(false)},
				{BOOL:aws.Bool(false)}, {BOOL:aws.Bool(true)} }},
			"DocName": {S: aws.String("Slice=4")},
		}
		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(sliceStruct4)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "sliceStruct4")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].L),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=4 returned %+v", docX)
	})
	t.Run("Slice=5", func(t *testing.T) {
		var a sliceStruct5
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {L: []*dynamodb.AttributeValue{{S:aws.String("a")},
				{N:aws.String("1")}, {N:aws.String("2.2")}, {BOOL:aws.Bool(true)}}},
			"DocName": {S: aws.String("Slice=5")},
		}
		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(sliceStruct5)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "sliceStruct5")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].L),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=5 returned %+v", docX)
	})
	t.Run("Slice=6", func(t *testing.T) {
		var a sliceStruct6
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {L: []*dynamodb.AttributeValue{
				{M: map[string]*dynamodb.AttributeValue{"a": {N: aws.String("1")}, "b": {N: aws.String("2")}, "c": {N: aws.String("3")}}},
				{M: map[string]*dynamodb.AttributeValue{"y": {N: aws.String("5")}, "z": {N: aws.String("6")}}},
			}},
			"DocName": {S: aws.String("Slice=6")},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(sliceStruct6)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "sliceStruct6")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].L),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=6 returned %+v", docX)
	})
	t.Run("Slice=7", func(t *testing.T) {
		var a struct1
		var jData = map[string]*dynamodb.AttributeValue{
			"AnArray": {L: []*dynamodb.AttributeValue{
				{M: map[string]*dynamodb.AttributeValue{"I": {N: aws.String("0")}}},
				{M: map[string]*dynamodb.AttributeValue{"I": {N: aws.String("1")}}},
				{M: map[string]*dynamodb.AttributeValue{"I": {N: aws.String("2")}}},
			}},
			"DocName": {S: aws.String("Slice=76")},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(struct1)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "struct1")
		testutil.AssertEqual(t, len(docX.AnArray), len(jData["AnArray"].L),
			"Expected %s length match. %+v\n", "AnArray", docX)
		t.Logf("UnmarshallAWS() Slice=7 returned %+v", docX)
	})
	t.Run("Map=1", func(t *testing.T) {
		var a map1
		var jData = map[string]*dynamodb.AttributeValue{
			"AMap": {M: map[string]*dynamodb.AttributeValue{
				"0": {S: aws.String("1")}, "2": {S: aws.String("3")}, "4": {S: aws.String("5")},
			}},
			"DocName": {S: aws.String("Map=1")},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(map1)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "map1")
		testutil.AssertEqual(t, len(docX.AMap), len(jData["AMap"].M),
			"Expected %s length match. %+v\n", "AMap", docX)
		t.Logf("UnmarshallAWS() Map=1 returned %+v", docX)
	})
	t.Run("Map=2", func(t *testing.T) {
		var a map2
		var jData = map[string]*dynamodb.AttributeValue{
			"AMap": {M: map[string]*dynamodb.AttributeValue{
				"a": {S: aws.String("1")}, "b": {S: aws.String("3")}, "c": {S: aws.String("5")},
			}},
			"DocName": {S: aws.String("Map=2")},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(map2)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "map2")
		testutil.AssertEqual(t, len(docX.AMap), len(jData["AMap"].M),
			"Expected %s length match. %+v\n", "AMap", docX)
		t.Logf("UnmarshallAWS() Map=2 returned %+v", docX)
	})
	t.Run("Map=3", func(t *testing.T) {
		var a map3
		var jData = map[string]*dynamodb.AttributeValue{
			"AMap": {M: map[string]*dynamodb.AttributeValue{
				"a": {N: aws.String("1")}, "b": {N: aws.String("3")}, "c": {N: aws.String("5")},
			}},
			"DocName": {S: aws.String("Map=3")},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(map3)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "map3")
		testutil.AssertEqual(t, len(docX.AMap), len(jData["AMap"].M),
			"Expected %s length match. %+v\n", "AMap", docX)
		t.Logf("UnmarshallAWS() Map=3 returned %+v", docX)
	})
	t.Run("Map=4", func(t *testing.T) {
		var a map4
		var jData = map[string]*dynamodb.AttributeValue{
			"AMap": {M: map[string]*dynamodb.AttributeValue{
				"1.1": {N: aws.String("1")}, "2.2": {N: aws.String("3")}, "3.3": {N: aws.String("5")},
			}},
			"DocName": {S: aws.String("Map=4")},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(map4)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "map4")
		testutil.AssertEqual(t, len(docX.AMap), len(jData["AMap"].M),
			"Expected %s length match. %+v\n", "AMap", docX)
		testutil.AssertStringsEqual(t, "Map=4", docX.DocName, "Expected key/value DocName to match. Actual:%+v\n", docX)
		t.Logf("UnmarshallAWS() Map=4 returned %+v", docX)
	})
	t.Run("Map=5", func(t *testing.T) {
		var a TestMarshalMap
		var jData = map[string]*dynamodb.AttributeValue{
			"DocName": {S: aws.String("TestMap")},
			"Amount": {N: aws.String("100")},
			"bool1": {L: []*dynamodb.AttributeValue{
				{BOOL: aws.Bool(true)}, {BOOL: aws.Bool(false)},
			}},
			"description": {M: map[string]*dynamodb.AttributeValue{
				"Category": {S: aws.String("cardboard contianers")}, "D": {S: aws.String("boxes")}},
			},
			"Field1": {BOOL: aws.Bool(true)},
			"inventory_count": {NS: []*string{aws.String("7"), aws.String("5"), aws.String("3"), aws.String("1")}},
			"items": {L: []*dynamodb.AttributeValue{
				{BOOL: aws.Bool(true)} , {N: aws.String("5")}, {S: aws.String("5x5 boxes")},
				{N: aws.String("4.99")}, {S: aws.String("USD")} },
			},
			"s1": {L: []*dynamodb.AttributeValue{{S: aws.String("sptr1")}, {S: aws.String("sptr2")}}},
		}

		x := UnmarshalAWS(a, jData).(reflect.Value)
		docX := x.Interface().(TestMarshalMap)
		testutil.AssertNotNil(t, docX, "Expected valid %v", "TestMarshalMap")
		t.Logf("UnmarshallAWS() Map=5 returned %+v", docX)
	})
}