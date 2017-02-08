package awsdynamodb

import (
	"testing"
	"fmt"
	"reflect"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/go-testutil/testutil"
)

var testDocTable = "testCollection"

// ***********************************************************
// Harness for testing AwsAwsDocumentKeyRetriever interface
type InterfTest struct {
	DocName string
	AValue  string
}

func (it *InterfTest) GetPartitionKeyField() (*dynamodb.AttributeDefinition) {
	return &dynamodb.AttributeDefinition{
		AttributeName: aws.String("DocName"),
		AttributeType: aws.String("S"),
	}
}
func (it *InterfTest) GetSortKeyField() (*dynamodb.AttributeDefinition) {
	return nil
}
func (it *InterfTest) GetAttributeValue(fieldName string) (*dynamodb.AttributeValue) {
	docVal := reflect.ValueOf(it)

	// FieldByName requires a struct
	if docVal.Kind() == reflect.Ptr {
		docVal = docVal.Elem()
	}
	if docVal.Kind() != reflect.Struct {
		return nil
	}

	fieldVal := docVal.FieldByName(fieldName)
	if !fieldVal.IsValid() {
		return nil
	}

	return marshalToAttributeValue(fieldVal)
}

// ***********************************************************
// For cleanup
type stack struct {
	s []interface{}
}

var docStack stack

func (s stack) Empty() bool {
	return len(s.s) == 0
}
func (s stack) Peek() interface{} {
	return s.s[len(s.s) - 1]
}
func (s stack) Len() int {
	return len(s.s)
}
func (s *stack) Put(i interface{}) {
	s.s = append(s.s, i)
}
func (s *stack) Pop() interface{} {
	d := s.s[len(s.s) - 1]
	s.s = s.s[:len(s.s) - 1]
	return d
}

// ***********************************************************
// Start test methods

type scalar1 struct {
	DocName   string `key-type:"primary"`
	IValue    int
	FValue    float32
	BoolValue bool
}
type scalar2 struct {
	DocName string `key-type:"primary"`
	V1      interface{}
	V2      interface{}
	V3      interface{}
	V4      interface{}
}
type sliceStruct1 struct {
	DocName string `key-type:"primary"`
	AnArray []string
}
type sliceStruct2 struct {
	DocName string `key-type:"primary"`
	AnArray []float32
}
type sliceStruct3 struct {
	DocName string `key-type:"primary"`
	AnArray []int8
}
type sliceStruct4  struct {
	DocName string `key-type:"primary"`
	AnArray []bool
}
type sliceStruct5 struct {
	DocName string `key-type:"primary"`
	AnArray []interface{}
}
type sliceStruct6 struct {
	DocName string `key-type:"primary"`
	AnArray []map[string]int
}
type intStruct struct {
	I int
	x int
}
type struct1 struct {
	DocName string `key-type:"primary"`
	AnArray []intStruct
}
type map1 struct {
	DocName string `key-type:"primary"`
	AMap    map[int]string
}
type map2 struct {
	DocName string `key-type:"primary"`
	AMap    map[string]string
}
type map3 struct {
	DocName string `key-type:"primary"`
	AMap    map[string]int
}
type map4 struct {
	DocName string `key-type:"primary"`
	AMap    map[float64]int
}

type TestMarshalMap map[string]interface{}
func (tmm *TestMarshalMap) GetPartitionKeyField() (*dynamodb.AttributeDefinition) {
	return &dynamodb.AttributeDefinition{
		AttributeName: aws.String("DocName"),
		AttributeType: aws.String("S"),
	}
}
func (tmm *TestMarshalMap) GetSortKeyField() (*dynamodb.AttributeDefinition) {
	return nil
}
var docMap = TestMarshalMap{
	"DocName": "TestMap",
	"Field_1": true,
	"Amount": 100,
	"inventory_count": []int64{1, 3, 5, 7},
	"items": []interface{}{true, 5, "5x5 boxes", 4.99, "USD"},
	"description": struct {
		D        string
		Category string
	}{"boxes", "cardboard contianers"},
	"s1": []*string{aws.String("sptr1"), aws.String("sptr2")},
	"bool1": []bool{true, false},
}

func TestDynamodbConnection_CreateDocument(t *testing.T) {
	getConnection(t)

	t.Run("Scalar=1", func(t *testing.T) {
		var a = scalar1{DocName:"Scalar=1", IValue:1, FValue:2.2, BoolValue:true}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Scalar=2", func(t *testing.T) {
		var a = scalar2{DocName:"Scalar=2", V1:1, V2:2.2, V3:true, V4:"x"}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=1", func(t *testing.T) {
		var a = sliceStruct1{DocName:"Slice=1", AnArray:[]string{"a", "b", "c", "d"}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=2", func(t *testing.T) {
		var a = sliceStruct2{DocName:"Slice=2", AnArray:[]float32{1.1, 2.2, 3.3, 4.4}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=3", func(t *testing.T) {
		var a = sliceStruct3{DocName:"Slice=3", AnArray:[]int8{10, 20, 30, 40, 50}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=4", func(t *testing.T) {
		var a = sliceStruct4{DocName:"Slice=4", AnArray:[]bool{true, false, false, true}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=5", func(t *testing.T) {
		var a = sliceStruct5{DocName:"Slice-5", AnArray:[]interface{}{"a", 1, 2.2, true}}
		_, err := docTest.CreateDocument(testDocTable, a)
		docStack.Put(a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=6", func(t *testing.T) {
		var a = sliceStruct6{DocName:"Slice=6", AnArray:[]map[string]int{
			{"a":1, "b":2, "c": 3},
			{"y":5, "z":6},
		}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Slice=7", func(t *testing.T) {
		var a = struct1{DocName:"Slice=7", AnArray:[]intStruct{{I:0, x:0}, {I:1, x:0}, {I:2, x:0}}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Map=1", func(t *testing.T) {
		var a = map1{DocName:"Map=1", AMap: map[int]string{0:"1", 2:"3", 4:"5"}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Map=2", func(t *testing.T) {
		var a = map2{DocName:"Map-2", AMap:map[string]string{"a":"1", "b":"3", "c":"5"}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Map=3", func(t *testing.T) {
		var a = map3{DocName:"Map=3", AMap:map[string]int{"a":1, "b":3, "c":5}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Map=4", func(t *testing.T) {
		var a = map4{DocName:"Map=4", AMap:map[float64]int{1.1:1, 2.2:3, 3.3:5}}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Map=5", func(t *testing.T) {
		var a = docMap
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
	t.Run("Intf=1", func(t *testing.T) {
		var a = InterfTest{"Intf=1", "Test GetPartitionKeyField."}
		docStack.Put(a)
		_, err := docTest.CreateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
}

func TestDynamodbConnection_UpdateDocument(t *testing.T) {
	t.Run("Intf=1", func(t *testing.T) {
		var a = InterfTest{"Intf=1", "Test GetPartitionKeyField."}
		a.AValue = "Test field Updated."
		_, err := docTest.UpdateDocument(testDocTable, a)
		testutil.AssertNil(t, err, "CreateDocument() error on insert/create. %s", err)
	})
}

func TestDynamodbConnection_DeleteDocument(t *testing.T) {
	var err error
	getConnection(t)

	for i := 0; !docStack.Empty(); i++ {
		doc := docStack.Pop()
		t.Run(fmt.Sprintf("A=%d", i), func(t *testing.T) {
			err = docTest.DeleteDocument(testDocTable, doc)
			testutil.AssertNil(t, err, "DeleteDocument() fail for doc \"%v\". Error: %v\n", doc, err)
		})
	}
}