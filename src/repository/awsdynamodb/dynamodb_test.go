package awsdynamodb

import (
	"testing"
	"github.com/go-testutil/testutil"
)

type TestDoc struct {
	DocName     string         `key-type:"primary"`
	Owner       string         `key-type:"secondary"`
	Description string
	Value       float32        `aws-name:"#item_price"`
	Size        int64          `aws-name:"#amount"`
	Currency    string
	SArray      []string
	IArray      []int
	FArray      []float32
	SMap        map[string]int
	IMap        map[int]int
	IntfArray   []interface{}
	StArray     []TestDoc
}
type TestDocBad struct {
	DocName     string
	Owner       string
	Description string
}

var (
	docTest *DynamodbConnection
	testCreateTableName = "testCreate"
	testDoc1 = TestDoc{
		DocName:        "Passengers",
		Owner:       "Jon Spaihts",
		Description: "On a routine journey through space to a new home, two passengers, sleeping in suspended animation, are awakened 90 years too early when their ship malfunctions. As Jim and Aurora face living the rest of their lives on board, with every luxury they could ever ask for, they begin to fall for each other, unable to deny their intense attraction... until they discover the ship is in grave danger. With the lives of 5000 sleeping passengers at stake, only Jim and Aurora can save them all.",
		Value:       9.75,
		Currency:    "USD",
		SArray: []string{"a", "b", "c", "d", "e"},
		IArray: []int{1, 2, 3, 4, 5},
		FArray: []float32{1.0, 2.0, 3.0, 4.0, 5.0},
		IMap: map[int]int{1:10, 2:20, 3:30, 4:40},
		SMap: map[string]int{"a":1, "b":2, "c":3},
		IntfArray: []interface{}{"a", 2, "b", 3.0, true},
		StArray: []TestDoc{{DocName:"st1"}, {DocName:"st2"}, {DocName:"st3"}},
	}
	testDocBad1 = TestDocBad{
		DocName:        "BadMovieName",
		Owner:       "Jon Q. Smith",
		Description: "This document has no primary key idenfied.",
	}
	testDocBad2 = struct {
		OtherName string
		OtherInt  int
		OtherBool bool
	}{"testDocBad2", 1, true}
)

// Test utility function
func getConnection(t *testing.T) {
	var err error
	if docTest == nil {
		docTest, err = NewClient()
		testutil.AssertNil(t, err, "TestDynamodbConnection_ListCollections. Expect <nil> error. %v.", err)
	}
}

func TestNewClient(t *testing.T) {
	getConnection(t)
	testutil.AssertNotNil(t, docTest, "Expected valid *DyanmodbConnection. Actual is <nil>.")
}

/********************
 Examples
********************* */