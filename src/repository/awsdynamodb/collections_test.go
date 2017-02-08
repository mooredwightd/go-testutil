package awsdynamodb

import (
	"fmt"
	"repository"
	"testing"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/go-testutil/testutil"
)

func TestDynamodbConnection_ListCollections(t *testing.T) {
	var err error
	getConnection(t)

	tblList, err := docTest.ListCollections()
	testutil.AssertNil(t, err, "Expected no error <nil> on ListCollections(). %v", err)
	testutil.AssertGreaterThan(t, len(tblList), 0, "Expected at least one table/collection.")
	t.Logf("TestDynamodbConnection_ListCollections: Tables:%v", tblList)
}

func TestDynamodbConnection_CreateCollection(t *testing.T) {
	getConnection(t)

	// Positive case: a document w/ a primary key defined.
	t.Run("A=1", func(t *testing.T) {
		attrDef := GetPartitionKeyField(testDoc1)
		//sKeyName, sType := ddbc.GetSortKey(testDoc1)

		kdef := repository.KeyDefinition{
			Primary: &repository.AttributeDef{
				Name: *attrDef.AttributeName,
				Type: *attrDef.AttributeType},
		}
		id, err := docTest.CreateCollection(testCreateTableName, kdef)
		testutil.AssertNil(t, err, "Expected no error <nil> on CreateCollection()")
		testutil.AssertNotEmptyString(t, id, "Expected valid ARN. Actual is empty string.")
		t.Logf("TestDynamodbConnection_CreateCollection: Tables name/arn:%s/%s", testCreateTableName, id)
	})
	// Negative case: a document w/o a primary key defined.
	// Positive case: a document w/ a primary key defined.
	t.Run("B=1", func(t *testing.T) {
		attrDef := GetPartitionKeyField(testDocBad1)
		//sKeyName, sType := ddbc.GetSortKey(testDoc1)
		if attrDef == nil {
			attrDef = &dynamodb.AttributeDefinition{
				AttributeName:aws.String(""), AttributeType: aws.String("NULL")}
		}

		kdef := repository.KeyDefinition{
			Primary: &repository.AttributeDef{
				Name: *attrDef.AttributeName,
				Type: *attrDef.AttributeType},
		}
		_, err := docTest.CreateCollection(testCreateTableName, kdef)
		testutil.AssertNotNil(t, err, "CreateCollection B=1, Expected error on CreateCollection() w/o primary key. %v", err)
	})
}

func TestDynamodbConnection_DropCollection(t *testing.T) {
	getConnection(t)
	err := docTest.DropCollection(testCreateTableName)
	testutil.AssertNil(t, err, fmt.Sprintf("Expected no error <nil> on DropCollection().%s", ""))
}
