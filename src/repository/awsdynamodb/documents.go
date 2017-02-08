package awsdynamodb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
	"repository"
	"reflect"
)

// Query
// @see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html?shortFooter=true#API_Query_RequestSyntax
// partitionKeyName = :partitionkeyval
// partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
// Valid comparisons for the sort key condition are as follows:
// sortKeyName = :sortkeyval - true if the sort key value is equal to :sortkeyval.
// sortKeyName < :sortkeyval - true if the sort key value is less than :sortkeyval.
// sortKeyName <= :sortkeyval - true if the sort key value is less than or equal to :sortkeyval.
// sortKeyName > :sortkeyval - true if the sort key value is greater than :sortkeyval.
// sortKeyName >= :sortkeyval - true if the sort key value is greater than or equal to :sortkeyval.
// sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2 - true if the sort key value is greater than or equal to :sortkeyval1, and less than or equal to :sortkeyval2.
// begins_with ( sortKeyName, :sortkeyval ) - true if the sort key value begins with a particular operand.
//     (You cannot use this function with a sort key that is of type Number.) Note that the function name begins_with is case-sensitive

var (
	// Errors
	MissingPrimaryKey = errors.New("Missing primary key field.")
)


// AWS DynamoDb CreateDocument (PutItem)
// Will look for a primary key field using either using the AwsDocumentKeyRetriever interface, if implemented,
// or looking for the struct tag key-type:"primary".
//
// Looks for a secondary (sort/range) key field using the AwsDocumentKeyRetriever interface, if implemented,
// or looking for the struct tag key-type:"secondary"
//
// Marshals the document into the AWS go-sdk format of map[string]*dynamodb.AttributeValue. If the
// AwsMarshaler inferface is implemented, it calls this method, otherwaise, it traverses the document and
// generates the map based on the data types.
//
// Returns nil, nil if successful, else nil and the AWS error.
func (ddbc *DynamodbConnection) CreateDocument(tableName string, doc interface{}) (interface{}, error) {
	var primaryKey *dynamodb.AttributeDefinition

	primaryKey = GetPartitionKeyField(doc)
	if primaryKey == nil {
		return nil, MissingPrimaryKey
	}

	// Marshal the document into an item
	var awsDoc map[string]*dynamodb.AttributeValue
	awsDoc = MarshalAWS(doc)

	// This prevents an existing document from being overwritten unintentionally.
	// To update, this is removed.
	// Get the hash key, i.e. primary/partition key
	conditionExpr := fmt.Sprintf("attribute_not_exists(%s)", *primaryKey.AttributeName)

	pii := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: awsDoc,
		ConditionExpression: aws.String(conditionExpr),
	}

	_, err := ddbc.svc.PutItem(pii)
	return nil, err
}

// AWS DynamoDb UpdateDocument (PutItem)
// This is a simple update document operation using PutItem w/o the constraint.
// If the document does not exist, it will create a new item. If the item already exists, with the
// same key(s), it will be replaced.
func (ddbc *DynamodbConnection) UpdateDocument(tableName string, doc interface{}) (interface{}, error) {

	// Marshal the document into an item
	var awsDoc map[string]*dynamodb.AttributeValue
	awsDoc = MarshalAWS(doc)

	pii := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: awsDoc,
	}

	_, err := ddbc.svc.PutItem(pii)
	return nil, err
}

func (ddbc *DynamodbConnection) GetDocument(tableName string, doc interface{}) (map[string]interface{}, error) {

	primaryKey := GetPartitionKeyField(doc)
	if primaryKey == nil {
		return map[string]interface{}{}, MissingPrimaryKey
	}
	pAv := GetAttributeValue(doc, *primaryKey.AttributeName)
	var keys = map[string]*dynamodb.AttributeValue{*primaryKey.AttributeName: pAv}

	secondaryKey := GetSortKeyField(doc)
	// Look for the key in struct tag: key-type:"secondary"  or call an interface method, if defined.
	// If found, add to the keys, else ignore
	if secondaryKey = GetSortKeyField(doc); secondaryKey != nil {
		sAv := GetAttributeValue(doc, *secondaryKey.AttributeName)
		keys[*secondaryKey.AttributeName] = sAv
	}

	gii := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: keys,
	}

	_, err := ddbc.svc.GetItem(gii)
	if err != nil {
		return map[string]interface{}{}, err
	}

	return map[string]interface{}{}, err
}

// AWS DynamoDb DeleteDocument
// Will look for a primary key field using either using the AwsDocumentKeyRetriever interface, if implemented,
// or looking for the struct tag key-type:"primary".
//
// Looks for a secondary (sort/range) key field using the AwsDocumentKeyRetriever interface, if implemented,
// or looking for the struct tag key-type:"secondary"
//
// Returns nil on success, else AWS error.
func (ddbc *DynamodbConnection) DeleteDocument(tableName string, doc interface{}) error {
	var primaryKey, secondaryKey *dynamodb.AttributeDefinition
	var pAv, sAv *dynamodb.AttributeValue

	// Look for the key in struct tag: key-type:"primary" or call an interface method, if defined.
	primaryKey = GetPartitionKeyField(doc)
	if primaryKey == nil {
		return MissingPrimaryKey
	}
	// Get the values for the primary key
	pAv = GetAttributeValue(doc, *primaryKey.AttributeName)
	var keys = map[string]*dynamodb.AttributeValue{*primaryKey.AttributeName: pAv}

	// Look for the key in struct tag: key-type:"secondary"  or call an interface method, if defined.
	// If found, add to the keys, else ignore
	if secondaryKey = GetSortKeyField(doc); secondaryKey != nil {
		sAv = GetAttributeValue(doc, *secondaryKey.AttributeName)
		keys[*secondaryKey.AttributeName] = sAv
	}

	_, err := ddbc.svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: keys},
	)

	return err
}

// Get the field name and AWS scalar type for the tag "key-type", and "aws-type".
// Valid values for the tag "aws-type" are "S", "N", or "B".
// Returns *dynamodb.AttributeDefinition, or if a tag is not found, or there is an error nil is returned.
func getAttributeDefinitionByTag(v interface{}, tagName string, targetValue string) (*dynamodb.AttributeDefinition) {
	var tagValue *string
	var pos int = 0
	var fieldName *string

	for pos = 0; pos >= 0 && (tagValue == nil || *tagValue != targetValue); pos++ {
		fieldName, tagValue, pos = repository.GetFieldByTagName(v, tagName, pos)
		if pos < 0 {
			// No more "key-type" tags found.
			break
		}
	}

	// Did not find a match for tag == tagName && tagValue
	if fieldName == nil {
		return nil
	}

	// Look for a tag indicating the field data type.
	fieldType := repository.GetTagByFieldName(v, *fieldName, "aws-type")
	if fieldType == nil {
		// Tag "aws-type" was not found, thus we make an intelligent guess.
		// This will only return a type S or N, or nil
		field, _ := reflect.TypeOf(v).FieldByName(*fieldName)
		x := mapToAwsType(field.Type)
		return &dynamodb.AttributeDefinition{AttributeName:fieldName, AttributeType: aws.String(x)}
	}

	return &dynamodb.AttributeDefinition{AttributeName:fieldName, AttributeType: fieldType}
}

