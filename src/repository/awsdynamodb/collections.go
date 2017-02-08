package awsdynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"repository"
	"time"
)

const ()

// List all DynamoDb tables accessible by the current user.
// Assumes the list is managable to fit in a typical []string.
// Returns [] string of table names and nil on success. Otherwise, an error.
func (ddbc *DynamodbConnection) ListCollections() ([]string, error) {
	var tables []string

	err := ddbc.svc.ListTablesPages(&dynamodb.ListTablesInput{},
		func(page *dynamodb.ListTablesOutput, lastPage bool) bool {
			for _, v := range page.TableNames {
				// Assumes # of tables is managable
				tables = append(tables, *v)
			}
			return !lastPage
		})

	if err != nil {
		ddbc.l.Error("dynamodb", "ListCollections", map[string]string{"message": err.Error()})
		return []string{}, err
	}

	return tables, nil
}

func (ddbc *DynamodbConnection) CreateCollection(collName string, key repository.KeyDefinition) (string, error) {
	var RCU = int64(5)  // read capacity units
	var WCU = int64(2)  // write capacity units

	// Setup the primary key/hash attribute
	attrList := []*dynamodb.AttributeDefinition{
		&dynamodb.AttributeDefinition{
			AttributeName:aws.String(key.Primary.Name),
			AttributeType:aws.String(key.Primary.Type),
		},
	}

	kse := []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(key.Primary.Name),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}

	// If a sort key is specified, setup the attribute
	if key.Sort != nil {
		attrList = append(attrList,
			&dynamodb.AttributeDefinition{
				AttributeName:aws.String(key.Sort.Name),
				AttributeType:aws.String(key.Sort.Type),
			},
		)

		kse = append(kse[0:],
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String(key.Sort.Name),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		)
	}

	cti := &dynamodb.CreateTableInput{
		TableName:            &collName,
		AttributeDefinitions: attrList,
		KeySchema:            kse,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(RCU),
			WriteCapacityUnits: aws.Int64(WCU),
		},
	}
	cto, err := ddbc.svc.CreateTable(cti)

	if err != nil {
		ddbc.l.Error("dynamodb", "CreateCollection", map[string]string{"message": err.Error()})
		return "", err
	}
	ddbc.l.Debug("dynamodb", "CreateCollection", map[string]string{
		"name":         *cto.TableDescription.TableName,
		"arn":          *cto.TableDescription.TableArn,
		"status":       *cto.TableDescription.TableStatus,
		"creationDate": cto.TableDescription.CreationDateTime.String(),
	})

	// Table creation is asynchronous. Other actions will receive an error until status indicates ACTIVE,
	return *cto.TableDescription.TableArn, err
}

func (ddbc *DynamodbConnection) DropCollection(collName string) error {
	// Make sure it's in ACTIVE status
	ddbc.waitUntilTableActive(collName)

	dti := &dynamodb.DeleteTableInput{
		TableName: &collName,
	}

	dto, err := ddbc.svc.DeleteTable(dti)
	if err != nil {
		ddbc.l.Error("dynamodb", "DropCollection", map[string]string{"message": err.Error()})
		return err
	}
	ddbc.l.Debug("dynamodb", "DropCollection", map[string]string{
		"name":   *dto.TableDescription.TableName,
		"arn":    *dto.TableDescription.TableArn,
		"status": *dto.TableDescription.TableStatus,
	})
	return nil
}

func (ddbc *DynamodbConnection) waitUntilTableActive(collName string) bool {
	status, err := ddbc.TableStatus(collName)
	for status != dynamodb.TableStatusActive && err == nil {
		time.Sleep(2 * time.Second)
		status, err = ddbc.TableStatus(collName)
	}
	if err != nil {
		return false
	}
	return true
}

func (ddbc *DynamodbConnection) TableStatus(collName string) (string, error) {
	x, err := ddbc.DescribeTable(collName)
	if err != nil {
		ddbc.l.Error("dynamodb", "TableStatus", map[string]string{"message": err.Error()})
		return "", err
	}
	dto := x.(*dynamodb.DescribeTableOutput)
	return *dto.Table.TableStatus, nil
}

func (ddbc *DynamodbConnection) DescribeTable(collName string) (interface{}, error) {
	dti := &dynamodb.DescribeTableInput{
		TableName: &collName,
	}

	dto, err := ddbc.svc.DescribeTable(dti)
	if err != nil {
		ddbc.l.Error("dynamodb", "DescribeTable", map[string]string{"message": err.Error()})
		return nil, err
	}
	ddbc.l.Debug("dynamodb", "DescribeTable", map[string]string{
		"name":   *dto.Table.TableName,
		"arn":    *dto.Table.TableArn,
		"status": *dto.Table.TableStatus,
	})
	return dto, nil
}
