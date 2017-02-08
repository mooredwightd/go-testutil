package awsdynamodb

// @see http://docs.aws.amazon.com/sdk-for-go/api/
import (
	"fmt"
	"logger"
	"reflect"
	"repository/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsc "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"os"
)

type DynamodbConnection struct {
	sess       *session.Session
	svc        *dynamodb.DynamoDB
	credential *credentials.Credential
	l          *logger.Log
	configKey  string
}

var (
	credentialFile string = "aws.json"
	appName = "aws-dynamodb"
	logFilename = "aws-dynamodb"
)

/* Setting credentials for aws-sdk-go
sess, err := session.NewSession(&aws.Config{
    Region:      aws.String("us-west-2"),
    Credentials: credentials.NewStaticCredentials(<id>, <secret>, <token>),
})
*/

// @see https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sessions.html
func NewClient() (ddbc *DynamodbConnection, err error) {

	f, err := logger.DailyFile(logFilename)
	if err != nil {
		panic(fmt.Errorf("Error opening log file %s", logFilename))
	}

	ddbc = &DynamodbConnection{l: logger.LogManger(appName, f)}

	awsC, cErr := credentials.AWS(credentialFile)
	if cErr != nil {
		ddbc.l.Error("dynamodb", "NewDynamicDbClient", map[string]string{"message": err.Error()})
	}

	cv, _ := awsC.Retrieve()
	os.Setenv("AWS_ACCESS_KEY_ID", cv.AccessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", cv.SecretAccessKey)
	ddbc.credential = credentials.New(awsC)
	//fmt.Printf("credential: %+v\n", awsC)

	ddbc.sess, err = session.NewSession(&aws.Config{
		Region:      aws.String(awsC.Region()),
		Credentials: awsc.NewCredentials(awsC), })

	if err != nil {
		ddbc.l.Error("dynamodb", "NewDynamoDbClient", map[string]string{
			"region": awsC.Region(), "message": err.Error()})

		return nil, err
	}
	ddbc.svc = dynamodb.New(ddbc.sess)

	return ddbc, nil
}


// Checks if the variable v (of an interfaceable type) implements the Interface of type t.
// Panics if t (type) is not type reflect.Interface, or the variable (v) is nil.
// Returns the interface or nil if it is not implemented.
func getInterface(v interface{}, t reflect.Type) (interfaceRef interface{}) {
	dType := reflect.TypeOf(v)
	if dType.Implements(t) {
		interfaceRef = v
	} else if reflect.PtrTo(dType).Implements(t) {
		ptr := reflect.New(dType)
		ptr.Elem().Set(reflect.ValueOf(v))
		interfaceRef = ptr.Interface()
	} else {
		interfaceRef = nil
	}
	return
}


