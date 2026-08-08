package article

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const tableName string = "articles"

// table: code, board, content
type DynamoDB struct{}

func (DynamoDB) Find(code string, a *Article) {
	if err := (DynamoDB{}).FindE(code, a); err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error("DynamoDB Find Article Failed")
	}
}

// FindE preserves AWS and decoding failures for reliable comment producers.
// A missing item remains a valid empty result, matching the Redis driver.
func (DynamoDB) FindE(code string, a *Article) error {
	dynamo := dynamodb.New(session.New())
	result, err := dynamo.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Code": {
				S: aws.String(code),
			},
		},
	})
	if err != nil {
		return err
	}

	if len(result.Item) == 0 {
		return nil
	}

	a.Code, err = requiredDynamoString(result.Item, "Code")
	if err != nil {
		return fmt.Errorf("DynamoDB article %s: %w", code, err)
	}
	a.Title = dynamoString(result.Item, "Title")
	a.Link = dynamoString(result.Item, "Link")
	a.Date = dynamoString(result.Item, "Date")
	a.Author = dynamoString(result.Item, "Author")
	a.Board, err = requiredDynamoString(result.Item, "Board")
	if err != nil {
		return fmt.Errorf("DynamoDB article %s: %w", code, err)
	}
	if value := result.Item["ID"]; value != nil {
		if err := dynamodbattribute.Unmarshal(value, &a.ID); err != nil {
			return fmt.Errorf("decode DynamoDB article %s ID: %w", code, err)
		}
	}
	if value := result.Item["PushSum"]; value != nil {
		if err := dynamodbattribute.Unmarshal(value, &a.PushSum); err != nil {
			return fmt.Errorf("decode DynamoDB article %s PushSum: %w", code, err)
		}
	}
	if lastPush := dynamoString(result.Item, "LastPushDateTime"); lastPush != "" {
		if a.LastPushDateTime, err = time.Parse(time.RFC3339, lastPush); err != nil {
			return fmt.Errorf("decode DynamoDB article %s LastPushDateTime: %w", code, err)
		}
	}
	comments, err := requiredDynamoString(result.Item, "Comments")
	if err != nil {
		return fmt.Errorf("DynamoDB article %s: %w", code, err)
	}
	if err = json.Unmarshal([]byte(comments), &a.Comments); err != nil {
		myutil.LogJSONDecode(err, comments)
		return fmt.Errorf("decode DynamoDB article %s Comments: %w", code, err)
	}
	if a.Comments == nil {
		a.Comments = make(Comments, 0)
	}
	return nil
}

func dynamoString(item map[string]*dynamodb.AttributeValue, name string) string {
	value := item[name]
	if value == nil || value.S == nil {
		return ""
	}
	return aws.StringValue(value.S)
}

func requiredDynamoString(item map[string]*dynamodb.AttributeValue, name string) (string, error) {
	value := item[name]
	if value == nil || value.S == nil {
		return "", fmt.Errorf("has no string %s value", name)
	}
	result := aws.StringValue(value.S)
	if strings.TrimSpace(result) == "" {
		return "", fmt.Errorf("has empty %s value", name)
	}
	return result, nil
}

func (DynamoDB) Save(a Article) error {
	commentsJSON, err := json.Marshal(a.Comments)
	if err != nil {
		myutil.LogJSONEncode(err, a)
		return err
	}
	dynamo := dynamodb.New(session.New())
	_, err = dynamo.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: aws.String(strconv.Itoa(a.ID)),
			},
			"Code": {
				S: aws.String(a.Code),
			},
			"Title": {
				S: aws.String(a.Title),
			},
			"Link": {
				S: aws.String(a.Link),
			},
			"Date": {
				S: aws.String(a.Date),
			},
			"Author": {
				S: aws.String(a.Author),
			},
			"Comments": {
				S: aws.String(string(commentsJSON)),
			},
			"LastPushDateTime": {
				S: aws.String(a.LastPushDateTime.Format(time.RFC3339)),
			},
			"Board": {
				S: aws.String(a.Board),
			},
			"PushSum": {
				N: aws.String(strconv.Itoa(a.PushSum)),
			},
		},
		TableName: aws.String(tableName),
	})

	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error("DynamoDB Save Article Failed")
	}
	return err
}

func (DynamoDB) Delete(code string) error {
	dynamo := dynamodb.New(session.New())
	_, err := dynamo.DeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Code": {
				S: aws.String(code),
			},
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error("DynamoDB Delete Article Failed")
	}
	return err
}
