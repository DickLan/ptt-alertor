package board

import (
	"encoding/json"
	"fmt"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
)

const tableName string = "boards"

// column: Board, Articles
type DynamoDB struct {
}

func (DynamoDB) GetArticles(boardName string) (articles article.Articles) {
	articles, _, err := (DynamoDB{}).GetArticlesE(boardName)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error("DynamoDB Find Board Failed")
	}
	return articles
}

// GetArticlesE distinguishes a missing board cursor from an AWS or decoding
// failure. Reliable notification producers must not turn either failure into a
// fresh baseline that advances past unseen articles.
func (DynamoDB) GetArticlesE(boardName string) (articles article.Articles, initialized bool, err error) {
	dynamo := dynamodb.New(session.New())
	result, err := dynamo.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"Board": {
				S: aws.String(boardName),
			},
		},
	})
	if err != nil {
		return nil, false, err
	}

	if len(result.Item) == 0 {
		return nil, false, nil
	}

	attribute, exists := result.Item["Articles"]
	if !exists || attribute == nil || attribute.S == nil {
		return nil, true, fmt.Errorf("DynamoDB board %s has no Articles value", boardName)
	}
	articlesJSON := aws.StringValue(attribute.S)
	if articlesJSON == "" {
		return make(article.Articles, 0), true, nil
	}
	if err = json.Unmarshal([]byte(articlesJSON), &articles); err != nil {
		myutil.LogJSONDecode(err, articlesJSON)
		return nil, true, err
	}
	if articles == nil {
		articles = make(article.Articles, 0)
	}
	return articles, true, nil
}

func (DynamoDB) Save(boardName string, articles article.Articles) error {
	articlesJSON, err := json.Marshal(articles)
	if err != nil {
		myutil.LogJSONEncode(err, articles)
		return err
	}

	dynamo := dynamodb.New(session.New())
	_, err = dynamo.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"Board": {
				S: aws.String(boardName),
			},
			"Articles": {
				S: aws.String(string(articlesJSON)),
			},
		},
		TableName: aws.String(tableName),
	})

	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error("DynamoDB Save Board Failed")
	}
	return err
}

func (DynamoDB) Delete(boardName string) error {
	dynamo := dynamodb.New(session.New())
	_, err := dynamo.DeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Board": {
				S: aws.String(boardName),
			},
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error("DynamoDB Delete Board Failed")
	}

	return err
}
