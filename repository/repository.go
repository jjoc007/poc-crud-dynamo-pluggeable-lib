package repository


import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/jjoc007/poc-crud-dynamo-pluggeable-lib/config"
	"github.com/jjoc007/poc-crud-dynamo-pluggeable-lib/config/db"

	"github.com/jjoc007/poc-crud-dynamo-pluggeable-lib/log"
)

// Repository describes the lock repository.
type Repository interface {
	Create(interface{}) error
	Update(interface{}) error
	GetByID(config.IDRepository) (interface{}, error)
	Delete(config.IDRepository) error
}

// NewRepository creates and returns a new repository instance
func NewRepository(tableName string) Repository {
	database, err := db.NewDynamoDBStorage()
	if err != nil {
		panic(err)
	}

	return &repository{
		database: database.GetConnection().(*dynamodb.DynamoDB),
		table:    tableName,
	}
}

type repository struct {
	database *dynamodb.DynamoDB
	table    string
}

func (s *repository) Create(resource interface{}) (err error) {
	log.Logger.Debug().Msgf("Adding a new row [%+v] ", resource)

	av, err := dynamodbattribute.MarshalMap(resource)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	_, err = s.database.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      av,
	})

	return nil
}

func (s *repository) Update(resource interface{}) (err error) {
	log.Logger.Debug().Msgf("Updating a new animal [%+v] ", resource)

	av, err := dynamodbattribute.MarshalMap(resource)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	_, err = s.database.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      av,
	})

	return nil
}

func (s *repository) GetByID(id config.IDRepository) (row interface{}, err error) {
	log.Logger.Debug().Msgf("Getting Row by ID")

	result, err := s.database.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]*dynamodb.AttributeValue{
			id.Name: {
				S: aws.String(id.Value),
			},
		},
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if result.Item == nil {
		return nil, errors.New("row not found")
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &row)
	if err != nil {
		return
	}

	return
}

func (s repository) Delete(id config.IDRepository) (err error) {
	log.Logger.Debug().Msgf("Deleting an row [%s] ", id)

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			id.Name: {
				S: aws.String(id.Value),
			},
		},
		TableName: aws.String(s.table),
	}

	_, err = s.database.DeleteItem(input)
	if err != nil {
		fmt.Println("Got error calling DeleteItem")
		fmt.Println(err.Error())
		return
	}

	return nil
}
