package main

import (
	"github.com/jjoc007/poc-crud-dynamo-pluggeable-lib/repository"
)

// NewRepository creates and returns a new Websocket repository instance
func NewRepository(tableName string) repository.Repository {
	return repository.NewRepository(tableName)
}
