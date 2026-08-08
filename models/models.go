package models

import (
	"os"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

var User = func() *user.User {
	return user.NewUser(new(user.Redis))
}
var Article = func() *article.Article {
	if strings.EqualFold(os.Getenv("STORAGE_BACKEND"), "dynamodb") {
		return article.NewArticle(new(article.DynamoDB))
	}

	// Redis is the default because the rest of the application already requires
	// it and this keeps a local installation self-contained.
	return article.NewArticle(new(article.Redis))
}
var Board = func() *board.Board {
	cache := new(board.Redis)
	if strings.EqualFold(os.Getenv("STORAGE_BACKEND"), "dynamodb") {
		return board.NewBoard(new(board.DynamoDB), cache)
	}
	return board.NewBoard(cache, cache)
}
