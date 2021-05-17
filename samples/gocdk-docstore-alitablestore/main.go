package main

import (
	"context"
	"log"

	"gocloud.dev/docstore"
	_ "gocloud.dev/docstore/alitablestore"
)


const (
	url = "tablestore://ali.tablestore.com?collection=gamble_play_users_v1&instance=learn&access_key" +
		"=LTAI4FjfyAruM5DFqvMS4rLS&access_secret=Q1Fm5LEly5VGevxI1XNqOVvQzW0FdA&partition_key=periodId&sort_key=userId"
)

type Entry struct {
	PeriodID   string `docstore:"periodId"`
	Period     string `docstore:"period"`
	IsWin      bool   `docstore:"isWin"`
	UserID     int64  `docstore:"userId"`
	Username   string `docstore:"nickname"`
	CreateTime int64  `docstore:"createTime"`
	UpdateTime int64  `docstore:"updateTime"`
	LuckyTime  int64  `docstore:"luckyTime"`
	Restore    int64  `docstore:"restore"`
}

func main() {

	// 这里如果要切换成 dynamodb, 只需要调整url 即可。
	coll, err := docstore.OpenCollection(context.Background(), url)
	if err != nil {
		log.Fatal(err)
	}
	defer coll.Close()

	ctx := context.Background()
	doc := Entry{
		Username: "testusername",
		IsWin: false,
		Restore: 12345,
		PeriodID: "100FB1CB-3E76-4255-8A81-29C424111114",
		UserID: int64(11221827),
	}
	err = coll.Put(ctx, &doc)
	if err != nil {
		log.Fatal(err)
	}

	result := coll.Query().
		Where("periodId", "=", doc.PeriodID).
		Where("userId", "=", doc.UserID).Get(ctx)

	err = result.Next(ctx, &doc)
	if err != nil {
		log.Fatal(err)
	}
}
