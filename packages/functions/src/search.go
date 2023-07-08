package main

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/rob3000/aws-etl-serverless/db"
)

func Handler(request events.APIGatewayV2HTTPRequest) (events.APIGatewayProxyResponse, error) {
	// Handles passing the inpputs from the frontend and running the athena query.

	params := request.QueryStringParameters

	var results = db.Search(db.SearchParams{
		Year:  params["year"],
		Month: params["month"],
		State: params["state"],
	})

	response, _ := json.Marshal(results)

	return events.APIGatewayProxyResponse{
		Body: string(response),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		StatusCode: 200,
	}, nil
}

func main() {
	lambda.Start(Handler)
}
