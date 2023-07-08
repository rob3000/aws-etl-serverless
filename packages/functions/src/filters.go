package main

import (
	"encoding/json"

	"github.com/rob3000/aws-etl-serverless/db"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func Handler(request events.APIGatewayV2HTTPRequest) (events.APIGatewayProxyResponse, error) {
	var filters = db.FilterOptions()

	if filters == nil {
		return events.APIGatewayProxyResponse{
			Body: `{"error":true}`,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			StatusCode: 404,
		}, nil
	}

	response, _ := json.Marshal(filters)

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
