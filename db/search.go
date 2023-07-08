package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type SearchParams struct {
	Year  string `default:"2023"`
	Month string
	State string `default:"WA"`
}

type Result struct {
	OffenceLocationCode int
	CameraLocationCode  int
	SumInfCount         int
	CameraTypeOffence   string
	Latitude            int
	Longitude           int
}

type SearchResult struct {
	Error *string             `json:"error"`
	Data  []map[string]string `json:"data"`
	Count int                 `json:"count"`
}

func Search(p SearchParams) SearchResult {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	s3Location := os.Getenv("S3_LOCATION")

	if s3Location == "" {
		log.Fatal("No bucket location set.")
	}

	var location string = fmt.Sprintf("s3://%s/Athena/", s3Location)

	var query string = `SELECT 
		offence_location_code,
		camera_location_code,
		sum_inf_count,
		camera_type_offence,
		latitude,
		longitude
		FROM offences_view WHERE offence_year = ? AND rego_state = ?`

	client := athena.NewFromConfig(cfg)

	result, err := client.StartQueryExecution(context.TODO(), &athena.StartQueryExecutionInput{
		QueryString:         &query,
		ExecutionParameters: []string{p.Year, p.State},
		WorkGroup:           aws.String("primary"),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String("traffic_camera"),
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: &location,
		},
		ResultReuseConfiguration: &types.ResultReuseConfiguration{
			ResultReuseByAgeConfiguration: &types.ResultReuseByAgeConfiguration{
				Enabled:         true,
				MaxAgeInMinutes: aws.Int32(60),
			},
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	var queryResult *athena.GetQueryExecutionOutput
	duration := time.Duration(500) * time.Millisecond // Pause for 500ms

	queryExecutionId := result.QueryExecutionId

	for {
		queryResult, err = client.GetQueryExecution(context.TODO(), &athena.GetQueryExecutionInput{
			QueryExecutionId: queryExecutionId,
		})

		if err != nil {
			fmt.Println(err)
			break
		}

		if queryResult.QueryExecution.Status.State == "SUCCEEDED" {
			break
		}

		if queryResult.QueryExecution.Status.State == "FAILED" {
			break
		}

		fmt.Println("waiting....")
		time.Sleep(duration)
	}

	if queryResult.QueryExecution.Status.State == "SUCCEEDED" {
		op, err := client.GetQueryResults(context.TODO(), &athena.GetQueryResultsInput{
			QueryExecutionId: queryExecutionId,
		})

		if err != nil {
			e := err.Error()
			fmt.Println(e)
			return SearchResult{
				Error: &e,
				Data:  []map[string]string{},
				Count: 0,
			}
		}

		var rc []map[string]string

		var columns []string

		for _, colInfo := range op.ResultSet.ResultSetMetadata.ColumnInfo {
			columns = append(columns, *colInfo.Name)
		}

		for i, element := range op.ResultSet.Rows {
			// Skip first line which is the header
			if i == 0 {
				continue
			}

			m := make(map[string]string)

			for j, el := range element.Data {
				m[columns[j]] = *el.VarCharValue
			}

			rc = append(rc, m)
		}

		return SearchResult{
			Error: nil,
			Data:  rc,
			Count: len(rc),
		}
	}

	return SearchResult{
		Error: nil,
		Data:  []map[string]string{},
	}
}
