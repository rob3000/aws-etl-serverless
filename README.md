# AWS ETL Athena

An example serverless app created with SST. 

[Blog post](https://rob3000.com/articles/aws-etl-stepfunction-athena-cdk)

## Getting Started

### Download data

curl -o ./traffic-speed-camera-locations.csv https://www.data.act.gov.au/api/views/426s-vdu4/rows.csv?accessType=DOWNLOAD
curl -o ./traffic-camera-offences-and-fines.csv https://www.data.act.gov.au/api/views/2sx9-4wg7/rows.csv?accessType=DOWNLOAD

### Upload to S3

aws s3 cp ./traffic-speed-camera-locations.csv s3://raw-etl-ingestion/traffic_camera_locations/lookup/traffic-speed-camera-locations.csv
aws s3 cp ./traffic-camera-offences-and-fines.csv s3://raw-etl-ingestion/traffic_offences/data/traffic-camera-offences-and-fines.csv

## Commands

### `npm run dev`

Starts the Live Lambda Development environment.

### `npm run build`

Build your app and synthesize your stacks.

### `npm run deploy [stack]`

Deploy all your stacks to AWS. Or optionally deploy, a specific stack.

### `npm run remove [stack]`

Remove all your stacks and all of their resources from AWS. Or optionally removes, a specific stack.

## Documentation

Learn more about the SST.

- [Docs](https://docs.sst.dev/)
- [sst](https://docs.sst.dev/packages/sst)
