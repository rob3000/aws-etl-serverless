import { StackContext, Bucket } from "sst/constructs";
import { StateMachine, Chain, StateMachineType, Choice, Condition, Map, Pass, LogLevel, JsonPath, IntegrationPattern } from 'aws-cdk-lib/aws-stepfunctions';
import { AthenaGetQueryResults, AthenaStartQueryExecution } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam";

export function ETLStack({ stack }: StackContext) {

  const rawBucket = new Bucket(stack, 'raw', {
    name: `raw-etl-ingestion`,
  });
  
  const cleanBucket = new Bucket(stack, 'clean', {
    name: `clean-etl-ingestion`,
  });
  
  const rawStorageName = rawBucket.bucketName;
  const cleanStorageName = cleanBucket.bucketName;

  const dbName = 'traffic_camera';
  const trafficOffencesDataName = 'traffic_offences_data_csv';
  const trafficCameraLocationDBName = 'traffic_speed_camera_locations_data_csv'

  const defaultQueryExecution = {
    workGroup: 'primary',
    resultConfiguration: {
      outputLocation: {
        bucketName: cleanStorageName,
        objectKey: 'athena'
      }
    }
  }

  const createDbSQL = `CREATE DATABASE IF NOT EXISTS ${dbName}`;

  // Create Glue DB
  const glueDb =  new AthenaStartQueryExecution(stack, 'glue-db', {
    queryString: createDbSQL,
    ...defaultQueryExecution
  });
  
  const tableLookupSQL = `SHOW TABLES IN ${dbName}`;

  // Check to make sure the table exists.
  const tableLookup = new AthenaStartQueryExecution(stack, 'run-table-lookup', {
    queryString: tableLookupSQL,
    integrationPattern: IntegrationPattern.RUN_JOB,
    ...defaultQueryExecution,
  })

  // Get lookup query results
  const lookupResults = new AthenaGetQueryResults(stack, 'lookup-results', {
    queryExecutionId: JsonPath.stringAt('$.QueryExecution.QueryExecutionId')
  });

  const createDataSQL = `
    CREATE EXTERNAL TABLE ${dbName}.${trafficOffencesDataName}(
      offence_month string,
      rego_state string,
      cit_catg string,
      camera_type string,
      location_code int,
      location_desc string,
      offence_desc string,
      sum_pen_amt int,
      sum_inf_count int,
      sum_with_amt int,
      sum_with_count int
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION 's3://${rawStorageName}/traffic_offences/data/' TBLPROPERTIES ('skip.header.line.count'='1')`;

  // Create our data table.
  const createDataTable = new AthenaStartQueryExecution(stack, 'create-data-table', {
    queryString: createDataSQL,
    ...defaultQueryExecution,
    integrationPattern: IntegrationPattern.RUN_JOB,
  });

  const createLookupTableSQL = `
    CREATE EXTERNAL TABLE ${dbName}.${trafficCameraLocationDBName} (
      camera_type string,
      camera_location_code int,
      location_code string,
      latitude string,
      longitude string,
      location_desc string
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION 's3://${rawStorageName}/traffic_camera_locations/lookup/' TBLPROPERTIES ('skip.header.line.count'='1')`;

  const createLookupTable = new AthenaStartQueryExecution(stack, 'create-lookup-table', {
    queryString: createLookupTableSQL,
    integrationPattern: IntegrationPattern.RUN_JOB,
    ...defaultQueryExecution,
  });

  const parquetDataTable = `traffic_offences_data_parquet`;
  const parquetLookupTable = `traffic_camera_locations_lookup_parquet`;

  const createParquetTableDataSQL = `
    CREATE TABLE IF NOT EXISTS ${dbName}.${parquetDataTable} WITH (
      format='PARQUET',
      parquet_compression='SNAPPY',
      partitioned_by=array['offence_month','offence_year'],
      external_location = 's3://${cleanStorageName}/traffic_offences/optimized-data/'
    ) AS SELECT
      offence_month as offence_date_raw,
      rego_state,
      cit_catg,
      camera_type,
      location_code,
      location_desc,
      offence_desc,
      sum_pen_amt,
      sum_inf_count,
      sum_with_amt,
      sum_with_count,
      substr("offence_month", 1,3) AS offence_month,
      substr("offence_month", 5,5) AS offence_year 
    FROM ${dbName}.${trafficOffencesDataName}
  `;

  const createParquetDataTable = new AthenaStartQueryExecution(stack, 'create-parquet-table-data', {
    queryString: createParquetTableDataSQL,
    ...defaultQueryExecution
  });

  const createParquetLookupTableDataSQL = `
    CREATE TABLE IF NOT EXISTS ${dbName}.${parquetLookupTable} WITH (
      format='PARQUET',
      parquet_compression='SNAPPY',
      external_location = 's3://${cleanStorageName}/traffic_camera_locations/optimized-data-lookup/'
    ) AS SELECT
      camera_type,
      camera_location_code,
      location_code,
      latitude,
      longitude,
      location_desc
    FROM ${dbName}.${trafficCameraLocationDBName}`;

  const createParquetLookupDataTable = new AthenaStartQueryExecution(stack, 'create-parquet-lookup-table-data', {
    queryString: createParquetLookupTableDataSQL,
    ...defaultQueryExecution
  });

  const createViewSQL = `
    CREATE OR REPLACE VIEW offences_view AS SELECT
      a.*,
      lkup.* 
      FROM (
        SELECT
            datatab.camera_type as camera_type_offence,
            rego_state,
            offence_month,
            offence_year,
            datatab.location_code as offence_location_code,
            SUM(sum_pen_amt) AS sum_pen_amt,
            SUM(sum_inf_count) AS sum_inf_count
        FROM ${dbName}.${parquetDataTable} datatab
        WHERE datatab.rego_state is NOT null
        GROUP BY datatab.location_code, offence_month, offence_year, datatab.camera_type, rego_state
      ) a,
      ${parquetLookupTable} lkup WHERE lkup.camera_location_code = a.offence_location_code
  `;
  const createView = new AthenaStartQueryExecution(stack, 'create-view', {
    queryString: createViewSQL,
    ...defaultQueryExecution,
    queryExecutionContext: {
        databaseName: dbName
    }
  });

  const insertDataSQL = `
    INSERT INTO ${dbName}.${parquetDataTable} 
    SELECT 
    offence_month as offence_date_raw,
      rego_state,
      cit_catg,
      camera_type,
      location_code,
      location_desc,
      offence_desc,
      sum_pen_amt int,
      sum_inf_count int,
      sum_with_amt int,
      sum_with_count int,
      substr(\"offence_date_raw\",1,3) offence_month,
      substr(\"offence_date_raw\",4,5) AS offence_year 
    FROM ${dbName}.${trafficOffencesDataName}
  `;

  // Insert data.
  const insertNewParquetData = new AthenaStartQueryExecution(stack, 'insert-parquet-data', {
    queryString: insertDataSQL,
    ...defaultQueryExecution
  });

  const passStep = new Pass(stack, 'pass-step');

  const checkAllTables = new Map(stack, 'check-all-tables', {
    inputPath: '$.ResultSet',
    itemsPath: '$.Rows',
    maxConcurrency: 0,
  }).iterator(
    new Choice(stack, 'CheckTable')
    .when(
      Condition.stringMatches('$.Data[0].VarCharValue', '*data_csv'),
      passStep
    )
    .when(
      Condition.stringMatches('$.Data[0].VarCharValue', '*data_parquet'),
      insertNewParquetData
    )
    .otherwise(passStep)
  )

  const logGroup = new LogGroup(stack, 'etl-log-group', {
    retention: RetentionDays.TWO_WEEKS
  })

  const sfn = new StateMachine(stack, 'process-data', {
    logs: {
      includeExecutionData: true,
      level: LogLevel.ALL,
      destination: logGroup
    },
    stateMachineName: 'athena-etl',
    stateMachineType: StateMachineType.STANDARD,
    definition: Chain.start(
      glueDb.next(
        tableLookup
        .next(lookupResults)
        .next(
          new Choice(stack, 'first-run', {
            comment: 'Sets up for the first time to ensure we have everything we need.',
          })
          .when(
            Condition.isNotPresent('$.ResultSet.Rows[0].Data[0].VarCharValue'),
            createDataTable.next(
              createLookupTable.next(
                createParquetDataTable.next(
                  createParquetLookupDataTable.next(
                    createView
                  )
                )
              )
            )
          ).when(
            Condition.isPresent('$.ResultSet.Rows[0].Data[0].VarCharValue'),
            checkAllTables
          )
        )
      )
    )
  })

  sfn.addToRolePolicy(new PolicyStatement({
    effect: Effect.ALLOW,
    actions: [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload",
      "s3:CreateBucket",
      "s3:PutObject"
    ],
    resources: [
      cleanBucket.bucketArn,
      `${cleanBucket.bucketArn}/*`,
      rawBucket.bucketArn,
      `${rawBucket.bucketArn}/*`
    ],
  }))

  const iamAccount = `${stack.region}:${stack.account}`

  sfn.addToRolePolicy(new PolicyStatement({
    effect: Effect.ALLOW,
    actions: [
      "glue:CreateDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:DeleteDatabase",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition"
    ],
    resources: [
      `arn:aws:glue:${iamAccount}:database/*`,
      `arn:aws:glue:${iamAccount}:table/*`,
      `arn:aws:glue:${iamAccount}:catalog`
    ],
  }))

  sfn.addToRolePolicy(new PolicyStatement({
    effect: Effect.ALLOW,
    actions: [
      "athena:getQueryResults",
      "athena:startQueryExecution",
      "athena:stopQueryExecution",
      "athena:getQueryExecution",
      "athena:getDataCatalog"
    ],
    resources: [
      `arn:aws:athena:${iamAccount}:datacatalog/*`,
      `arn:aws:athena:${iamAccount}:workgroup/*`
    ],
  }))

  return {
    rawBucket,
    cleanBucket,
  }
}