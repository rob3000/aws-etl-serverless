import { StackContext, use, Api, Function } from "sst/constructs";
import { ETLStack } from './etl';
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam";

export function APIStack({ stack }: StackContext) {

    const { cleanBucket } = use(ETLStack)

    const searchFn = new Function(stack, 'search-fn', {
        handler: 'packages/functions/src/search.go',
        bind: [cleanBucket],
        environment: {
            S3_LOCATION: cleanBucket.bucketName,
        }
    })

    const iamAccount = `${stack.region}:${stack.account}`

    searchFn.addToRolePolicy(new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "athena:StartQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
        ],
        resources: [
          `arn:aws:athena:${iamAccount}:workgroup/primary`,
        ],
    }))

    searchFn.addToRolePolicy(new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
        ],
        resources: [
          `arn:aws:glue:${iamAccount}:database/traffic_camera`,
          `arn:aws:glue:${iamAccount}:table/traffic_*`,
          `arn:aws:glue:${iamAccount}:catalog`
        ],
    }))


    const api = new Api(stack, 'api', {
        routes: {
            "GET /filters": "packages/functions/src/filters.go",
            "POST /search": searchFn
        }
    })
  

    stack.addOutputs({
        apiUrl: api.url
    })
}