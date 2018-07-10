# dynamodb-utils

A kotlin library with utility function for dynamdb 

## DynamoBatchExecutor

DynamoDB allows writing and deleting items in batches of up to 25. However if there is a provisioned throughput exception, some or all the requests may fail. 
The aws sdk does not retry these requests transparently as it does for other operations, but it is left up the client to retry, ideally with exponential backoff. 
This library encapsulates does that for you using rx-kotlin. 

Failed writes are retried up until the attempt limit is reached using exponential backoff with jitter. 

For refernence, [github link to issue](https://github.com/aws/aws-sdk-js/issues/1262)
For details on exponential backoff and jitter in aws see [her](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

## DynamoTableCloner

This library allows you to clone a dynamo table, including hash and sort key,local and secondary indexes, provisioned throughput settings, dynamo streaming even source mappings and autoscaling properties if defined. 
The library also exposes functions that return the aws sdk requests that would create all of the above without applying them so that tables can be partially cloned, and also streaming event sources or autoscaling properties from one table to 
be applied to another 

