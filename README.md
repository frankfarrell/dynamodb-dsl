# dynamodb-utils

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/7722315c9da948bc876aa6993d7e96bb)](https://www.codacy.com/app/frankfarrell/dynamo-batch?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=frankfarrell/dynamo-batch&amp;utm_campaign=Badge_Grade)
[![Build Status](https://travis-ci.org/frankfarrell/dynamodb-utils.svg?branch=master)](https://travis-ci.org/frankfarrell/dynamodb-utils)

A kotlin library with utility function for dynamdb 

## Get it

The easiest thing is to use [jitpack](jitpack.io). Add this to your gradle file
```
repositories {
        jcenter()
        maven { url "https://jitpack.io" }
   }
   dependencies {
         implementation 'com.github.frankfarrell:dynamodb-utils:0.0.1'
   }
```

## DynamoBatchExecutor

DynamoDB allows writing and deleting items in batches of up to 25. However if there is a provisioned throughput exception, some or all the requests may fail. 
The aws sdk does not retry these requests transparently as it does for other operations, but it is left up the client to retry, ideally with exponential backoff. 
This library encapsulates does that for you using rx-kotlin. 

Failed writes are retried up until the attempt limit is reached using exponential backoff with jitter. 

### Examples
```


```

For refernence, [github link to issue](https://github.com/aws/aws-sdk-js/issues/1262)
For details on exponential backoff and jitter in aws see [her](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

## DynamoTableCloner

This library allows you to clone a dynamo table, including hash and sort key,local and secondary indexes, provisioned throughput settings, dynamo streaming even source mappings and autoscaling properties if defined. 
The library also exposes functions that return the aws sdk requests that would create all of the above without applying them so that tables can be partially cloned, and also streaming event sources or autoscaling properties from one table to 
be applied to another 

