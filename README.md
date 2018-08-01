# dynamodb-utils

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/7722315c9da948bc876aa6993d7e96bb)](https://www.codacy.com/app/frankfarrell/dynamo-batch?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=frankfarrell/dynamo-batch&amp;utm_campaign=Badge_Grade)
[![Build Status](https://travis-ci.org/frankfarrell/dynamodb-utils.svg?branch=master)](https://travis-ci.org/frankfarrell/dynamodb-utils)
[![](https://jitpack.io/v/frankfarrell/dynamodb-utils.svg)](https://jitpack.io/#frankfarrell/dynamodb-utils)

A kotlin library with utility function for dynamdb 

## Get it

The easiest thing is to use [jitpack](jitpack.io). Add this to your gradle file
```
repositories {
        jcenter()
        maven { url "https://jitpack.io" }
   }
   dependencies {
         implementation 'com.github.frankfarrell:dynamodb-utils:v0.0.1'
   }
```

## DynamoBatchExecutor

DynamoDB allows writing and deleting items in batches of up to 25. However if there is a provisioned throughput exception, some or all the requests may fail. 
The aws sdk does not retry these requests transparently as it does for other operations, but it is left up the client to retry, ideally with exponential backoff. 
This library encapsulates does that for you using rx-kotlin. 

Failed writes are retried up until the attempt limit is reached using exponential backoff with jitter. 

For refernence, [github link to issue](https://github.com/aws/aws-sdk-js/issues/1262)
For details on exponential backoff and jitter in aws see [her](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

### Examples
```kotlin 

data class Person(val name:String, val address: String)

class PersonMapper : DynamoMapper<Person> {
    override fun mapToDynamoItem(person: Person): Map<String, AttributeValue> {
        return mapOf(Pair("name", AttributeValue(person.name)),
                Pair("address", AttributeValue(person.address)))
    }
}

val amazonDynamoDB: AmazonDynamoDB = ...

val dyanmoBatch: DynamoBatchExecutor<Person> = DynamoBatchExecutor<Person> (amazonDynamoDB)

dynamoBatch.persist(listOf(Person("bob", "France"), Person("jack", "Dublin")), PersonMapper(), "targetTable")


```


## DynamoTableCloner

This library allows you to clone a dynamo table, including hash and sort key,local and secondary indexes, provisioned throughput settings, dynamo streaming even source mappings and autoscaling properties if defined. 
The library also exposes functions that return the aws sdk requests that would create all of the above without applying them so that tables can be partially cloned, and also streaming event sources or autoscaling properties from one table to 
be applied to another 

### Examples
```kotlin

// You can create your own clients as necessary, the default is to use the standard client. 
val amazonDynamoDB: AmazonDynamoDB = ...
val awsLambdaClient: AWSLambdaClient = ...
val autoScalingClient: AWSApplicationAutoScaling = ...

val dynamoTableCloner: DynamoTableCloner = DynamoTableCloner(amazonDynamoDB, awsLambdaClient, autoScalingClient)

dynamoTableCloner.cloneTable("template", "target")

// You now have a new table called "target"
```

## Dynamo DSL (Work in Progess)
A kotlin internal DSL for Query, Scan and Update operations on Dynamo

```kotlin

var result = DynamoDSL().query("mytable") { 
            hashKey("myHashKey") {
                eq("abcd")
            }
            sortKey("mysortkey"){
                between ( 2 AND 3)
            }
            filtering {
                attribute("age") {
                    eq(44)
                } and attributeExists("name") or {
                     attribute("nested"){
                         eq("x")
                     } and attributeExists("movie"){
                         eq("y")
                     }
                }
            }
        }
        
while(result.hasNext()){
    println(result.next());
}
```
