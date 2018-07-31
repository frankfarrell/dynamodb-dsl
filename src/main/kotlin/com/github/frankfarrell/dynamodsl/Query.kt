package com.github.frankfarrell.dynamodsl

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.dynamodbv2.model.QueryResult

/**
 * Created by frankfarrell on 17/07/2018.
 */

/*
TODO Make this generic for Scan
 */
class QueryIterator(val dynamoDB: AmazonDynamoDB,
                    val tableName: String,
                    val hash: HashKey,
                    val sort: SortKey?,
                    val filtering: RootFilter?): Iterator<Map<String, AttributeValue>>{ //Make it generic

    var lastEvaluatedKey: Map<String, AttributeValue> = emptyMap()
    private val results: MutableList<Map<String, AttributeValue>> = mutableListOf()

    private var index: Int = 0

    override fun hasNext(): Boolean {
        return index < results.size || lastEvaluatedKey.isNotEmpty()
    }

    override fun next(): Map<String, AttributeValue> {
        if(index >= results.size && lastEvaluatedKey.isNotEmpty()){
            query()
            return next()
        }
        if(index >= results.size){
            throw RuntimeException("No more elements")
        }
        else{
            val toReturn = results[index]
            index++
            return toReturn
        }
    }

    fun query(){

        val request = QueryRequest()
        request.withTableName(tableName)

        if(sort == null){
            request.withKeyConditions(mapOf(Pair(hash.keyName, hash.equals.toCondition())))
        }
        else{
            request.withKeyConditions(mapOf(Pair(hash.keyName, hash.equals.toCondition()), Pair(sort.sortKeyName, sort.comparisonOperator.toCondition())))
        }

        if(filtering != null) {
            val props = filtering.getFilterRequestProperties()

            request.withFilterExpression(props.filterExpression)
            if(!props.expressionAttributeNames.isEmpty()){
                request.withExpressionAttributeNames(props.expressionAttributeNames)
            }
            if(!props.expressionAttributeValues.isEmpty()){
                request.withExpressionAttributeValues(props.expressionAttributeValues)
            }
        }

        //Returns last evaulated key
        val result: QueryResult = dynamoDB.query(request)

        results.addAll(result.items)
        if(result.lastEvaluatedKey == null){
            lastEvaluatedKey = emptyMap()
        }
        else{
            lastEvaluatedKey = result.lastEvaluatedKey
        }

    }

}

@DynamoDSLMarker
class QueryIteratorBuilder(val dynamoDB: AmazonDynamoDB) {
    var tableName: String? = null
    var hashkey: HashKey? = null
    var sortKey: SortKey? = null
    var filtering: RootFilter? = null
    fun build(): QueryIterator  {
        //TODO Assert hashkey and sortkey aren't null
        return QueryIterator(dynamoDB, tableName!!, hashkey!!, sortKey, filtering)
    }

}

fun QueryIteratorBuilder.hashKey(keyName: String, block: HashKeyBuilder.() -> Unit) {
    hashkey = HashKeyBuilder(keyName).apply(block).build()
}

fun QueryIteratorBuilder.sortKey(keyName: String, block: SortKeyBuilder.() -> Unit) {
    sortKey = SortKeyBuilder(keyName).apply(block).build()
}