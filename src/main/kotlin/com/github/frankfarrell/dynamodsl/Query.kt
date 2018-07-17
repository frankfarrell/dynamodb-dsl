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
                    val hash: HashKey,
                    val sort: SortKey?,
                    val filtering: Filter?): Iterator<Map<String, AttributeValue>>{ //Make it generic

    var lastEvaluatedKey: Map<String, AttributeValue> = emptyMap()
    private val results: MutableList<Map<String, AttributeValue>> = mutableListOf()

    private var index: Int = 0

    override fun hasNext(): Boolean {
        return lastEvaluatedKey.isNotEmpty()
    }

    override fun next(): Map<String, AttributeValue> {
        if(index >= results.size && hasNext()){
            query()
            return next()
        }
        if(index >= results.size){
            throw RuntimeException("No more elements")
        }
        else{
            val toReturn = results[index]
            //could do it in a one liner but confusing
            index++
            return toReturn
        }
    }

    fun query(){

        val request = QueryRequest()

        if(sort == null){
            request.withKeyConditions(mapOf(Pair(hash.keyName, hash.equals.toCondition())))
        }
        else{
            request.withKeyConditions(mapOf(Pair(hash.keyName, hash.equals.toCondition()), Pair(sort.sortKeyName, sort.comparisonOperator.toCondition())))
        }

        if(filtering != null){

            /*
            Walk over the ChainableFilterQuery building up a query string with brackets etc
             */

            filtering.filterQuery.comparator.toCondition()
            if(filtering.filterQuery.right != null){
                val rightCondition = filtering.filterQuery.right.comparator.toCondition()
                val rightConnector = filtering.filterQuery.right.connectionToRight
            }
        }

        //Returns last evaulated key
        val result: QueryResult = dynamoDB.query(request)

        results.addAll(result.items)
        lastEvaluatedKey = result.lastEvaluatedKey
    }

}

@DynamoDSLMarker
class QueryIteratorBuilder(val dynamoDB: AmazonDynamoDB) {
    var hashkey: HashKey? = null
    var sortKey: SortKey? = null
    var filtering: Filter? = null
    fun build(): QueryIterator  {
        //TODO Assert hashkey and sortkey aren't null
        return QueryIterator(dynamoDB, hashkey!!, sortKey, filtering)
    }

}

fun QueryIteratorBuilder.hashKey(keyName: String, block: HashKeyBuilder.() -> Unit) {
    hashkey = HashKeyBuilder(keyName).apply(block).build()
}

fun QueryIteratorBuilder.sortKey(keyName: String, block: SortKeyBuilder.() -> Unit) {
    sortKey = SortKeyBuilder(keyName).apply(block).build()
}