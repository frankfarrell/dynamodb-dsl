package com.github.frankfarrell.dynamodsl

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.*
import java.nio.ByteBuffer


/**
 * Created by frankfarrell on 16/07/2018.
 * https://proandroiddev.com/writing-dsls-in-kotlin-part-1-7f5d2193f277
 * https://proandroiddev.com/writing-dsls-in-kotlin-part-2-cd9dcd0c4715
 * https://blog.jetbrains.com/kotlin/2011/10/dsls-in-kotlin-part-1-whats-in-the-toolbox-builders/
 */

@DslMarker()
annotation class DynamoDSLMarker

/*
Base class that we issue query and scans from
 */
class DynamoDSL(val dynamoDB: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient()) {
    fun query(tableName: String, block: QueryIteratorBuilder.() -> Unit): QueryIterator {
        val queryBuilder = QueryIteratorBuilder(dynamoDB)
        block(queryBuilder)
        val queryIterator = queryBuilder.build()
        //Initial query
        queryIterator.query()
        return queryIterator
    }
}

fun toAttributeValue(value: Any): AttributeValue {
    when(value){
        is ByteBuffer -> return AttributeValue().withB(value)
        is String -> return AttributeValue(value)
        is Number -> return AttributeValue().withN(value.toString())
        is Boolean -> return AttributeValue().withBOOL(value)
        is List<*> -> return AttributeValue().withL(value.map { toAttributeValue(it!!) })
        is Map<*, *> -> return AttributeValue().withM(value.entries.associate { it.key as String to toAttributeValue(it.value!!) })
        is Set<*> -> when(value.first()){
            is ByteBuffer -> return AttributeValue().withBS(value.map { it as ByteBuffer })
            is Number -> return AttributeValue().withNS(value.map { (it as Number).toString() })
            else -> return AttributeValue().withSS(value.map { it as String })
        }
        else -> return AttributeValue(value.toString())
    }
    //Do we need to handle NULL type?
}

val queteResult = DynamoDSL().query("mytable") {
            hashKey("myHashKey") {
                eq(2)
            }
            sortKey("mysortkey"){
                eq (2)
                between ( 2 AND 3)
            }
            filtering {
                attribute("age") {
                    eq(44)
                } and attribute("weight"){
                    eq(14)
                } or attribute("weight"){
                    eq(14)
                } or {
                    attribute(value = "age") {
                        eq(14)
                    } and {
                        attribute("weight") {
                            eq("12")
                        } or attribute("colour"){
                            eq("blue")
                        }
                    }
                }
            }
        }

/*
How to stop between being called if eq already called in above?
 */