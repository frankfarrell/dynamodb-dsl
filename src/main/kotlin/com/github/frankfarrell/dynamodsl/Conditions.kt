package com.github.frankfarrell.dynamodsl

import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition

/**
 * Created by frankfarrell on 17/07/2018.
 */
interface DynamoComparator {
    fun toCondition(): Condition
}

interface ComparableBuilder {}

class Equals(val right: Any): DynamoComparator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(toAttributeValue(right))
    }

    val value = ComparisonOperator.EQ
}

class Between(val left: Any, val right: Any): DynamoComparator{
    val value = ComparisonOperator.BETWEEN

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.BETWEEN).withAttributeValueList(toAttributeValue(left), toAttributeValue(right))
    }
}

infix fun Number.AND(other:  Number): Pair<Number, Number> = Pair(this, other)
infix fun String.AND(other:  String): Pair<String, String> = Pair(this, other)


//TODO Flesh this out