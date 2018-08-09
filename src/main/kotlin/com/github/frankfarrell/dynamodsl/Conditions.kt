package com.github.frankfarrell.dynamodsl

import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition

/**
 * Created by frankfarrell on 17/07/2018.
 */
interface DynamoComparator {
    fun toCondition(): Condition
}

interface SingleValueDynamoCompator: DynamoComparator {
    val right: Any
}

interface ComparableBuilder {}

class Equals(override val right: Any): SingleValueDynamoCompator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(toAttributeValue(right))
    }
}

class NotEquals(override  val right: Any): SingleValueDynamoCompator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(toAttributeValue(right))
    }
}

class GreaterThan(override val right: Any): SingleValueDynamoCompator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(toAttributeValue(right))
    }

}

class LessThan(override val right: Any): SingleValueDynamoCompator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.LT).withAttributeValueList(toAttributeValue(right))
    }

}

class GreaterThanOrEquals(override val right: Any): SingleValueDynamoCompator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(toAttributeValue(right))
    }

}

class LessThanOrEquals(override val right: Any): SingleValueDynamoCompator{

    override fun toCondition(): Condition {
        return Condition().withComparisonOperator(ComparisonOperator.LE).withAttributeValueList(toAttributeValue(right))
    }
}

class InList(val right: List<Any>): DynamoComparator{

    override fun toCondition(): Condition {
        //Is this right?
        return Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(toAttributeValue(right))
    }
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