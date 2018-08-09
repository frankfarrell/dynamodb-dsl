package com.github.frankfarrell.dynamodsl

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.omg.SendingContext.RunTime
import java.util.*
import kotlin.streams.asSequence


/**
 * Created by frankfarrell on 17/07/2018.
 *
 * Allows for nested filtering queries by chaining blocks with 'and' and 'or'
 */


class FilterRequestProperties(val expressionAttributeValues: Map<String, AttributeValue>,
                              val filterExpression: String,
                              val expressionAttributeNames: Map<String, String>)

interface FilterQuery

//Represents a bracketed section
class RootFilter(val filterQuery: List<FilterConnection>) : FilterQuery {

    fun getFilterRequestProperties(): FilterRequestProperties{

        val expressionAttributeValues: MutableMap<String, AttributeValue> = mutableMapOf()
        val expressionAttributeNames: MutableMap<String, String> = mutableMapOf()
        var filterExpression: String = ""

        val filterClosure : (FilterQuery) -> Unit = {
            when(it){
                is RootFilter -> {
                    val nestedProperties = it.getFilterRequestProperties()
                    filterExpression += "(${nestedProperties.filterExpression})"
                    expressionAttributeValues.putAll(nestedProperties.expressionAttributeValues)
                    expressionAttributeNames.putAll(nestedProperties.expressionAttributeNames)
                }
                is ConcreteFilter -> {
                    val nestedProperties = it.getFilterRequestProperties()
                    filterExpression += "${nestedProperties.filterExpression}"
                    expressionAttributeValues.putAll(nestedProperties.expressionAttributeValues)
                    expressionAttributeNames.putAll(nestedProperties.expressionAttributeNames)
                }
            }
        }

        val condition = filterQuery.first().value
        filterClosure(condition)

        filterQuery.drop(1).forEach({
            when(it.connectionToLeft){
                FilterBooleanConnection.AND -> {
                    filterExpression += " AND "
                    filterClosure(it.value)
                }
                FilterBooleanConnection.OR -> {
                    filterExpression += " OR "
                    filterClosure(it.value)
                }
                null -> throw RuntimeException("Non head Filter without connection to left")
            }
        })

        return FilterRequestProperties(expressionAttributeValues, filterExpression, expressionAttributeNames)

    }
}

class ConcreteFilter(val dynamoFunction: DynamoFunction,
                     val comparator: DynamoComparator? = null): FilterQuery {

    fun getFilterRequestProperties(): FilterRequestProperties{

        val expressionAttributeValues: MutableMap<String, AttributeValue> = mutableMapOf()
        val expressionAttributeNames: MutableMap<String, String> = mutableMapOf()
        var filterExpression: String = ""

        when(dynamoFunction){
            is Attribute -> {
                val expressionAttributeName = toExpressionAttributeName(dynamoFunction.attributeName)
                filterExpression += expressionAttributeName
                expressionAttributeNames.put(expressionAttributeName, dynamoFunction.attributeName)

                fun singleValueComparator(operator: String, comparator: SingleValueDynamoCompator){
                    val expressionAttributeValue = toExpressionAttributeValue(dynamoFunction.attributeName)
                    filterExpression += " $operator ${expressionAttributeValue}"
                    expressionAttributeValues.put(expressionAttributeValue, toAttributeValue(comparator.right))
                }

                when(comparator){
                    is Equals -> singleValueComparator("=", comparator)
                    is NotEquals -> singleValueComparator("<>", comparator)
                    is GreaterThan -> singleValueComparator(">", comparator)
                    is GreaterThanOrEquals -> singleValueComparator(">=", comparator)
                    is LessThan -> singleValueComparator("<", comparator)
                    is LessThanOrEquals -> singleValueComparator("<=", comparator)
                    is Between -> {
                        val leftExpressionAttributeValue = toExpressionAttributeValue(dynamoFunction.attributeName + "left")
                        val rightExpressionAttributeValue = toExpressionAttributeValue(dynamoFunction.attributeName + "right")
                        filterExpression += " BETWEEN ${leftExpressionAttributeValue} AND ${rightExpressionAttributeValue} "
                        expressionAttributeValues.put(leftExpressionAttributeValue, toAttributeValue(comparator.left))
                        expressionAttributeValues.put(rightExpressionAttributeValue, toAttributeValue(comparator.right))
                    }
                    is InList -> {

                       val listOfAttributeValues = comparator.right
                                .map({
                                    val expressionAttributeValue = toExpressionAttributeValue(dynamoFunction.attributeName)
                                    expressionAttributeValues.put(expressionAttributeValue, toAttributeValue(it))
                                    expressionAttributeValue
                                })
                               .joinToString()

                        filterExpression += " IN (${listOfAttributeValues})"

                    }
                }

            }
            is AttributeExists -> {
                val expressionAttributeName = toExpressionAttributeName(dynamoFunction.attributeName)
                filterExpression += "attribute_exists(${expressionAttributeName})"
                expressionAttributeNames.put(expressionAttributeName, dynamoFunction.attributeName)
            }
        }

        return FilterRequestProperties(expressionAttributeValues, filterExpression, expressionAttributeNames)
    }

    val source = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    val random = Random()

    private fun toExpressionAttributeName(attributeName: String): String {
        return "#" + attributeName.filter { it in ('a' until 'z') + ('A' until 'Z')} + nonce()
    }
    private fun toExpressionAttributeValue(attributeName: String): String {
        return ":" + attributeName.filter { it in ('a' until 'z') + ('A' until 'Z') } + nonce()
    }

    private fun nonce(length: Long = 5): String {
        return random.ints(length, 0, source.length)
                .asSequence()
                .map(source::get)
                .joinToString("")
    }
}

//Represents a connector and an individual condition 'AND X' , 'OR (Y AND Z)' , etc
class FilterConnection(val value: FilterQuery,
                       val connectionToLeft : FilterBooleanConnection?) //Ie last in chain won't have a right connection

enum class FilterBooleanConnection{
    AND, OR
}

interface DynamoFunction

data class Attribute(val attributeName: String): DynamoFunction
data class AttributeExists(val attributeName: String): DynamoFunction


@DynamoDSLMarker
fun QueryIteratorBuilder.filtering(block: RootFilterBuilder.() -> Unit) {
    filtering = RootFilterBuilder().apply(block).build()
}


interface FilterQueryBuilder {
    fun build() : FilterQuery
}

class ConcreteFilterBuilder: FilterQueryBuilder {

    var dynamoFunction: DynamoFunction? = null
    var comparator: DynamoComparator? = null

    override fun build(): FilterQuery {
        return ConcreteFilter(dynamoFunction!!, comparator)
    }

}

//TODO Add all other comparators here
@DynamoDSLMarker
fun ConcreteFilterBuilder.eq(value: Any){
    comparator = Equals(value)
}

@DynamoDSLMarker
fun ConcreteFilterBuilder.noteq(value: Any){
    comparator = NotEquals(value)
}

@DynamoDSLMarker
fun ConcreteFilterBuilder.gt(value: Any){
    comparator = GreaterThan(value)
}

@DynamoDSLMarker
fun ConcreteFilterBuilder.lt(value: Any){
    comparator = LessThan(value)
}

@DynamoDSLMarker
fun ConcreteFilterBuilder.gteq(value: Any){
    comparator = GreaterThanOrEquals(value)
}

@DynamoDSLMarker
fun ConcreteFilterBuilder.lteq(value: Any){
    comparator = LessThanOrEquals(value)
}

@DynamoDSLMarker
fun ConcreteFilterBuilder.inList(value: List<Any>){
    comparator = InList(value)
}

@DynamoDSLMarker
class RootFilterBuilder: FilterQueryBuilder{

    var currentFilter: FilterQuery? = null

    var filterQueries: MutableList<FilterConnection> = mutableListOf()
    override fun build(): RootFilter = RootFilter(filterQueries)

    //Following 2 method are equivalent to bracketed conditions
    @DynamoDSLMarker
    infix fun and(block: RootFilterBuilder.() -> Unit): RootFilterBuilder {
        val value = RootFilterBuilder().apply(block)
        val connectionToLeft = FilterBooleanConnection.AND
        filterQueries.add(FilterConnection(value.build(), connectionToLeft))
        return this
    }

    @DynamoDSLMarker
    infix fun or(block: RootFilterBuilder.() -> Unit): RootFilterBuilder {
        val value = RootFilterBuilder().apply(block)
        val connectionToLeft = FilterBooleanConnection.OR
        filterQueries.add(FilterConnection(value.build(), connectionToLeft))
        return this
    }

    @DynamoDSLMarker
    infix fun and(value: RootFilterBuilder): RootFilterBuilder {
        filterQueries.add(FilterConnection(this.currentFilter!!, FilterBooleanConnection.AND))
        return this
    }

    @DynamoDSLMarker
    infix fun or(value: RootFilterBuilder): RootFilterBuilder {
        filterQueries.add(FilterConnection(this.currentFilter!!, FilterBooleanConnection.OR))
        return this
    }
}



@DynamoDSLMarker
fun RootFilterBuilder.attribute(value: String, block: ConcreteFilterBuilder.() -> Unit) : RootFilterBuilder{

    if(this.filterQueries.isEmpty()){
        val concreteFilter = ConcreteFilterBuilder().apply(block)
        concreteFilter.dynamoFunction = Attribute(value)
        this.filterQueries.add(FilterConnection(concreteFilter.build(), null))
        return this
    }
    else{
        val concreteFilter = ConcreteFilterBuilder().apply(block)
        concreteFilter.dynamoFunction = Attribute(value)
        this.currentFilter = concreteFilter.build()
        return this
    }


}

@DynamoDSLMarker
fun RootFilterBuilder.attributeExists(value: String) : RootFilterBuilder{
    this.currentFilter = ConcreteFilter(AttributeExists(value))
    return this
}