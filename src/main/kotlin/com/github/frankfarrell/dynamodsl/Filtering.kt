package com.github.frankfarrell.dynamodsl

/**
 * Created by frankfarrell on 17/07/2018.
 *
 * Allows for nested filtering queries by chaining blocks with 'and' and 'or'
 */

class Filter(val filterQuery: ChainableFilterQuery){}


@DynamoDSLMarker
class FilterBuilder(){
    var filterQuery: ChainableFilterQueryBuilder? = null
    fun build(): Filter = Filter(filterQuery!!.build())
}

@DynamoDSLMarker
fun QueryIteratorBuilder.filtering(block: FilterBuilder.() -> Unit) {
    filtering = FilterBuilder().apply(block).build()
}

@DynamoDSLMarker
fun FilterBuilder.attribute(value: String, block: ChainableFilterQueryBuilder.() -> Unit) : ChainableFilterQueryBuilder{
    filterQuery = ChainableFilterQueryBuilder().apply(block)
    return filterQuery!!
}

enum class ChainableFilterConnection{
    AND, OR
}

class ChainableFilterQueryBuilder(){
    var comparator: DynamoComparator? = null
    var right: ChainableFilterQueryBuilder? = null
    var connectionToRight : ChainableFilterConnection? = null
    fun build(): ChainableFilterQuery = ChainableFilterQuery( comparator!!, right?.build(), connectionToRight)

    @DynamoDSLMarker
    infix fun and(chain: ChainableFilterQueryBuilder.() -> Unit): ChainableFilterQueryBuilder{
        val innerRight = ChainableFilterQueryBuilder().apply(chain)
        right = innerRight
        connectionToRight = ChainableFilterConnection.AND
        return innerRight
    }

    @DynamoDSLMarker
    infix fun or(chain: ChainableFilterQueryBuilder.() -> Unit): ChainableFilterQueryBuilder{
        val innerRight = ChainableFilterQueryBuilder().apply(chain)
        right = innerRight
        connectionToRight = ChainableFilterConnection.OR
        return innerRight
    }

    @DynamoDSLMarker
    infix fun and(chain: ChainableFilterQueryBuilder): ChainableFilterQueryBuilder{
        right = chain
        connectionToRight = ChainableFilterConnection.AND
        return chain
    }

    @DynamoDSLMarker
    infix fun or(chain: ChainableFilterQueryBuilder): ChainableFilterQueryBuilder{
        right = chain
        connectionToRight = ChainableFilterConnection.OR
        return chain
    }
}

@DynamoDSLMarker
fun ChainableFilterQueryBuilder.eq(value: Any){
    comparator = Equals(value)
}

class ChainableFilterQuery(val comparator: DynamoComparator,
                           val right: ChainableFilterQuery?,
                           val connectionToRight : ChainableFilterConnection?)