package com.github.frankfarrell.dynamodsl

import com.sun.org.apache.xpath.internal.operations.Bool

/**
 * Created by frankfarrell on 17/07/2018.
 *
 * Allows for nested filtering queries by chaining blocks with 'and' and 'or'
 */

class Filter(val filterQuery: ChainableFilterQuery){}


@DynamoDSLMarker
class FilterBuilder(){
    var ChainableFilterQuery: ChainableFilterQuery? = null
    fun build(): Filter = Filter(ChainableFilterQuery!!.build())
}

@DynamoDSLMarker
fun QueryIteratorBuilder.filtering(block: FilterBuilder.() -> Unit) {
    filtering = FilterBuilder().apply(block).build()
}

@DynamoDSLMarker
fun FilterBuilder.attribute(value: String, block: ChainableFilterQuery.() -> Unit) : ChainableFilterQuery{

    //First in chain
    if(ChainableFilterQuery== null){
        ChainableFilterQuery = ChainableFilterQuery().apply(block)
        ChainableFilterQuery!!.dynamoFunction = Attribute(value)
        return ChainableFilterQuery!!
    }
    else{
        val x = ChainableFilterQuery().apply(block)
        x.dynamoFunction = Attribute(value)
        return x
    }
}

@DynamoDSLMarker
fun FilterBuilder.attributeExists(value: String) : ChainableFilterQuery{
    return if(ChainableFilterQuery== null){
        ChainableFilterQuery = ChainableFilterQuery()
        ChainableFilterQuery!!.dynamoFunction = AttributeExists(value)
        ChainableFilterQuery!!
    } else{
        val chainedFilter = ChainableFilterQuery()
        chainedFilter.dynamoFunction = AttributeExists(value)
        chainedFilter
    }
}




//TODO Would be nice to have something like this, but we can't have a reference to an objects class super method, eg super::method
private fun attributeInner(nullCheck: () -> Boolean,
                           attributeFunction: (String, ChainableFilterQuery.() -> Unit) -> ChainableFilterQuery,
                           value: String,
                           block: ChainableFilterQuery.() -> Unit): ChainableFilterQuery{
    //First in this level of nesting
    return if(nullCheck()){
        attributeFunction(value, block)
    }
    else{
        val chainedFilter = ChainableFilterQuery()
        chainedFilter.dynamoFunction = Attribute(value)
        chainedFilter
    }
}

interface DynamoFunction

data class Attribute(val attributeName: String): DynamoFunction
data class AttributeExists(val attributeName: String): DynamoFunction


enum class ChainableFilterBooleanConnection{
    AND, OR
}

class ChainableFilterConnection(val value: ChainableFilterQuery,
                                       val connectionToRight : ChainableFilterBooleanConnection)

open class ChainableFilterQuery{
    var dynamoFunction: DynamoFunction? = null
    var comparator: DynamoComparator? = null

    var left: ChainableFilterQuery? = null
    var right: ChainableFilterConnection? = null

    var parent: ChainableFilterQuery? = null
    var children: MutableList<ChainableFilterConnection> = mutableListOf()

    //TODO This properly!
    fun build(): ChainableFilterQuery {
        return this
    }

    //Following 2 method are equivalent to bracketed conditions
    @DynamoDSLMarker
    open infix fun and(chain: NestedChainableFilterQuery.() -> Unit): NestedChainableFilterQuery {
        val innerBlock = NestedChainableFilterQuery().apply(chain)
        innerBlock.parent = this
        this.children.add(ChainableFilterConnection(innerBlock, ChainableFilterBooleanConnection.AND))
        return innerBlock
    }

    @DynamoDSLMarker
    open infix fun or(chain: NestedChainableFilterQuery.() -> Unit): NestedChainableFilterQuery {
        val innerBlock = NestedChainableFilterQuery().apply(chain)
        innerBlock.parent = this
        this.children.add(ChainableFilterConnection(innerBlock, ChainableFilterBooleanConnection.OR))
        return innerBlock
    }

    @DynamoDSLMarker
    open infix fun and(chain: ChainableFilterQuery): ChainableFilterQuery {
        val chainedFilter = ChainableFilterConnection(chain, ChainableFilterBooleanConnection.AND)
        this.right = chainedFilter
        chain.left = this
        return chain
    }

    @DynamoDSLMarker
    open infix fun or(chain: ChainableFilterQuery): ChainableFilterQuery {
        val chainedFilter = ChainableFilterConnection(chain, ChainableFilterBooleanConnection.OR)
        this.right = chainedFilter
        chain.left = this
        return chain
    }

    @DynamoDSLMarker
    open fun attribute(value: String, block: ChainableFilterQuery.() -> Unit) : ChainableFilterQuery {
        this.apply(block)
        this.dynamoFunction = Attribute(value)
        return this
    }

    @DynamoDSLMarker
    open fun attributeExists(value: String) : ChainableFilterQuery{
        this.dynamoFunction = AttributeExists(value)
        return this
    }
}

class NestedChainableFilterQuery: ChainableFilterQuery() {

    //And after a bracketed section
    @DynamoDSLMarker
    override infix fun and(chain: ChainableFilterQuery): ChainableFilterQuery {
        val chainedFilter = ChainableFilterConnection(chain, ChainableFilterBooleanConnection.AND)
        return if(this.parent != null){
            this.parent!!.right = chainedFilter
            chain.left = this.parent!!
            this.parent!!
        } else {
            this.right = chainedFilter
            chain.left = this
            chain
        }

    }

    @DynamoDSLMarker
    override infix fun or(chain: ChainableFilterQuery): ChainableFilterQuery {
        val chainedFilter = ChainableFilterConnection(chain, ChainableFilterBooleanConnection.OR)
        return if(this.parent != null){
            this.parent!!.right = chainedFilter
            chain.left = this.parent!!
            this.parent!!
        } else {
            this.right = chainedFilter
            chain.left = this
            chain
        }
    }

    @DynamoDSLMarker
    override fun attribute(value: String, block: ChainableFilterQuery.() -> Unit) : ChainableFilterQuery {
        //First in this level of nesting
        return if(this.dynamoFunction == null ){
            super.attribute(value, block)
        } else{
            val chainedFilter = ChainableFilterQuery().apply(block)
            chainedFilter.dynamoFunction = Attribute(value)
            chainedFilter
        }
    }

    @DynamoDSLMarker
    override fun attributeExists(value: String) : ChainableFilterQuery{
        //First in this level of nesting
        return if(this.dynamoFunction == null ){
            super.attributeExists(value)
        } else{
            val chainedFilter = ChainableFilterQuery()
            chainedFilter.dynamoFunction = AttributeExists(value)
            chainedFilter
        }
    }
}

//TODO Add all other comparators here
@DynamoDSLMarker
fun ChainableFilterQuery.eq(value: Any){
    comparator = Equals(value)
}