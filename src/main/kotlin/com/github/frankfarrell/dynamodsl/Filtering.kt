package com.github.frankfarrell.dynamodsl


/**
 * Created by frankfarrell on 17/07/2018.
 *
 * Allows for nested filtering queries by chaining blocks with 'and' and 'or'
 */



interface FilterQuery

//Represents a bracketed section
class RootFilter(val filterQuery: List<FilterConnection>) : FilterQuery

class ConcreteFilter(val dynamoFunction: DynamoFunction,
                     val comparator: DynamoComparator? = null): FilterQuery

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