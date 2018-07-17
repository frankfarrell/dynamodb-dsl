package com.github.frankfarrell.dynamodsl

/**
 * Created by frankfarrell on 17/07/2018.
 */

@DynamoDSLMarker
class SortKey(val sortKeyName: String,
              val comparisonOperator: DynamoComparator): ComparableBuilder{

}

@DynamoDSLMarker
class SortKeyBuilder(val keyName: String){
    var comparator: DynamoComparator? = null
    fun build(): SortKey = SortKey(keyName, comparator!!)
}

fun SortKeyBuilder.between(values: Pair<Any, Any>){
    comparator = Between(values.first, values.second)
}

fun SortKeyBuilder.eq(value: Any){
    comparator = Equals(value)
}