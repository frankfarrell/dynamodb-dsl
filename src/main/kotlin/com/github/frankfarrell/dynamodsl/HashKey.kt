package com.github.frankfarrell.dynamodsl

/**
 * Created by frankfarrell on 17/07/2018.
 */

@DynamoDSLMarker
class HashKey(val keyName: String,
              val equals: Equals){
}

@DynamoDSLMarker
class HashKeyBuilder(val keyName: String){
    var comparator: Equals? = null
    fun build(): HashKey = HashKey(keyName, comparator!!)
}

fun HashKeyBuilder.eq(value: Any){
    comparator = Equals(value)
}