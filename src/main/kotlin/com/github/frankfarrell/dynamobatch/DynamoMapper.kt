package com.github.frankfarrell.dynamobatch

import com.amazonaws.services.dynamodbv2.model.AttributeValue

/**
 * Created by frankfarrell on 09/07/2018.
 */

interface DynamoMapper<T> {
    fun mapToDynamoItem(t: T): Map<String, AttributeValue>
}