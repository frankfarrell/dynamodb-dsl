package com.github.frankfarrell.dynamoclone

import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScaling
import com.amazonaws.services.applicationautoscaling.model.DescribeScalableTargetsResult
import com.amazonaws.services.applicationautoscaling.model.DescribeScalingPoliciesResult
import com.amazonaws.services.applicationautoscaling.model.ScalableTarget
import com.amazonaws.services.applicationautoscaling.model.ScalingPolicy
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.*
import com.amazonaws.services.lambda.AWSLambdaClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

/**
 * Created by frankfarrell on 11/07/2018.
 */

class DynamoTableClonerTest {

    val mockAmazonDynamoDB: AmazonDynamoDB = mockk()
    val mockAwsLambdaClient: AWSLambdaClient = mockk()
    val mockAutoScalingClient: AWSApplicationAutoScaling = mockk()

    lateinit var dynamoTableClonerUnderTest: DynamoTableCloner

    @Before
    fun setup(){

        //TODO We need unit testing around autoscaling
        every {
            mockAutoScalingClient.describeScalingPolicies(any())
        } answers {
            DescribeScalingPoliciesResult().withScalingPolicies(emptyList<ScalingPolicy>())
        }

        every {
            mockAutoScalingClient.describeScalableTargets(any())
        } answers {
            DescribeScalableTargetsResult().withScalableTargets(emptyList<ScalableTarget>())
        }

        dynamoTableClonerUnderTest = DynamoTableCloner(mockAmazonDynamoDB, mockAwsLambdaClient, mockAutoScalingClient)
    }

    @Test
    fun `it should query for table and create where stream is disabled`(){

        val templateTableDescription = TableDescription()
                .withTableName("template")
                .withGlobalSecondaryIndexes(emptyList<GlobalSecondaryIndexDescription>())
                .withLocalSecondaryIndexes(emptyList<LocalSecondaryIndexDescription>())
                .withProvisionedThroughput(
                        ProvisionedThroughputDescription().withWriteCapacityUnits(5L).withReadCapacityUnits(7L))
                .withKeySchema(KeySchemaElement("hash", KeyType.HASH))
                .withStreamSpecification(StreamSpecification().withStreamEnabled(false))

        val mockDescribeTableResult: DescribeTableResult = mockk()
        every { mockDescribeTableResult.table } answers { templateTableDescription }
        every { mockAmazonDynamoDB.describeTable(any<String>())} answers {mockDescribeTableResult}

        val captor = slot<CreateTableRequest>()
        every { mockAmazonDynamoDB.createTable(capture<CreateTableRequest>(captor)) } answers {mockk()}

        dynamoTableClonerUnderTest.cloneTable("template", "target")


        verify { mockAmazonDynamoDB.describeTable("template") }

        assertEquals(captor.captured.tableName, "target")
        assertEquals(captor.captured.keySchema.get(0), KeySchemaElement("hash", KeyType.HASH))
        assertEquals(captor.captured.provisionedThroughput.writeCapacityUnits, 5L)
        assertEquals(captor.captured.provisionedThroughput.readCapacityUnits, 7L)

        verify(exactly = 0) { mockAwsLambdaClient.getEventSourceMapping(any()) }
        verify(exactly = 0) { mockAwsLambdaClient.createEventSourceMapping(any()) }
    }

}