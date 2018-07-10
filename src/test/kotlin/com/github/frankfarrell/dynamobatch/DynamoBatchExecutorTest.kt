package com.github.frankfarrell.dynamobatch

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.*
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.reactivex.schedulers.TestScheduler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Created by frankfarrell on 10/07/2018.
 */
class DynamoBatchExecutorTest {

    val mockAmazonDynamoDB: AmazonDynamoDB = mockk()

    lateinit var dynamoBatchServiceUnderTest: DynamoBatchExecutor<Any>

    var testScheduler: TestScheduler = TestScheduler()
    var testRandom: Random = Random(1)

    @BeforeEach
    fun init() {
        clearMocks(mockAmazonDynamoDB)
    }

    @Test
    fun `persist does not retry if there are no unprocessed items`() {

        dynamoBatchServiceUnderTest = DynamoBatchExecutor<Any>(mockAmazonDynamoDB, testScheduler, testRandom, attemptLimit = 1000)

        val mockResultWithNoUnprocessedItems = BatchWriteItemResult().withUnprocessedItems(emptyMap<String, List<WriteRequest>>())

        val captor = slot<BatchWriteItemRequest>()

        every {
            mockAmazonDynamoDB.batchWriteItem(capture<BatchWriteItemRequest>(captor))
        } answers {
            mockResultWithNoUnprocessedItems
        }

        val initialRequest = ArrayList<Map<String, AttributeValue>>()
        initialRequest.add(Collections.singletonMap("pk", AttributeValue("test")))
        initialRequest.add(Collections.singletonMap("pk", AttributeValue("test2")))

        dynamoBatchServiceUnderTest.persist(initialRequest, "testtable")

        testScheduler.advanceTimeBy(1000, TimeUnit.MINUTES)
        testScheduler.triggerActions()

        assertEquals(captor.captured.requestItems["testtable"]?.size, 2)
    }

    @Test
    fun `persist retries if there are unprocessed items`() {

        dynamoBatchServiceUnderTest = DynamoBatchExecutor<Any>(mockAmazonDynamoDB, testScheduler, testRandom, attemptLimit = 1000)

        val mockResultWithUnprocessedItems = BatchWriteItemResult()
                .withUnprocessedItems(Collections.singletonMap("testtable",
                        listOf(WriteRequest(PutRequest().withItem(Collections.singletonMap("pk", AttributeValue("test")))))))

        val mockResultWithNoUnprocessedItems = BatchWriteItemResult().withUnprocessedItems(emptyMap<String, List<WriteRequest>>())

        val captor = mutableListOf<BatchWriteItemRequest>()

        every {
            mockAmazonDynamoDB.batchWriteItem(capture<BatchWriteItemRequest>(captor))
        } answers {
            mockResultWithUnprocessedItems
        } andThen {
            mockResultWithNoUnprocessedItems
       }

        val initialRequest = ArrayList<Map<String, AttributeValue>>()
        initialRequest.add(Collections.singletonMap("pk", AttributeValue("test")))
        initialRequest.add(Collections.singletonMap("pk", AttributeValue("test2")))

        dynamoBatchServiceUnderTest.persist(initialRequest, "testtable")

        testScheduler.advanceTimeBy(1000, TimeUnit.MINUTES)
        testScheduler.triggerActions()

        assertEquals(captor.get(0).requestItems["testtable"]?.size, 2)
        assertEquals(captor.get(1).requestItems["testtable"]?.size, 1)

    }
}