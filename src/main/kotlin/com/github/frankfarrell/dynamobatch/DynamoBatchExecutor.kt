package com.github.frankfarrell.dynamobatch

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.*
import io.reactivex.Scheduler
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import java.lang.Boolean.FALSE
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Function

/**
 * Created by frankfarrell on 09/07/2018.
 */
private val BATCH_SIZE = 25 // DynamoDB's BatchWriteItem allows a max of 25 items per batch

open class DynamoBatchExecutor<T>(private val amazonDynamoDB: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient(),
                                 private val scheduler: Scheduler =  Schedulers.io(),
                                 private val random: Random = Random()) {

    fun delete(items: List<T>, primaryKeyFunction: Function<T, Map<String, AttributeValue>>, tableName: String) {

        val writeRequests =
                items.map { record ->
                    WriteRequest()
                            .withDeleteRequest(DeleteRequest().withKey(primaryKeyFunction.apply(record)))
                }.map { writeRequest -> TableItemTuple(tableName, writeRequest) }

        batchPersist(writeRequests)
    }

    fun persist(items: List<T>, mapper: DynamoMapper<T>,
                tableName: String) {
        persist(items, mapper, BATCH_SIZE, tableName)
    }

    fun persist(items: List<T>, mapper: DynamoMapper<T>,
                batchSize: Int,
                tableName: String) {
        val writeList = getWriteRequests<T>(items, mapper)
        persist(writeList.map { TableItemTuple(tableName, it) } )
    }

    fun persist(items: List<Map<String, AttributeValue>>,
                tableName: String) {

        val writeList = items
                .map { WriteRequest().withPutRequest(PutRequest().withItem(it)) }

        persist(writeList
                .map { TableItemTuple(tableName, it) })
    }

    fun persist(writeList: List<TableItemTuple>): List<PublishSubject<RetryablePut>> {

        //TODO How to accumultate finite observable into list
        val listOfSubjects = ArrayList<PublishSubject<RetryablePut>>()

        // Split into batches and then persist to Dynamo
        io.reactivex.Observable.fromIterable(writeList)
                .buffer(BATCH_SIZE)
                .forEach({listOfSubjects.add(this.batchPersist(it))})
        return listOfSubjects
    }

    fun persistWithCallbackOnComplete(items: List<T>,
                                      mapper: DynamoMapper<T>,
                                      tableName: String,
                                      complete: Function<Instant, Void>) {
        val writeList = getWriteRequests(items, mapper)

        val result = persist(writeList.map { w -> TableItemTuple(tableName, w) })
                .map( { it to FALSE })
                .toMap()
                .toMutableMap()

        result.forEach({ subject, value ->

            //TODO Is there a more kotlin-esque way to do this?
            val consumer = object : Consumer<Any?> {
                @Synchronized override fun accept( x : Any?) {
                    result.put(subject, true)
                    val allComplete = result.entries.all {it.value }
                    if (allComplete) {
                        //No parameter, no return value, pure side-effect
                        complete.apply(Instant.now())
                    }
                }
            }

            subject.doOnComplete({ consumer.accept(null) })

            //Case where it was finished before method returned
            if (subject.hasComplete()) {
                consumer.accept(null)
            }
        })
    }

    private fun batchPersist(writeList: List<TableItemTuple>): PublishSubject<RetryablePut> {
        val subject = PublishSubject.create<RetryablePut>()

        subject.subscribe({ retry ->
            val batchWriteItemRequest = BatchWriteItemRequest()
            val requestItems = writeList.groupBy({it.tableName}, { it.writeRequest})

            batchWriteItemRequest.requestItems = requestItems
            try {
                val result = amazonDynamoDB.batchWriteItem(batchWriteItemRequest)
                // Partial failure
                if (result.unprocessedItems.isNotEmpty()) {

                    val retryRequests =
                            result.unprocessedItems
                                    .entries
                                    .flatMap{ entry ->
                                        entry.value.map { value -> TableItemTuple(entry.key, value) }
                                    }

                    createRetry(retry.attempt, retryRequests, subject)
                } else {
                    // Full Success
                    subject.onComplete()
                }
            } catch (e: ProvisionedThroughputExceededException) {
                createRetry(retry.attempt, writeList, subject)
            }
            // Total failure: Retry the whole lot
        })
        subject.onNext(RetryablePut(0, writeList))
        return subject
    }

    private fun <R : T> getWriteRequests(items: List<R>,
                                           mapper: DynamoMapper<R>): List<WriteRequest> {
        return items
                .map { mapper.mapToDynamoItem(it) }
                .map { WriteRequest().withPutRequest(PutRequest().withItem(it)) }
    }

    private fun createRetry(attempt: Int, writeList: List<TableItemTuple>,
                            subject: PublishSubject<RetryablePut>) {
        io.reactivex.Observable
                .timer(getExponentialBackoffWithJitter(attempt), TimeUnit.MILLISECONDS, scheduler)
                .subscribe({ subject.onNext(RetryablePut(attempt + 1, writeList)) })
    }

    // See: https://www.awsarchitectureblog.com/2015/03/backoff.html
    private fun getExponentialBackoffWithJitter(attempt: Number): Long {
        return random
                .longs(0, (1000 * Math.pow(2.0, attempt.toDouble())).toLong())
                .findFirst()
                // Should never happen, but fatal if it does
                .orElseThrow { RuntimeException("No random int found") }
    }
}

class TableItemTuple(val tableName: String, val writeRequest: WriteRequest)
class RetryablePut(val attempt: Int, val items: List<TableItemTuple>)