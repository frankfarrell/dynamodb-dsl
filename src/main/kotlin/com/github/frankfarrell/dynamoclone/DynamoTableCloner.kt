package com.github.frankfarrell.dynamoclone

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.*
import com.amazonaws.services.lambda.AWSLambda
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScaling
import com.amazonaws.services.applicationautoscaling.model.*
import com.amazonaws.services.lambda.model.CreateEventSourceMappingRequest
import com.amazonaws.services.lambda.model.EventSourceMappingConfiguration
import com.amazonaws.services.lambda.model.EventSourcePosition
import com.amazonaws.services.lambda.model.ListEventSourceMappingsRequest

/**
 * Created by frankfarrell on 10/07/2018.
 *
 * Clone a given table
 */
open class DynamoTableCloner(private val amazonDynamoDB: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient(),
                             private val lambdaClient: AWSLambda ,
                             private val autoScalingClient: AWSApplicationAutoScaling){

    /**
     * Create a new cloned table with name [targetTableName] from table [templateTableName]
     */
    fun cloneTable(templateTableName: String,
                   targetTableName: String) {

        //Base table properties (hash&sort keys, global secondary indexes, local indexes
        val descriptionOfTemplateTable = amazonDynamoDB.describeTable(templateTableName).table
        val request = createTableRequest(targetTableName, descriptionOfTemplateTable)
        val createTableResult = amazonDynamoDB.createTable(request)

        //DynamoDB Streaming
        if (descriptionOfTemplateTable.streamSpecification?.streamEnabled!!) {
            createEventSourceMappingRequestsFromArns(descriptionOfTemplateTable.latestStreamArn, createTableResult.tableDescription.latestStreamArn)
                    .forEach({ lambdaClient.createEventSourceMapping(it) })
        }

        //DynamoDB Autoscaling
        val scalableTargets = describeScalableTargetsForTable(templateTableName)
        if (scalableTargets.isNotEmpty()) {
            val scalingPolicies = describeScalingPoliciesForTable(templateTableName)

            createScalableTargetRequestsFromScalableTarget(scalableTargets, targetTableName).forEach({autoScalingClient.registerScalableTarget(it)})
            createScalingPolicyRequestsFromScalingPolicy(scalingPolicies, targetTableName).forEach({autoScalingClient.putScalingPolicy(it)})
        }
    }

    /**
     * Returns a CreateTableRequest without executing
     * Allows for more specific table cloning
     */
    fun createTableRequest(templateTableName: String,
                           targetTableName: String): CreateTableRequest {
        val descriptionOfTemplateTable = amazonDynamoDB.describeTable(templateTableName).table
        return createTableRequest(targetTableName, descriptionOfTemplateTable)
    }

    /**
     * Creates the CreateEventSourceMappingRequests for a given table, assuming it already exists.
     * For instance this allows you to clone event source mappings from one table to another existing table
     */
    fun createEventSourceMappingRequests(templateTableName: String,
                                         targetTableName: String): List<CreateEventSourceMappingRequest> {
        val descriptionOfTemplateTable = amazonDynamoDB.describeTable(templateTableName).table
        val descriptionOfTargetTable = amazonDynamoDB.describeTable(targetTableName).table

        return createEventSourceMappingRequestsFromArns(descriptionOfTemplateTable.latestStreamArn, descriptionOfTargetTable.latestStreamArn)
    }

    fun createScalableTargetRequests(templateTableName: String,
                                     targetTableName: String): List<RegisterScalableTargetRequest> {

        return createScalableTargetRequestsFromScalableTarget(describeScalableTargetsForTable(templateTableName), targetTableName)
    }

    fun createScalingPolicyRequests(templateTableName: String,
                                    targetTableName: String): List<PutScalingPolicyRequest> {
        return createScalingPolicyRequestsFromScalingPolicy(describeScalingPoliciesForTable(templateTableName), targetTableName)
    }

    fun describeScalableTargetsForTable(tableName: String): List<ScalableTarget> {
        val request = DescribeScalableTargetsRequest()
                .withServiceNamespace(ServiceNamespace.Dynamodb)
                .withResourceIds(String.format("table/%s", tableName))

        return autoScalingClient.describeScalableTargets(request).scalableTargets
    }

    fun describeScalingPoliciesForTable(tableName: String): List<ScalingPolicy> {
        val dspRequest = DescribeScalingPoliciesRequest()
                .withServiceNamespace(ServiceNamespace.Dynamodb)
                .withResourceId(String.format("table/%s", tableName))

        return autoScalingClient.describeScalingPolicies(dspRequest).scalingPolicies
    }

    protected fun createEventSourceMappingRequestsFromArns(templateTableStreamArn: String,
                                                           targetTableStreamArn: String): List<CreateEventSourceMappingRequest>{

        val eventSourceMappingRequest = ListEventSourceMappingsRequest()
        eventSourceMappingRequest.withEventSourceArn(templateTableStreamArn)
        val result = lambdaClient.listEventSourceMappings(eventSourceMappingRequest)

        return result.eventSourceMappings.map { eventSourceMappingConfiguration ->
           copyEventSourceMapping(eventSourceMappingConfiguration, targetTableStreamArn)
        }
    }

    protected fun createTableRequest(targetTableName: String,
                                     descriptionOfSeedTable: TableDescription): CreateTableRequest {
        val request = CreateTableRequest()

        if (descriptionOfSeedTable.globalSecondaryIndexes != null && descriptionOfSeedTable.globalSecondaryIndexes.size > 0) {
            val globalSecondaryIndexList =
                    descriptionOfSeedTable.globalSecondaryIndexes
                            .map { gsi ->
                                GlobalSecondaryIndex()
                                        .withProvisionedThroughput(getProvisionedThroughputFromDescription(gsi.provisionedThroughput))
                                        .withIndexName(gsi.indexName)
                                        .withKeySchema(gsi.keySchema)
                                        .withProjection(gsi.projection)
                            }

            request.withGlobalSecondaryIndexes(globalSecondaryIndexList)
        }

        if (descriptionOfSeedTable.localSecondaryIndexes != null && descriptionOfSeedTable.localSecondaryIndexes.size > 0) {
            val localSecondaryIndices =
                    descriptionOfSeedTable.localSecondaryIndexes
                            .map { lsi ->
                                LocalSecondaryIndex()
                                        .withIndexName(lsi.indexName)
                                        .withKeySchema(lsi.keySchema)
                                        .withProjection(lsi.projection)
                            }
            request.withLocalSecondaryIndexes(localSecondaryIndices)
        }

        return request.withAttributeDefinitions(descriptionOfSeedTable.attributeDefinitions)
                .withKeySchema(descriptionOfSeedTable.keySchema)
                .withProvisionedThroughput(getProvisionedThroughputFromDescription(descriptionOfSeedTable.provisionedThroughput))
                .withStreamSpecification(descriptionOfSeedTable.streamSpecification)
                .withTableName(targetTableName)
    }

    protected fun getProvisionedThroughputFromDescription(description: ProvisionedThroughputDescription): ProvisionedThroughput {
        return ProvisionedThroughput()
                .withReadCapacityUnits(description.readCapacityUnits)
                .withWriteCapacityUnits(description.writeCapacityUnits)
    }

    protected fun copyEventSourceMapping(eventSourceMappingConfiguration: EventSourceMappingConfiguration,
                                         dynamoStreamArn: String): CreateEventSourceMappingRequest {

        val createEventSourceMappingRequest = CreateEventSourceMappingRequest()
        val targetFunctionArn = eventSourceMappingConfiguration.functionArn
        val enabled = !eventSourceMappingConfiguration.state.equals("Disabled", ignoreCase = true)

        return createEventSourceMappingRequest
                .withEventSourceArn(dynamoStreamArn)
                .withBatchSize(eventSourceMappingConfiguration.batchSize)
                .withEnabled(enabled)
                .withStartingPosition(EventSourcePosition.TRIM_HORIZON)
                .withFunctionName(targetFunctionArn)
    }

    protected fun createScalableTargetRequestsFromScalableTarget(scalableTargets: List<ScalableTarget>, targetTableName: String): List<RegisterScalableTargetRequest> {
        return scalableTargets.map { scalableTarget ->
            RegisterScalableTargetRequest()
                    .withServiceNamespace(scalableTarget.serviceNamespace)
                    .withResourceId(String.format("table/%s", targetTableName))
                    .withScalableDimension(scalableTarget.scalableDimension)
                    .withMinCapacity(scalableTarget.minCapacity)
                    .withMaxCapacity(scalableTarget.maxCapacity)
                    .withRoleARN(scalableTarget.roleARN)
        }
    }

    protected fun createScalingPolicyRequestsFromScalingPolicy(scalingPolicies: List<ScalingPolicy>,
                                                               targetTableName: String): List<PutScalingPolicyRequest>{
        return scalingPolicies.map { scalingPolicy ->
            PutScalingPolicyRequest()
                    .withServiceNamespace(scalingPolicy.serviceNamespace)
                    .withScalableDimension(scalingPolicy.scalableDimension)
                    .withResourceId(String.format("table/%s", targetTableName))
                    .withPolicyName(scalingPolicy.policyName)
                    .withPolicyType(scalingPolicy.policyType)
                    .withTargetTrackingScalingPolicyConfiguration(scalingPolicy.getTargetTrackingScalingPolicyConfiguration())
        }
    }
}