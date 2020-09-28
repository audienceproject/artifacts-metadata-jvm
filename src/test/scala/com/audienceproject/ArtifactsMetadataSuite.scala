package com.audienceproject

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ArtifactsMetadataSuite extends FunSuite with BeforeAndAfterEach {
    val client = new ArtifactsMetadataClient
    val testType = "test-artifact-type-unit-test"

    override def afterEach() {
        val maxAttempts = 30
        var attempts = 0
        var scanResult: Option[ArtifactMetadata] = client.getByType(testType)
        while(scanResult.isDefined && attempts < maxAttempts) {
            client.delete(scanResult.get.uuid)
            scanResult = client.getByType(testType)
            attempts = attempts + 1
        }
        if (attempts == maxAttempts) throw new IllegalStateException(
            s"Something went wrong when trying to clean up after test." +
              s"Tried $maxAttempts times, but didn't succeed in cleaning." +
              s"The DynamoDB instance is in a dirty state, as some number of test object(s) with type $testType are left untouched.")
    }

    test("Test adding artifacts, scanning and querying") {
        val metadata = Map("iterations" -> 3, "sgd-optimizer" -> "adam")

        val uuid = client.log(testType, metadata=metadata)

        val scanResult = client.getByType(testType, metadata=metadata)
        assert(scanResult.get.artifactType == testType)
        assert(scanResult.get.metadata.toSeq.sortBy(_._1) sameElements scanResult.get.metadata.toSeq.sortBy(_._1))

        val queryResult = client.getByUuid(uuid)
        assert(queryResult.get.artifactType == testType)
        assert(queryResult.get.metadata.toSeq.sortBy(_._1) sameElements queryResult.get.metadata.toSeq.sortBy(_._1))
    }

    test("Test scanning for conflicting queries resolved by timestamp") {
        val metadata = Map("iterations" -> 3, "sgd-optimizer" -> "adam")

        val uuid1 = client.log(testType, metadata=metadata)
        val uuid2 = client.log(testType, metadata=metadata)
        val uuid3 = client.log(testType, metadata=metadata)

        assert(client.getByType(testType, metadata=metadata).get.uuid == uuid3)
        assert(client.getByType(testType, latest=false, metadata=metadata).get.uuid == uuid1)
    }

    test("Test resolve reference time") {
        val metadata = Map("iterations" -> 3, "sgd-optimizer" -> "adam")

        val uuid1 = client.logWithReferenceTime(testType,2019, 1, 30, metadata=metadata)
        val uuid2 = client.logWithReferenceTimeAsDateString(testType, "2019-10-12", metadata=metadata)
        val uuid3 = client.logWithReferenceTime(testType,2020, 2, 1, metadata=metadata)
        val uuid4 = client.logWithReferenceTime(testType,2019, 10, 14, metadata=metadata)
        val uuid5 = client.logWithReferenceTimeAsDateString(testType,"2019-10-14", metadata=metadata)

        assert(client.getLatestBeforeWithDateString(testType, "2019-01-28").isEmpty)
        assert(client.getLatestBefore(testType, 1998, 12, 31).isEmpty)
        assert(client.getLatestBefore(testType, 2019, 1, 30).get.uuid == uuid1)
        assert(client.getLatestBefore(testType, 2019, 2, 22).get.uuid == uuid1)
        assert(client.getLatestBefore(testType, 2031, 2, 22).get.uuid == uuid3)
        assert(client.getLatestBeforeWithDateString(testType, "2019-10-13").get.uuid == uuid2)
        assert(client.getLatestBefore(testType, 2019, 10, 14).get.uuid == uuid5)
        assert(client.getLatestBefore(testType, 2020, 1, 1).get.uuid == uuid5)
    }
}
